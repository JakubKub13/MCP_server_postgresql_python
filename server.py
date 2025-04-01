#!/usr/bin/env python

import sys
import json
import logging
import argparse
import asyncio
from urllib.parse import urlparse, urlunparse
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass

# MCP low-level and type imports
from mcp.server.lowlevel import Server, NotificationOptions
from mcp.server.models import InitializationOptions
import mcp.server.stdio as mcp_stdio
import mcp.types as types

# PostgreSQL async driver and pool
import psycopg
import psycopg_pool

# Nastavenie základného logovania
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Config and Lifespan ---

@dataclass
class AppContext:
    """Application context containing the connection pool."""
    pool: psycopg_pool.AsyncConnectionPool
    resource_base_url: str 

@asynccontextmanager
async def app_lifespan(server: Server) -> AsyncIterator[AppContext]:
    """
    Manages the lifespan of the application: creates and closes the connection pool.
    """
    db_url = getattr(server, 'db_url', None)
    resource_base_url = getattr(server, 'resource_base_url', 'postgres://unknown/')

    if not db_url:
        logger.error("Database URL was not provided for lifespan.")
        raise ValueError("Database URL was not provided for lifespan.")

    logger.info(f"Initializing connection pool for {db_url[:db_url.find('@') + 1 if '@' in db_url else len(db_url)]}...")
    pool = None
    try:
        pool = psycopg_pool.AsyncConnectionPool(
            conninfo=db_url, min_size=1, max_size=5
        )
        await pool.open(wait=True)
        logger.info("Connection pool initialized.")
        yield AppContext(pool=pool, resource_base_url=resource_base_url)
    except Exception as e:
        logger.error(f"Connection pool initialization failed: {e}")
        raise
    finally:
        if pool:
            logger.info("Closing connection pool...")
            await pool.close()
            logger.info("Pool closed.")

# --- Initialize MCP Server (Low-Level) ---

server = Server(
    name="example-servers/postgres",
    version="0.1.0",
    lifespan=app_lifespan
)

# --- Handlery pre Low-Level Server ---

# Helper function to get the lifespan context
def get_lifespan_context() -> AppContext:
    """Helper function to get the lifespan context."""
    ctx = server.request_context 
    if ctx is None or not isinstance(ctx.lifespan_context, AppContext):
         logger.error("Lifespan context not available or has incorrect type.")
         raise RuntimeError("App context not available")
    return ctx.lifespan_context

@server.list_resources()
async def handle_list_resources() -> types.ListResourcesResult:
    """Explicitly retrieves and returns a list of tables as MCP resources."""
    logger.info("Handling ListResources request...")
    app_ctx = get_lifespan_context()
    pool = app_ctx.pool
    base_url_str = app_ctx.resource_base_url

    resources = []
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public' ORDER BY table_name;
                    """
                )
                tables = await cur.fetchall()
                for (table_name,) in tables:
                    resource_uri = f"{base_url_str.rstrip('/')}/{table_name}/schema"
                    resources.append(types.Resource(
                        uri=resource_uri,
                        mimeType="application/json",
                        name=f'"{table_name}" database schema'
                    ))
        return types.ListResourcesResult(resources=resources)
    except Exception as e:
        logger.error(f"Failed to list resources: {e}")
        raise RuntimeError(f"Failed to list resources: {e}")

@server.read_resource()
async def handle_read_resource(uri: str) -> types.ReadResourceResult:
    """Reads the schema of a specific table based on the URI."""
    logger.info(f"Handling ReadResource request for URI: {uri}")
    app_ctx = get_lifespan_context()
    pool = app_ctx.pool

    try:
        parsed_uri = urlparse(uri)
        path_parts = parsed_uri.path.strip('/').split('/')
        if len(path_parts) < 2 or path_parts[-1] != 'schema':
            raise ValueError("Invalid resource URI format")
        table_name = path_parts[-2]
    except Exception as e:
        logger.error(f"Failed to parse resource URI '{uri}': {e}")
        raise ValueError(f"Invalid resource URI: {uri}")

    logger.info(f"Extracting schema for table: {table_name}")
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT column_name, data_type FROM information_schema.columns
                    WHERE table_name = %s AND table_schema = 'public'
                    ORDER BY ordinal_position;
                    """,
                    (table_name,)
                )
                columns = await cur.fetchall()
                schema_info = [{"column_name": name, "data_type": dtype} for name, dtype in columns]
                json_result_string = json.dumps(schema_info, indent=2)

                # Vrátime výsledok v správnom formáte
                return types.ReadResourceResult(contents=[
                    types.ResourceContent(
                        uri=uri, # Vrátime pôvodné URI požiadavky
                        mimeType="application/json",
                        text=json_result_string
                    )
                ])
    except Exception as e:
        logger.error(f"Chyba pri získavaní schémy pre tabuľku '{table_name}': {e}")
        raise RuntimeError(f"Failed to read resource '{uri}': {e}")


@server.list_tools()
async def handle_list_tools() -> types.ListToolsResult:
    """Lists the available tools."""
    logger.info("Handling ListTools request...")
    query_tool = types.Tool(
        name="query",
        description="Run a read-only SQL query",
        inputSchema={
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "The SQL query to execute."},
            },
            "required": ["sql"],
        },
        # outputSchema should be defined if we want to be more precise
    )
    return types.ListToolsResult(tools=[query_tool])


@server.call_tool()
async def handle_call_tool(name: str, arguments: dict | None) -> types.CallToolResult:
    """Handles the call of a tool."""
    logger.info(f"Handling CallTool request for tool: {name}")
    app_ctx = get_lifespan_context()
    pool = app_ctx.pool

    if name != "query":
        raise ValueError(f"Unknown tool: {name}")

    if arguments is None or "sql" not in arguments:
        raise ValueError("Missing 'sql' argument for query tool")

    sql = arguments["sql"]
    if not isinstance(sql, str):
         raise ValueError("'sql' argument must be a string")

    logger.info("Executing read-only SQL query tool...")

    disallowed_keywords = ['insert', 'update', 'delete', 'drop', 'create', 'alter', 'truncate', 'grant', 'revoke']
    if any(keyword in sql.lower() for keyword in disallowed_keywords):
        logger.warning(f"Attempt to run potentially harmful query: {sql[:100]}...")
        raise ValueError("Only read-only (SELECT) queries are allowed.")

    conn = None
    try:
        conn = await pool.acquire()
        async with conn.transaction(readonly=True, isolation_level='REPEATABLE READ'):
            async with conn.cursor() as cur:
                await cur.execute(sql)
                if cur.description:
                    results = await cur.fetchall()
                    column_names = [desc[0] for desc in cur.description]
                    rows_as_dicts = [dict(zip(column_names, row)) for row in results]
                    result_text = json.dumps(rows_as_dicts, indent=2, default=str)
                else:
                    result_text = json.dumps({"message": "Query executed, no rows returned."}, indent=2)

                return types.CallToolResult(
                    content=[types.TextContent(type="text", text=result_text)],
                    isError=False
                )
    except psycopg.Error as db_err:
        logger.error(f"Database error during tool execution: {db_err}")
        raise RuntimeError(f"Database query failed: {db_err}")
    except Exception as e:
        logger.error(f"Unexpected error during tool execution: {e}")
        raise RuntimeError(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            await pool.release(conn)


# --- Spustenie Servera (Low-Level) ---

async def run_stdio_server():
    """Spustí server pomocou stdio transportu."""
    logger.info("Starting server with stdio transport...")

    capabilities = server.get_capabilities(
        notification_options=NotificationOptions(), 
        experimental_capabilities={}            
    )

    init_opts = InitializationOptions( 
        server_name=server.name,
        server_version=server.version,
        capabilities=capabilities,
    )
    async with mcp_stdio.stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, init_opts)

def main():
    parser = argparse.ArgumentParser(description="MCP PostgreSQL Server (Low-Level API)")
    parser.add_argument("database_url", help="URL pripojenia k PostgreSQL databáze (napr. postgresql://user:pass@host:port/dbname)")
    args = parser.parse_args()

    if not args.database_url:
        print("Chyba: Prosím, poskytnite URL databázy ako argument.", file=sys.stderr)
        sys.exit(1)
   
    setattr(server, 'db_url', args.database_url)

    parsed_url = urlparse(args.database_url)
    netloc_parts = []
    if parsed_url.username: netloc_parts.append(parsed_url.username)
    if parsed_url.hostname: netloc_parts.append(parsed_url.hostname)
    if parsed_url.port and parsed_url.hostname: netloc_parts.append(f":{parsed_url.port}")
    netloc = ""
    if len(netloc_parts) > 1 and '@' not in netloc_parts[0]:
        netloc = f"{netloc_parts[0]}@{':'.join(netloc_parts[1:])}"
    else:
        netloc = '@'.join(netloc_parts)
    resource_base_url_parsed = parsed_url._replace(
        scheme="postgres",      
        netloc=netloc,         
        path=parsed_url.path,  
        params="",             
        query="",               
        fragment=""             
    )
    resource_base_url = urlunparse(resource_base_url_parsed)
    setattr(server, 'resource_base_url', resource_base_url)


    logger.info(f"Starting MCP server for database: {resource_base_url}")
    try:
        asyncio.run(run_stdio_server())
    except KeyboardInterrupt:
         logger.info("Server shutdown requested.")
    except Exception as e:
         logger.error(f"Server exited with error: {e}", exc_info=True)
         sys.exit(1)
    finally:
         logger.info("Server has shut down.")


if __name__ == "__main__":
    main()