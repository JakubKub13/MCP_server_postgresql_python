#!/usr/bin/env python

import sys
import json
import argparse
from loguru import logger
import psycopg
import psycopg_pool
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass

# Use the higher-level FastMCP
from mcp.server.fastmcp import FastMCP, Context

# Configure loguru
logger.remove()
logger.add(sys.stderr, level="DEBUG", colorize=True, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")

# --- Lifespan Management ---

@dataclass
class AppContext:
    """Context to hold shared resources like the connection pool."""
    pool: psycopg_pool.AsyncConnectionPool

@asynccontextmanager
async def app_lifespan(server: FastMCP, db_url: str) -> AsyncIterator[AppContext]:
    """Manage application lifecycle: create and close the pool."""
    pool = None
    logger.info(f"Initializing connection pool for {db_url[:db_url.find('@') + 1 if '@' in db_url else len(db_url)]}...")
    try:
        pool = psycopg_pool.AsyncConnectionPool(
            conninfo=db_url, min_size=1, max_size=5
        )
        await pool.open(wait=True)
        logger.info("Connection pool initialized.")
        yield AppContext(pool=pool) # Yield the context containing the pool
    except Exception as e:
        logger.error(f"Connection pool initialization failed: {e}", exc_info=True)
        raise # Re-raise the exception to stop the server startup
    finally:
        if pool:
            logger.info("Closing connection pool...")
            await pool.close()
            logger.info("Pool closed.")

# Create the FastMCP server instance, passing the lifespan *later*
# We need the db_url first, which comes from args
mcp = FastMCP("PostgreSQL")

# --- Tool Definition ---

@mcp.tool()
async def query(ctx: Context, sql: str) -> str:
    """
    Run a read-only SQL query against the PostgreSQL database.

    Args:
        sql: The SQL query to execute (SELECT queries only)

    Returns:
        The query results as JSON string
    """
    # Access the pool from the lifespan context injected by FastMCP
    app_context = ctx.request_context.lifespan_context
    if not isinstance(app_context, AppContext):
         logger.error("Lifespan context is not the expected AppContext type.")
         raise RuntimeError("Internal server error: Lifespan context not set up correctly.")
    pool = app_context.pool

    logger.info(f"Executing query: {sql[:100]}...")

    # Basic safety check
    if not sql.strip().lower().startswith('select'):
        logger.warning(f"Attempt to run non-SELECT query: {sql[:100]}...")
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
                    return json.dumps(rows_as_dicts, indent=2, default=str)
                else:
                    return json.dumps({"message": "Query executed, no rows returned."}, indent=2)
    except psycopg.Error as db_err:
        logger.error(f"Database error during query execution: {db_err}")
        # Propagate a generic error message, hide specific DB errors
        raise RuntimeError("Database query failed.")
    except Exception as e:
        logger.error(f"Unexpected error during query execution: {e}", exc_info=True)
        raise RuntimeError("An unexpected server error occurred.")
    finally:
        if pool and conn: # Ensure pool exists before releasing
            await pool.release(conn)

# --- Main Execution ---

def main():
    parser = argparse.ArgumentParser(description="MCP PostgreSQL Server (FastMCP API)")
    parser.add_argument("database_url", help="URL pripojenia k PostgreSQL databáze (napr. postgresql://user:pass@host:port/dbname)")
    args = parser.parse_args()

    if not args.database_url:
        logger.error("Database URL is required")
        sys.exit(1)

    # Define a wrapper for the lifespan manager to pass the db_url
    async def lifespan_wrapper(server: FastMCP) -> AsyncIterator[AppContext]:
        async with app_lifespan(server, args.database_url) as context:
            yield context

    # Assign the lifespan wrapper to the mcp instance
    mcp.lifespan = lifespan_wrapper

    logger.info(f"Starting MCP server for database URL: {args.database_url[:args.database_url.find('@') + 1 if '@' in args.database_url else len(args.database_url)]}")

    # Run the FastMCP server with stdio transport
    # No need for transport_args here, lifespan handles the db_url
    mcp.run(transport="stdio")

if __name__ == "__main__":
    main()