#!/usr/bin/env python

import sys
import json
import argparse
from loguru import logger
import psycopg
import psycopg_pool
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from typing import Dict, Any, Optional
import asyncio

# Use the higher-level FastMCP
from mcp.server.fastmcp import FastMCP, Context

# --- Module-Level Variable for DB Pool ---
_db_pool: Optional[psycopg_pool.AsyncConnectionPool] = None
_pool_initialized = asyncio.Event()  # Event to signal when pool is ready

# Configure loguru
logger.remove()
logger.add(sys.stderr, level="DEBUG", colorize=True, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")


# --- Helper Functions (Example: could add safe URL here if needed) ---
def _get_safe_db_url(db_url: str) -> str:
    """Returns a version of the database URL safe for logging."""
    try:
        at_index = db_url.find('@')
        if at_index != -1:
            return db_url[:db_url.find('://') + 3] + '*****@' + db_url[at_index + 1:]
        return "DB URL (credentials hidden or missing)"
    except Exception:
        return "Invalid DB URL format"


# --- Initialize pool at server startup (not in lifespan) ---
async def initialize_db_pool(db_url: str) -> None:
    """Initialize the database pool at server startup."""
    global _db_pool
    safe_db_url = _get_safe_db_url(db_url)
    
    logger.info(f"Initializing connection pool for {safe_db_url}...")
    try:
        pool = psycopg_pool.AsyncConnectionPool(
            conninfo=db_url,
            min_size=1,
            max_size=5,
        )
        # Open the pool and wait for it to be ready
        await pool.open(wait=True)
        logger.info("Connection pool initialized successfully.")
        
        # Store the pool in the module-level variable
        _db_pool = pool
        # Signal that pool is initialized
        _pool_initialized.set()
        
    except Exception as e:
        logger.error(f"Connection pool initialization failed: {e}", exc_info=True)
        sys.exit(1)  # Exit if we can't initialize the pool


# --- Simplified Lifespan Management (just for cleanup) ---
@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[None]:
    """Simple lifespan manager - only handles shutdown cleanup."""
    global _db_pool
    
    # Pool is already initialized before lifespan starts
    logger.info("Lifespan started - pool already initialized.")
    
    try:
        yield  # Yield to run the application
    finally:
        # Cleanup on shutdown
        if _db_pool:
            logger.info("Closing connection pool...")
            await _db_pool.close()
            logger.info("Pool closed.")
            _db_pool = None
            _pool_initialized.clear()


# Create the FastMCP server instance
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
    global _db_pool, _pool_initialized
    
    # Wait for pool to be initialized (should already be done, but just to be safe)
    if not _pool_initialized.is_set():
        logger.warning("Pool not yet initialized, waiting...")
        try:
            # Wait with timeout to avoid hanging indefinitely
            await asyncio.wait_for(_pool_initialized.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.error("Timeout waiting for pool to initialize")
            raise RuntimeError("Database pool initialization timed out")
    
    # Check if we have a valid pool
    if _db_pool is None:
        logger.error("Database pool is None even though initialization event is set!")
        raise RuntimeError("Database pool not available after initialization")
    
    logger.info(f"Executing query: {sql[:100]}...")

    # Basic safety check
    if not sql.strip().lower().startswith('select'):
        logger.warning(f"Attempt to run non-SELECT query: {sql[:100]}...")
        raise ValueError("Only read-only (SELECT) queries are allowed.")

    # Use the pool with the reliable async with context manager for connections
    try:
        async with _db_pool.connection() as conn:
            conn.readonly = True
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
        err_msg = str(db_err)
        if hasattr(db_err, 'diag') and hasattr(db_err.diag, 'message_primary'):
             err_msg = db_err.diag.message_primary
        raise RuntimeError(f"Database query failed: {err_msg}")
    except Exception as e:
        logger.error(f"Unexpected error during query execution: {e}", exc_info=True)
        raise RuntimeError("An unexpected server error occurred during query execution.")


# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description="MCP PostgreSQL Server (FastMCP API)")
    parser.add_argument("database_url", help="URL pripojenia k PostgreSQL databáze (napr. postgresql://user:pass@host:port/dbname)")
    args = parser.parse_args()

    if not args.database_url:
        logger.error("Database URL is required")
        sys.exit(1)

    try:
        # Validate database URL format
        if not args.database_url.startswith("postgresql://"):
            logger.error("Invalid database URL format. Must start with postgresql://")
            sys.exit(1)

        # Initialize the pool immediately, before the server starts
        asyncio.run(initialize_db_pool(args.database_url))
        
        # Simplified lifespan just for cleanup
        async def lifespan_wrapper(server: FastMCP) -> AsyncIterator[None]:
            async with app_lifespan(server):
                yield None

        # Assign the lifespan wrapper
        mcp.lifespan = lifespan_wrapper

        # Log startup with masked URL
        safe_db_url = _get_safe_db_url(args.database_url)
        logger.info(f"Starting MCP server for database URL: {safe_db_url}")

        # Run the FastMCP server
        mcp.run(transport="stdio")
    except Exception as e:
        logger.error(f"Failed to start server: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()