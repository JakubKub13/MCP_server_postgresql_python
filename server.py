#!/usr/bin/env python

import sys
import json
import argparse
from loguru import logger
import psycopg
import psycopg_pool
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from typing import Optional, Dict, List, Any
import asyncio
from dataclasses import dataclass

from mcp.server.fastmcp import FastMCP, Context

# --- Configuration Constants ---
DB_MIN_POOL_SIZE = 1
DB_MAX_POOL_SIZE = 5
QUERY_TIMEOUT_SECONDS = 5.0
LOG_FORMAT = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"

# --- Configure Logger ---
logger.remove()
logger.add(sys.stderr, level="DEBUG", colorize=True, format=LOG_FORMAT)

# --- Data Models ---
@dataclass
class QueryResult:
    """Represents the result of a SQL query."""
    rows: List[Dict[str, Any]]
    message: str = ""
    
    def to_json(self) -> str:
        """Convert query result to JSON string."""
        if not self.rows and self.message:
            return json.dumps({"message": self.message}, indent=2)
        return json.dumps(self.rows, indent=2, default=str)

# --- Database Pool Manager ---
class DatabasePoolManager:
    """Manages the lifecycle of a database connection pool."""
    
    def __init__(self):
        self._pool: Optional[psycopg_pool.AsyncConnectionPool] = None
        self._initialized = asyncio.Event()
    
    @property
    def is_initialized(self) -> bool:
        """Check if the pool is initialized and ready to use."""
        return self._initialized.is_set() and self._pool is not None
    
    def get_pool(self) -> Optional[psycopg_pool.AsyncConnectionPool]:
        """Get the connection pool if initialized."""
        return self._pool
    
    @staticmethod
    def _sanitize_db_url(db_url: str) -> str:
        """Returns a version of the database URL safe for logging."""
        try:
            at_index = db_url.find('@')
            if at_index != -1:
                return db_url[:db_url.find('://') + 3] + '*****@' + db_url[at_index + 1:]
            return "DB URL (credentials hidden or missing)"
        except Exception:
            return "Invalid DB URL format"
    
    async def initialize(self, db_url: str) -> None:
        """Initialize the database pool."""
        safe_db_url = self._sanitize_db_url(db_url)
        logger.info(f"Initializing connection pool for {safe_db_url}...")
        
        try:
            pool = psycopg_pool.AsyncConnectionPool(
                conninfo=db_url,
                min_size=DB_MIN_POOL_SIZE,
                max_size=DB_MAX_POOL_SIZE,
            )
            await pool.open(wait=True)
            logger.info("Connection pool initialized successfully.")
            
            self._pool = pool
            self._initialized.set()
            
        except Exception as e:
            logger.error(f"Connection pool initialization failed: {e}", exc_info=True)
            raise
    
    async def wait_for_initialization(self, timeout: float = QUERY_TIMEOUT_SECONDS) -> None:
        """Wait for the pool to be initialized with a timeout."""
        if not self._initialized.is_set():
            logger.warning("Pool not yet initialized, waiting...")
            try:
                await asyncio.wait_for(self._initialized.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.error("Timeout waiting for pool to initialize")
                raise RuntimeError("Database pool initialization timed out")
    
    async def close(self) -> None:
        """Close the connection pool."""
        if self._pool:
            logger.info("Closing connection pool...")
            await self._pool.close()
            logger.info("Pool closed.")
            self._pool = None
            self._initialized.clear()

# --- Query Execution ---
class QueryExecutor:
    """Executes SQL queries using the provided database pool."""
    
    def __init__(self, pool_manager: DatabasePoolManager):
        self.pool_manager = pool_manager
    
    @staticmethod
    def _validate_select_query(sql: str) -> None:
        """Validate that the query is a SELECT query."""
        if not sql.strip().lower().startswith('select'):
            logger.warning(f"Attempt to run non-SELECT query: {sql[:100]}...")
            raise ValueError("Only read-only (SELECT) queries are allowed.")
    
    @staticmethod
    async def _format_results(cur) -> QueryResult:
        """Format cursor results into a QueryResult object."""
        if cur.description:
            results = await cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            rows = [dict(zip(column_names, row)) for row in results]
            return QueryResult(rows=rows)
        else:
            return QueryResult(rows=[], message="Query executed, no rows returned.")
    
    async def execute_query(self, sql: str) -> QueryResult:
        """Execute a read-only SQL query and return the results."""
        # Ensure pool is ready
        await self.pool_manager.wait_for_initialization()
        pool = self.pool_manager.get_pool()
        
        if pool is None:
            logger.error("Database pool is None after initialization check!")
            raise RuntimeError("Database pool not available")
        
        # Validate query is SELECT
        self._validate_select_query(sql)
        
        logger.info(f"Executing query: {sql[:100]}...")
        
        try:
            async with pool.connection() as conn:
                conn.readonly = True
                async with conn.cursor() as cur:
                    await cur.execute(sql)
                    return await self._format_results(cur)
        except psycopg.Error as db_err:
            logger.error(f"Database error during query execution: {db_err}")
            err_msg = str(db_err)
            if hasattr(db_err, 'diag') and hasattr(db_err.diag, 'message_primary'):
                err_msg = db_err.diag.message_primary
            raise RuntimeError(f"Database query failed: {err_msg}")
        except Exception as e:
            logger.error(f"Unexpected error during query execution: {e}", exc_info=True)
            raise RuntimeError("An unexpected server error occurred")

# --- Application Setup ---
# Create shared instances
db_manager = DatabasePoolManager()
query_executor = QueryExecutor(db_manager)

# Create the FastMCP server instance
mcp = FastMCP("PostgreSQL")

# --- Lifespan Management ---
@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[None]:
    """Simple lifespan manager - only handles shutdown cleanup."""
    logger.info("Lifespan started - pool already initialized.")
    
    try:
        yield  # Yield to run the application
    finally:
        # Cleanup on shutdown
        await db_manager.close()

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
    try:
        result = await query_executor.execute_query(sql)
        return result.to_json()
    except ValueError as e:
        # Re-raise validation errors with the same message
        raise ValueError(str(e))
    except RuntimeError as e:
        # Re-raise runtime errors with the same message
        raise RuntimeError(str(e))
    except Exception as e:
        # Catch any unexpected errors
        logger.error(f"Unexpected error in query tool: {e}", exc_info=True)
        raise RuntimeError("An unexpected server error occurred")

# --- Command-line interface ---
def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="MCP PostgreSQL Server (FastMCP API)")
    parser.add_argument(
        "database_url", 
        help="URL pripojenia k PostgreSQL databáze (napr. postgresql://user:pass@host:port/dbname)"
    )
    args = parser.parse_args()
    
    if not args.database_url:
        logger.error("Database URL is required")
        sys.exit(1)
        
    if not args.database_url.startswith("postgresql://"):
        logger.error("Invalid database URL format. Must start with postgresql://")
        sys.exit(1)
        
    return args

# --- Main Execution ---
def main():
    """Main entry point for the application."""
    args = parse_arguments()
    
    try:
        # Initialize the pool immediately, before the server starts
        asyncio.run(db_manager.initialize(args.database_url))
        
        # Setup lifespan wrapper
        async def lifespan_wrapper(server: FastMCP) -> AsyncIterator[None]:
            async with app_lifespan(server):
                yield None
        
        # Assign the lifespan wrapper and start server
        mcp.lifespan = lifespan_wrapper
        
        safe_db_url = db_manager._sanitize_db_url(args.database_url)
        logger.info(f"Starting MCP server for database URL: {safe_db_url}")
        
        # Run the FastMCP server
        mcp.run(transport="stdio")
    except Exception as e:
        logger.error(f"Failed to start server: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()