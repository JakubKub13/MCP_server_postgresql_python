#!/usr/bin/env python3
"""
PostgreSQL Database Client Agent
Implements a ReAct agent interface for database interactions using MultiServerMCPClient.
"""

import asyncio
import os
import sys
from typing import List, Optional, Dict, Any
import argparse
from dataclasses import dataclass
from loguru import logger
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, BaseMessage
from langchain_anthropic import ChatAnthropic
from langgraph.prebuilt import create_react_agent
from langchain_mcp_adapters.client import MultiServerMCPClient

# --- Constants ---
CONFIG = {
    "MODEL": {
        "MAX_TOKENS": 4000,
        "TEMPERATURE": 0
    },
    "LOGGING": {
        "FORMAT": "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        "LEVEL": "INFO"
    }
}

SYSTEM_PROMPT = """You are a helpful assistant with access to a PostgreSQL database via the 'query' tool.
When asked a question that requires database access:
1. Think about the SQL query needed based on the user's request.
2. Construct the SQL query. Remember to use double quotes for table and column names if they contain special characters or are case-sensitive (PostgreSQL standard).
3. Use the 'query' tool with the constructed SQL query.
4. Analyze the result and answer the user's question based on the data returned.
Only use the 'query' tool for database interaction. Ask for clarification if the request is ambiguous."""

@dataclass
class ServerConfig:
    """Server configuration data structure."""
    command: str
    args: List[str]
    transport: str
    env: Optional[Dict[str, str]] = None

class LoggerSetup:
    """Handles logger configuration and setup."""
    
    @staticmethod
    def configure():
        """Configure logger with standard format and colors."""
        logger.remove()
        logger.add(
            sys.stderr,
            level=CONFIG["LOGGING"]["LEVEL"],
            colorize=True,
            format=CONFIG["LOGGING"]["FORMAT"]
        )

class ModelManager:
    """Manages LLM model initialization and configuration."""
    
    @staticmethod
    def initialize_model() -> ChatAnthropic:
        """Initialize and return the language model."""
        try:
            return ChatAnthropic(
                model=os.getenv("ANTHROPIC_MODEL"),
                temperature=CONFIG["MODEL"]["TEMPERATURE"],
                max_tokens=CONFIG["MODEL"]["MAX_TOKENS"]
            )
        except Exception as e:
            logger.error(f"Error initializing LLM: {e}")
            raise

class DatabaseAgent:
    """Manages database interaction through ReAct agent."""
    
    def __init__(self, model: ChatAnthropic, database_url: str):
        self.model = model
        self.database_url = database_url
        self.server_configs = self._create_server_config()

    def _create_server_config(self) -> Dict[str, ServerConfig]:
        """Create server configuration for MultiServerMCPClient."""
        return {
            "postgres_server": ServerConfig(
                command=sys.executable,
                args=["server.py", self.database_url],
                transport="stdio"
            ).__dict__
        }

    async def _setup_tools(self, client: MultiServerMCPClient):
        """Set up and validate tools from MCP client."""
        try:
            mcp_tools = client.get_tools()
            if not mcp_tools:
                raise ValueError("No tools loaded from MCP server")
            
            query_tool = next(
                (t for t in mcp_tools if t.name.endswith("query")),
                None
            )
            if not query_tool:
                raise ValueError(f"Query tool not found in: {[t.name for t in mcp_tools]}")
            
            return mcp_tools
        except Exception as e:
            logger.error(f"Tool setup error: {e}")
            raise

    async def _process_user_input(self, agent, input_messages: List[BaseMessage]):
        """Process user input and stream agent responses."""
        async for chunk in agent.astream(
            {"messages": input_messages},
            stream_mode="values"
        ):
            last_message = chunk.get("messages", [])[-1] if chunk.get("messages") else None
            if last_message and hasattr(last_message, 'content'):
                if isinstance(last_message.content, str):
                    print(last_message.content, end="", flush=True)
        print()

    async def run(self):
        """Run the database agent interaction loop."""
        async with MultiServerMCPClient(self.server_configs) as client:
            try:
                mcp_tools = await self._setup_tools(client)
                agent = create_react_agent(self.model, mcp_tools)
                
                while True:
                    user_input = await asyncio.to_thread(input, "You: ")
                    if user_input.lower() == 'exit':
                        break
                    if not user_input.strip():
                        continue

                    input_messages = [
                        HumanMessage(content=SYSTEM_PROMPT),
                        HumanMessage(content=user_input)
                    ]
                    
                    await self._process_user_input(agent, input_messages)
                    
            except Exception as e:
                logger.error(f"Agent runtime error: {e}", exc_info=True)

def validate_environment():
    """Validate required environment variables."""
    required_vars = ["ANTHROPIC_API_KEY"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.warning(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False
    return True

async def main():
    """Main entry point with proper setup and error handling."""
    parser = argparse.ArgumentParser(description="PostgreSQL Database Client Agent")
    parser.add_argument(
        "database_url",
        help="Database connection URL (postgresql://user:pass@host:port/dbname)"
    )
    args = parser.parse_args()

    try:
        load_dotenv()
        LoggerSetup.configure()
        
        if not validate_environment():
            sys.exit(1)

        model = ModelManager.initialize_model()
        agent = DatabaseAgent(model, args.database_url)
        await agent.run()

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Application error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
