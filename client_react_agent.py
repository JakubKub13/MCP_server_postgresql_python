import asyncio
import os
import sys
import argparse

from loguru import logger
from dotenv import load_dotenv

# Remove direct ClientSession/stdio_client imports if no longer needed elsewhere
# from mcp import ClientSession, StdioServerParameters
# from mcp.client.stdio import stdio_client

from langchain_core.messages import HumanMessage, BaseMessage
from langchain_anthropic import ChatAnthropic
from langgraph.prebuilt import create_react_agent
# Import the MultiServer client and the tools loader
from langchain_mcp_adapters.client import MultiServerMCPClient
# Keep load_mcp_tools import if needed, but MultiServerMCPClient has its own get_tools()
# from langchain_mcp_adapters.tools import load_mcp_tools

load_dotenv()

# Remove default handler and configure loguru with colors
logger.remove()
logger.add(sys.stderr, level="INFO", colorize=True, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")

DOCKER_IMAGE_TAG = "mcp-postgres-py"

try:
    model = ChatAnthropic(
        model=os.getenv("ANTHROPIC_MODEL"), 
        temperature=0,
        max_tokens=4000
    )

except Exception as e:
    logger.error(f"Error initializing LLM: {e}")
    sys.exit(1)



async def run_client_agent(database_url: str):
    """
    Connects to a locally running MCP server using MultiServerMCPClient,
    creates a ReAct agent and starts the conversation loop.
    """
    logger.info("Preparing to connect to local MCP server...")

    # Define the server configuration for MultiServerMCPClient
    # Use the command to run the local server.py script
    # Make sure 'server.py' is accessible or provide the full path.
    server_configs = {
        "postgres_server": { # Logical name for the server connection
            "command": sys.executable, # Use the current Python interpreter
            "args": ["server.py", database_url], # Command to run the server
            "transport": "stdio",
            # Add "env": {} if specific environment variables are needed for the server
        }
    }
    logger.info(f"Server configs for MultiServerMCPClient: {server_configs}")

    try:
        logger.info("DEBUG: ==> Entering 'async with MultiServerMCPClient' block...")
        # Use MultiServerMCPClient as an async context manager
        async with MultiServerMCPClient(server_configs) as client:
            logger.info("DEBUG: <== MultiServerMCPClient connection established.")

            # Get tools using the client's method - this handles initialization internally
            logger.info("Loading tools using client.get_tools()...")
            try:
                mcp_tools = client.get_tools() # Use the client's method
                if not mcp_tools:
                    logger.error("Failed to load any tools from MCP server via MultiServerMCPClient.")
                    return
                logger.info(f"Tools loaded successfully via MultiServerMCPClient: {[tool.name for tool in mcp_tools]}")

                # Check for the 'query' tool (name might include server prefix, e.g., 'postgres_server_query')
                # Let's print the actual tool names first
                logger.info(f"Full tool names loaded: {[t.name for t in mcp_tools]}")
                # Adapt the check if needed based on the actual name format
                query_tool_name = "query" # Or potentially "postgres_server_query" - check logs
                query_tool = next((t for t in mcp_tools if t.name.endswith(query_tool_name)), None)

                if not query_tool:
                     logger.error(f"Required tool ending with '{query_tool_name}' not found among loaded tools: {[t.name for t in mcp_tools]}")
                     return
                logger.info(f"Found query tool: {query_tool.name}")

            except Exception as e:
                 logger.error(f"Error during client.get_tools(): {e!r}", exc_info=True)
                 logger.error(f"Exception Type: {type(e).__name__}")
                 logger.error(f"Exception Args: {e.args}")
                 return

            # --- Agent Setup and Loop (remains largely the same) ---
            try:
                system_prompt = """You are a helpful assistant with access to a PostgreSQL database via the 'query' tool.
                When asked a question that requires database access:
                1. Think about the SQL query needed based on the user's request.
                2. Construct the SQL query. Remember to use double quotes for table and column names if they contain special characters or are case-sensitive (PostgreSQL standard). Assume public schema if not specified otherwise by user, but it's safer to query information_schema if unsure about table structure first.
                3. Use the 'query' tool with the constructed SQL query.
                4. Analyze the result and answer the user's question based on the data returned.
                Only use the 'query' tool for database interaction. Ask for clarification if the request is ambiguous."""

                agent = create_react_agent(model, mcp_tools) # Pass the tools loaded by the client

                logger.info("ReAct agent created successfully.")
                print("\n--- Agent is ready. Enter your request or 'exit' to end the conversation. ---")

            except Exception as e:
                logger.error(f"Error creating agent: {e}")
                return

            while True:
                try:
                    user_input = await asyncio.to_thread(input, "You: ")
                    if user_input.lower() == 'exit':
                        logger.info("Ending client.")
                        break
                    if not user_input.strip():
                        continue

                    print("\nAgent:", flush=True)
                    input_messages: list[BaseMessage] = [
                        HumanMessage(content=system_prompt),
                        HumanMessage(content=user_input)
                    ]

                    final_answer = ""
                    async for chunk in agent.astream(
                        {"messages": input_messages},
                        stream_mode="values"
                    ):
                        last_message = chunk.get("messages", [])[-1] if chunk.get("messages") else None
                        if last_message and isinstance(last_message, HumanMessage):
                             pass
                        elif last_message:
                            if hasattr(last_message, 'content') and isinstance(last_message.content, str):
                                print(last_message.content, end="", flush=True)
                                if last_message.type == 'ai' and last_message.response_metadata.get('finish_reason') == 'stop':
                                    final_answer = last_message.content
                                    # Don't print final answer twice if streaming works
                            else:
                                 print(f"\n[{last_message.type}] {last_message.content}", flush=True) # Debug other messages

                    print() # New line after agent response

                except Exception as e:
                    logger.error(f"Error during conversation with agent: {e}", exc_info=True)
                    break

        logger.info("DEBUG: <== Exited 'async with MultiServerMCPClient' block.") # Log exit

    except ConnectionRefusedError:
         logger.error("Connection refused. Ensure the local server process is running.")
    except FileNotFoundError:
         logger.error(f"Command '{sys.executable}' or script 'server.py' not found.")
    except Exception as e:
        logger.error(f"Unexpected error setting up MultiServerMCPClient: {e!r}", exc_info=True)
    finally:
        logger.info("Client ended.")
        
# --- Main entry point of the script ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MCP Client Agent using MultiServerMCPClient")
    parser.add_argument(
        "database_url",
        help="Database connection URL passed to the MCP server (e.g., 'postgresql://user:pass@host:port/dbname')"
    )
    args = parser.parse_args()

    if not os.getenv("ANTHROPIC_API_KEY") and not os.getenv("OPENAI_API_KEY"): # Add more as needed
        logger.warning("LLM API key not found in .env or environment variables.")

    try:
        asyncio.run(run_client_agent(args.database_url))
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
