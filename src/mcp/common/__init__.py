"""
MCP Common module providing shared functionality and utilities.
"""

from typing import Any, Dict, Optional

__version__ = "0.1.0"

class MCPError(Exception):
    """Base exception class for MCP-related errors."""
    pass

class ConfigurationError(MCPError):
    """Raised when there's a configuration-related error."""
    pass

def validate_config(config: Dict[str, Any]) -> None:
    """
    Validate MCP configuration.
    
    Args:
        config: Dictionary containing configuration parameters
        
    Raises:
        ConfigurationError: If configuration is invalid
    """
    required_fields = ['host', 'port']
    for field in required_fields:
        if field not in config:
            raise ConfigurationError(f"Missing required configuration field: {field}") 