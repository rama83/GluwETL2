"""
Configuration management for the AWS Data Lake Framework.
Handles loading and validating configuration from YAML files and environment variables.
"""
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Default paths
CONFIG_DIR = Path(__file__).parent
DEFAULT_CONFIG_PATH = CONFIG_DIR / "settings.yaml"
ENV_CONFIG_PREFIX = "GLUE_ETL_"


class Config:
    """Configuration manager for the AWS Data Lake Framework."""

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the configuration manager.

        Args:
            config_path: Path to the configuration file. If None, uses the default.
        """
        self.config_path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
        self.config: Dict[str, Any] = {}
        self.load_config()

    def load_config(self) -> None:
        """Load configuration from the YAML file and override with environment variables."""
        # Load from YAML file
        if self.config_path.exists():
            with open(self.config_path, "r") as f:
                self.config = yaml.safe_load(f) or {}
        else:
            self.config = {}

        # Override with environment variables
        self._override_from_env()

    def _override_from_env(self) -> None:
        """Override configuration values with environment variables."""
        for key, value in os.environ.items():
            if key.startswith(ENV_CONFIG_PREFIX):
                # Convert GLUE_ETL_AWS_REGION to aws.region
                config_key = key[len(ENV_CONFIG_PREFIX):].lower().replace("_", ".")
                self._set_nested_dict(self.config, config_key, value)

    def _set_nested_dict(self, d: Dict[str, Any], key_path: str, value: Any) -> None:
        """
        Set a value in a nested dictionary using a dot-separated key path.

        Args:
            d: The dictionary to modify
            key_path: Dot-separated path (e.g., 'aws.region')
            value: Value to set
        """
        keys = key_path.split(".")
        for key in keys[:-1]:
            if key not in d or not isinstance(d[key], dict):
                d[key] = {}
            d = d[key]
        d[keys[-1]] = value

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get a configuration value using a dot-separated key path.

        Args:
            key_path: Dot-separated path (e.g., 'aws.region')
            default: Default value if the key doesn't exist

        Returns:
            The configuration value or the default
        """
        keys = key_path.split(".")
        value = self.config
        for key in keys:
            if not isinstance(value, dict) or key not in value:
                return default
            value = value[key]
        return value

    def set(self, key_path: str, value: Any) -> None:
        """
        Set a configuration value using a dot-separated key path.

        Args:
            key_path: Dot-separated path (e.g., 'aws.region')
            value: Value to set
        """
        self._set_nested_dict(self.config, key_path, value)

    def save(self, config_path: Optional[str] = None) -> None:
        """
        Save the current configuration to a YAML file.

        Args:
            config_path: Path to save the configuration file. If None, uses the current path.
        """
        save_path = Path(config_path) if config_path else self.config_path
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "w") as f:
            yaml.dump(self.config, f, default_flow_style=False)


# Global configuration instance
config = Config()