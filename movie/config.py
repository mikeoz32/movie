from typing import Any, Dict, TypeVar, cast

from movie.utils import ClassLoader

T = TypeVar("T")


def _merge_dicts(primary: Dict[str, Any], fallback: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(fallback)  # Start with fallback values
    for key, value in primary.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _merge_dicts(value, result[key])
        else:
            result[key] = value
    return result


class Config:
    """Configuration settings for the application."""

    @staticmethod
    def from_dict(config_dict: Dict[str, Any]) -> "Config":
        """Create a Config instance from a dictionary.

        Args:
            config_dict (Dict[str, Any]): The configuration dictionary.

        Returns:
            Config: A new Config instance.
        """
        return Config(config_dict)

    @staticmethod
    def from_toml_file(file_path: str, *, required: bool = True) -> "Config":
        """Create a Config instance from a TOML file.

        Args:
            file_path (str): The path to the TOML configuration file.

        Returns:
            Config: A new Config instance.
        """
        import tomllib

        try:
            with open(file_path, "rb") as f:
                config_dict = tomllib.load(f)
        except FileNotFoundError:
            if required:
                raise
            config_dict = {}
        return Config(config_dict)

    @staticmethod
    def from_json_file(file_path: str) -> "Config":
        """Create a Config instance from a JSON file.

        Args:
            file_path (str): The path to the JSON configuration file.

        Returns:
            Config: A new Config instance.
        """
        import json

        with open(file_path, "r", encoding="utf-8") as f:
            config_dict = json.load(f)
        return Config(config_dict)

    def __init__(self, config: Dict[str, Any]) -> None:
        self._dict = config

    def get(self, path: str, default: Any = None) -> Any:
        """Retrieve a configuration value by its path.

        Args:
            path (str): The dot-separated path to the configuration value.
            default (Any, optional): The default value returned when the path is absent.

        Returns:
            Any: The configuration value or the default if not found.
        """
        keys = path.split(".")
        current = self._dict

        for key in keys:
            match current:
                case dict() if key in current:
                    current = current[key]
                case _:
                    return default
        return current

    def get_config(self, path: str) -> "Config | None":
        """Retrieve a nested configuration as a Config object.

        Args:
            path (str): The dot-separated path to the nested configuration.

        Returns:
            Config: A new Config instance for the nested configuration.
        """
        value = self.get(path)
        match value:
            case dict() as d:
                return Config(d)
            case _:
                return None

    def get_string(self, path: str, default: str | None = None) -> str | None:
        """Retrieve a string configuration value by its path.

        Args:
            path (str): The dot-separated path to the configuration value.
            default (str | None, optional): The default value returned when the path is absent.

        Returns:
            str | None: The configuration value as a string or the default if not found.
        """
        value = self.get(path, default)
        match value:
            case str() as s:
                return s
            case _:
                return default

    def get_int(self, path: str, default: int | None = None) -> int | None:
        """Retrieve an integer configuration value by its path.

        Args:
            path (str): The dot-separated path to the configuration value.
            default (int | None, optional): The default value returned when the path is absent.

        Returns:
            int | None: The configuration value as an integer or the default if not found.
        """
        value = self.get(path, default)
        match value:
            case int() as i:
                return i
            case str() as s:
                try:
                    return int(s)
                except ValueError:
                    return default
            case _:
                return default

    def get_instance(self, path: str, cls: T) -> T | None:
        """Retrieve a configuration value and ensure it is an instance of the specified class.

        Args:
            path (str): The dot-separated path to the configuration value.
            cls (Type[T]): The expected class type.

        Returns:
            T | None: The configured class, or None when the value is absent or invalid.
        """
        value = self.get(path)
        match value:
            case str() as class_path:
                result = ClassLoader.load_class(class_path)
                return cast(cls, result)
        return None

    def has_path(self, path: str) -> bool:
        """Check if a configuration path exists.

        Args:
            path (str): The dot-separated path to check.

        Returns:
            bool: True if the path exists, False otherwise.
        """
        keys = path.split(".")
        current = self._dict

        for key in keys:
            match current:
                case dict() if key in current:
                    current = current[key]
                case _:
                    return False
        return True

    def is_empty(self) -> bool:
        """Check if the configuration is empty.

        Returns:
            bool: True if the configuration is empty, False otherwise.
        """
        return not bool(self._dict)

    def with_fallback(self, fallback: "Config") -> "Config":
        """Create a new Config that falls back to another Config for missing values.

        Args:
            fallback (Config): The fallback configuration.

        Returns:
            Config: A new Config instance with fallback behavior.
        """
        combined_dict = _merge_dicts(self._dict, fallback._dict)
        return Config(combined_dict)

    def __repr__(self) -> str:
        return f"Config({self._dict})"
