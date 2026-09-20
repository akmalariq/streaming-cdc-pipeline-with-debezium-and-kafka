"""Streaming CDC pipeline package."""

from cdc_pipeline.config import Settings, get_settings
from cdc_pipeline.events import ChangeEvent, DecodeError, decode_debezium

__all__ = ["ChangeEvent", "DecodeError", "Settings", "decode_debezium", "get_settings"]
__version__ = "0.1.0"
