"""Source adapters for strict-open raw ingest."""

from .base import SourceAdapter, StageResult
from .tcp_ds005237 import TCPDS005237SourceAdapter

__all__ = ["SourceAdapter", "StageResult", "TCPDS005237SourceAdapter"]
