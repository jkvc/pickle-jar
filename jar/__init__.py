"""pickle-jar — chunked pickle serialization."""

from .jar import DEFAULT_CHUNK_SIZE, dump, load

__all__ = ["dump", "load", "DEFAULT_CHUNK_SIZE"]
