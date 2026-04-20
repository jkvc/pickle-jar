# MIT License
# Copyright (c) 2020 Junshen Kevin Chen
# See LICENSE for details.

from __future__ import annotations

import os
import pickle
import shutil
from pathlib import Path
from typing import Any

DEFAULT_CHUNK_SIZE: int = 5_000_000
_PICKLE_EXT: str = "pkl"


def dump(obj: Any, path: str | os.PathLike[str], chunk_size: int = DEFAULT_CHUNK_SIZE) -> int:
    """Serialize an object to disk as chunked pickle slices.

    The object is pickled, then the resulting bytes are split into
    ``chunk_size``-byte files inside a directory at *path*.

    Args:
        obj: Any picklable Python object.
        path: Directory to create (or overwrite) on disk.
        chunk_size: Max bytes per slice file. Defaults to 5 MB.

    Returns:
        Number of chunk files written.

    Raises:
        ValueError: If *chunk_size* is not a positive integer.
    """
    if not isinstance(chunk_size, int) or chunk_size <= 0:
        raise ValueError(f"chunk_size must be a positive integer, got {chunk_size!r}")

    path = Path(path)

    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)

    data = pickle.dumps(obj)
    num_bytes = len(data)
    num_chunks = -(-num_bytes // chunk_size)  # ceiling division

    for i in range(num_chunks):
        chunk_path = path / f"{i}.{_PICKLE_EXT}"
        chunk_path.write_bytes(data[i * chunk_size : (i + 1) * chunk_size])

    return num_chunks


def load(path: str | os.PathLike[str]) -> Any:
    """Reassemble and deserialize an object from chunked pickle slices.

    .. warning::

        Like ``pickle.loads``, this executes arbitrary code embedded in the
        pickle stream.  **Never load data from untrusted sources.**

    Args:
        path: Directory previously created by :func:`dump`.

    Returns:
        The deserialized Python object.

    Raises:
        FileNotFoundError: If *path* does not exist.
        NotADirectoryError: If *path* is not a directory.
        ValueError: If chunk indices are not contiguous starting from 0.
    """
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Jar directory not found: {path}")
    if not path.is_dir():
        raise NotADirectoryError(f"Expected a directory, got a file: {path}")

    indices: set[int] = set()
    for entry in path.iterdir():
        if entry.is_file() and entry.suffix == f".{_PICKLE_EXT}" and entry.stem.isdigit():
            indices.add(int(entry.stem))

    if not indices:
        raise ValueError(f"No pickle chunks found in {path}")

    max_idx = max(indices)
    expected = set(range(max_idx + 1))
    missing = expected - indices
    if missing:
        raise ValueError(f"Missing chunk indices in {path}: {sorted(missing)}")

    buf = bytearray()
    for i in range(max_idx + 1):
        buf += (path / f"{i}.{_PICKLE_EXT}").read_bytes()

    return pickle.loads(buf)  # noqa: S301
