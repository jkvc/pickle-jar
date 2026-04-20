# pickle-jar

[![PyPI](https://img.shields.io/pypi/v/pickle-jar)](https://pypi.org/project/pickle-jar/)
[![Python](https://img.shields.io/pypi/pyversions/pickle-jar)](https://pypi.org/project/pickle-jar/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A container of pickle slices.

Serialize Python objects to disk the same way you'd use `pickle` — but instead
of one monolithic file, **pickle-jar splits the output into small, numbered
chunks** inside a directory. This makes it easy to commit large serialized
objects (ML model weights, embeddings, datasets) to Git repositories that
enforce per-file size limits.

## Install

```bash
pip install pickle-jar
```

## Quick start

```python
import jar

# Save any picklable object
jar.dump(my_model.state_dict(), "model_weights")

# Load it back
weights = jar.load("model_weights")
```

The call above creates a directory called `model_weights/` containing numbered
chunk files (`0.pkl`, `1.pkl`, …). Each chunk defaults to **5 MB** — small
enough for GitHub's file-size limits.

## API

### `jar.dump(obj, path, chunk_size=5_000_000)`

Serialize `obj` and write it as chunked `.pkl` files inside `path`.

| Parameter | Type | Description |
|---|---|---|
| `obj` | `Any` | Any picklable Python object. |
| `path` | `str \| Path` | Directory to create (overwritten if it exists). |
| `chunk_size` | `int` | Max bytes per chunk file. Default `5_000_000` (5 MB). |

Returns the number of chunk files written.

### `jar.load(path)`

Reassemble and deserialize an object from a jar directory.

| Parameter | Type | Description |
|---|---|---|
| `path` | `str \| Path` | Directory previously created by `jar.dump`. |

Returns the deserialized Python object.

## Tuning chunk size

```python
# Smaller chunks for strict hosting limits
jar.dump(obj, "output", chunk_size=1_000_000)   # 1 MB per file

# Larger chunks when size limits aren't a concern
jar.dump(obj, "output", chunk_size=50_000_000)  # 50 MB per file
```

## Security warning

> **pickle-jar uses Python's `pickle` module under the hood.**
> `pickle.loads()` can execute arbitrary code. **Never load jar directories
> from untrusted sources.** This is the same caveat that applies to `pickle`,
> `torch.load`, and similar serialization tools.

## Development

```bash
# Clone and set up
git clone https://github.com/jkvc/pickle-jar.git
cd pickle-jar
uv venv .venv && source .venv/bin/activate
uv pip install -e ".[dev]"

# Run tests & lint
pytest tests/ -v
ruff check .
```

## License

[MIT](LICENSE)
