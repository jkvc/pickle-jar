from __future__ import annotations

import pickle
from pathlib import Path

import pytest

import jar

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def jar_dir(tmp_path: Path) -> Path:
    """Return a fresh temporary directory for each test."""
    return tmp_path / "jar_output"


# ---------------------------------------------------------------------------
# dump / load round-trip — core types (no heavy deps)
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_string(self, jar_dir: Path) -> None:
        jar.dump("hello world", jar_dir)
        assert jar.load(jar_dir) == "hello world"

    def test_empty_string(self, jar_dir: Path) -> None:
        jar.dump("", jar_dir)
        assert jar.load(jar_dir) == ""

    def test_dict(self, jar_dir: Path) -> None:
        d = {"a": 1, "b": [2, 3], "c": {"nested": True}}
        jar.dump(d, jar_dir)
        assert jar.load(jar_dir) == d

    def test_list(self, jar_dir: Path) -> None:
        lst = list(range(1000))
        jar.dump(lst, jar_dir)
        assert jar.load(jar_dir) == lst

    def test_none(self, jar_dir: Path) -> None:
        jar.dump(None, jar_dir)
        assert jar.load(jar_dir) is None

    def test_bytes(self, jar_dir: Path) -> None:
        data = b"\x00\xff" * 500
        jar.dump(data, jar_dir)
        assert jar.load(jar_dir) == data

    def test_nested_structure(self, jar_dir: Path) -> None:
        obj = {"users": [{"name": "a", "scores": [1, 2, 3]}, {"name": "b", "scores": []}]}
        jar.dump(obj, jar_dir)
        assert jar.load(jar_dir) == obj


# ---------------------------------------------------------------------------
# Chunking behaviour
# ---------------------------------------------------------------------------


class TestChunking:
    def test_single_chunk(self, jar_dir: Path) -> None:
        jar.dump("small", jar_dir, chunk_size=999_999)
        chunks = list(jar_dir.glob("*.pkl"))
        assert len(chunks) == 1
        assert jar.load(jar_dir) == "small"

    def test_many_small_chunks(self, jar_dir: Path) -> None:
        data = list(range(200))
        num = jar.dump(data, jar_dir, chunk_size=50)
        assert num > 1
        chunks = list(jar_dir.glob("*.pkl"))
        assert len(chunks) == num
        assert jar.load(jar_dir) == data

    def test_chunk_size_one(self, jar_dir: Path) -> None:
        """Every byte in its own file — extreme but valid."""
        jar.dump("ab", jar_dir, chunk_size=1)
        loaded = jar.load(jar_dir)
        assert loaded == "ab"

    def test_exact_multiple(self, jar_dir: Path) -> None:
        """Data length is an exact multiple of chunk_size."""
        data = b"x" * 100
        pickled_len = len(pickle.dumps(data))
        jar.dump(data, jar_dir, chunk_size=pickled_len)
        assert len(list(jar_dir.glob("*.pkl"))) == 1
        assert jar.load(jar_dir) == data

    def test_returns_chunk_count(self, jar_dir: Path) -> None:
        n = jar.dump("hello", jar_dir)
        assert isinstance(n, int)
        assert n >= 1


# ---------------------------------------------------------------------------
# Path handling
# ---------------------------------------------------------------------------


class TestPathHandling:
    def test_str_path(self, tmp_path: Path) -> None:
        p = str(tmp_path / "str_jar")
        jar.dump(42, p)
        assert jar.load(p) == 42

    def test_pathlib_path(self, tmp_path: Path) -> None:
        p = tmp_path / "pathlib_jar"
        jar.dump(42, p)
        assert jar.load(p) == 42

    def test_overwrites_existing(self, jar_dir: Path) -> None:
        jar.dump("first", jar_dir)
        jar.dump("second", jar_dir)
        assert jar.load(jar_dir) == "second"

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        deep = tmp_path / "a" / "b" / "c" / "jar"
        jar.dump("deep", deep)
        assert jar.load(deep) == "deep"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrors:
    def test_load_missing_path(self) -> None:
        with pytest.raises(FileNotFoundError):
            jar.load("/tmp/does_not_exist_pickle_jar_test")

    def test_load_file_not_dir(self, tmp_path: Path) -> None:
        f = tmp_path / "not_a_dir.txt"
        f.write_text("hi")
        with pytest.raises(NotADirectoryError):
            jar.load(f)

    def test_load_empty_dir(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="No pickle chunks"):
            jar.load(tmp_path)

    def test_load_missing_chunk(self, jar_dir: Path) -> None:
        jar.dump("data", jar_dir, chunk_size=1)
        # Remove a middle chunk to create a gap
        chunks = sorted(jar_dir.glob("*.pkl"))
        if len(chunks) > 2:
            chunks[1].unlink()
            with pytest.raises(ValueError, match="Missing chunk"):
                jar.load(jar_dir)

    def test_dump_invalid_chunk_size_zero(self, jar_dir: Path) -> None:
        with pytest.raises(ValueError, match="positive integer"):
            jar.dump("x", jar_dir, chunk_size=0)

    def test_dump_invalid_chunk_size_negative(self, jar_dir: Path) -> None:
        with pytest.raises(ValueError, match="positive integer"):
            jar.dump("x", jar_dir, chunk_size=-1)

    def test_dump_invalid_chunk_size_float(self, jar_dir: Path) -> None:
        with pytest.raises(ValueError, match="positive integer"):
            jar.dump("x", jar_dir, chunk_size=3.14)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Optional heavy-dep tests (numpy)
# ---------------------------------------------------------------------------


class TestNumpy:
    np = pytest.importorskip("numpy")

    def test_numpy_array(self, jar_dir: Path) -> None:
        a = self.np.random.rand(50, 50, 50)
        jar.dump(a, jar_dir)
        loaded = jar.load(jar_dir)
        assert (a == loaded).all()

    def test_numpy_large_chunked(self, jar_dir: Path) -> None:
        a = self.np.zeros((100, 100))
        n = jar.dump(a, jar_dir, chunk_size=1000)
        assert n > 1
        loaded = jar.load(jar_dir)
        assert (a == loaded).all()
