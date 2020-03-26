import pickle
import os
import io
import shutil

DEFAULT_CHUNK_SIZE = 40000000


def write_obj(obj, path, chunk_size=DEFAULT_CHUNK_SIZE, force_overwrite=True):
    if force_overwrite:
        if os.path.exists(path):
            shutil.rmtree(path)
    os.mkdir(path)  # can raise

    bytesobj = pickle.dumps(obj)
    num_bytes = len(bytesobj)
    num_chunks = num_bytes // chunk_size + \
        (0 if num_bytes % chunk_size == 0 else 1)

    for chunk_idx in range(num_chunks):
        filepath = os.path.join(path, f'{chunk_idx}.pkl')
        with open(filepath, 'wb') as f:
            f.write(bytesobj[chunk_idx*chunk_size: (chunk_idx+1)*chunk_size])

    return num_chunks


class ChunkWriter(io.IOBase):
    def __init__(self, path, chunk_size=DEFAULT_CHUNK_SIZE, force_overwrite=True):
        super().__init__()

        if force_overwrite:
            if os.path.exists(path):
                shutil.rmtree(path)
        os.mkdir(path)  # can raise

        self.path = path
        self.chunk_size = chunk_size
        self.bytestrings = []
        self.write_chunk_idx = 0

    def write(self, b):
        self.bytestrings.append(b)
        return len(b)

    def flush(self):
        bytesobj = b''.join(self.bytestrings)
        num_bytes = len(bytesobj)
        num_chunks = num_bytes // self.chunk_size + \
            (0 if num_bytes % self.chunk_size == 0 else 1)
        for chunk_idx in range(self.write_chunk_idx, self.write_chunk_idx+num_chunks):
            filepath = os.path.join(self.path, f'{chunk_idx}.pkl')
            with open(filepath, 'wb') as f:
                f.write(
                    bytesobj[chunk_idx*self.chunk_size: (chunk_idx+1)*self.chunk_size])
        self.write_chunk_idx += num_chunks
        self.bytestrings = []

    def close(self):
        self.flush()


def read_as_bytes(path):
    assert os.path.exists(path)
    assert os.path.isdir(path)

    filenames = sorted([f for f in os.listdir(
        path) if os.path.isfile(os.path.join(path, f))])

    bytestrings = []
    for filename in filenames:
        filepath = os.path.join(path, filename)
        with open(filepath, 'rb') as f:
            b = f.read()
            bytestrings.append(b)
    bytesobj = b''.join(bytestrings)
    return bytesobj


def read_as_obj(path):
    bytesobj = read_as_bytes(path)
    return pickle.loads(bytesobj)


def read_as_file(path):
    bytesobj = read_as_bytes(path)
    return io.BytesIO(bytesobj)
