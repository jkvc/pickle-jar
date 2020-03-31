import pickle
import os
import io
import shutil

DEFAULT_CHUNK_SIZE = 5000000
PICKLE_EXTENSION = 'pkl'
WRITE_MODE = 'wb'
READ_MODE = 'rb'


def dump(obj, path, chunk_size=DEFAULT_CHUNK_SIZE):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.mkdir(path)

    bytesobj = pickle.dumps(obj)
    num_bytes = len(bytesobj)
    num_chunks = num_bytes // chunk_size + \
        (0 if num_bytes % chunk_size == 0 else 1)

    for i in range(num_chunks):
        filepath = os.path.join(path, f'{i}.{PICKLE_EXTENSION}')
        with open(filepath, WRITE_MODE) as f:
            f.write(bytesobj[i*chunk_size: (i+1)*chunk_size])
    return num_chunks


def load(path):
    assert os.path.exists(path)
    assert os.path.isdir(path)

    idxs = set(
        int(os.path.splitext(filename)[0])
        for filename in os.listdir(path)
        if (
            os.path.isfile(os.path.join(path, filename))
            and
            os.path.splitext(filename)[-1] == f'.{PICKLE_EXTENSION}'
            and
            os.path.splitext(filename)[0].isnumeric()
        )
    )
    max_idx = max(idxs)
    for i in range(max_idx+1):
        assert i in idxs

    ba = bytearray()
    for i in range(max_idx+1):
        filename = os.path.join(path, f'{i}.{PICKLE_EXTENSION}')
        with open(filename, READ_MODE) as f:
            ba += f.read()

    return pickle.loads(ba)
