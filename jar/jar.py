# MIT License

# Copyright (c) 2020 Junshen Kevin Chen

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import os
import pickle
import shutil

DEFAULT_CHUNK_SIZE = 5000000
PICKLE_EXTENSION = "pkl"
WRITE_MODE = "wb"
READ_MODE = "rb"


def dump(obj, path, chunk_size=DEFAULT_CHUNK_SIZE):
    """Save a pickle-able object to disk, in chunks

    Arguments:
        obj {any} -- the object to picklize
        path {str} -- path name on disk to save object

    Keyword Arguments:
        chunk_size {int} -- how large each individual chunk is when saved
            to disk (default: {DEFAULT_CHUNK_SIZE})

    Returns:
        int -- number of chunk saved to disk
    """
    if os.path.exists(path):
        shutil.rmtree(path)
    os.mkdir(path)

    bytesobj = pickle.dumps(obj)
    num_bytes = len(bytesobj)
    num_chunks = num_bytes // chunk_size + (0 if num_bytes % chunk_size == 0 else 1)

    for i in range(num_chunks):
        filepath = os.path.join(path, f"{i}.{PICKLE_EXTENSION}")
        with open(filepath, WRITE_MODE) as f:
            f.write(bytesobj[i * chunk_size : (i + 1) * chunk_size])
    return num_chunks


def load(path):
    """Load an object from a path

    Arguments:
        path {str} -- path name on disk to load object

    Returns:
        any -- the loaded objcet
    """
    assert os.path.exists(path)
    assert os.path.isdir(path)

    idxs = set(
        int(os.path.splitext(filename)[0])
        for filename in os.listdir(path)
        if (
            os.path.isfile(os.path.join(path, filename))
            and os.path.splitext(filename)[-1] == f".{PICKLE_EXTENSION}"
            and os.path.splitext(filename)[0].isnumeric()
        )
    )
    max_idx = max(idxs)
    for i in range(max_idx + 1):
        assert i in idxs

    ba = bytearray()
    for i in range(max_idx + 1):
        filename = os.path.join(path, f"{i}.{PICKLE_EXTENSION}")
        with open(filename, READ_MODE) as f:
            ba += f.read()

    return pickle.loads(ba)
