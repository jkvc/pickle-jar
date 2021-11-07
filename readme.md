# pickle-jar

A container of pickle slices. 

Use this to serialize/read objects to/from disk, but instead of a single pickle file, split into multiple slices of smaller, separate files. This is useful to get around github single-file size restriction. 

## install

`jar` is on [PyPI](https://pypi.org/project/pickle-jar/), use pip:

`python -m pip install pickle-jar`

## usage

Use `jar.dump` and `jar.load` as you would with with python's `pickle`, see examples in `test.py`.
