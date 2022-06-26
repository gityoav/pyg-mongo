from pyg_mongo._encoders import csv_write, parquet_write, npy_write, _csv, _npy, _npa, _parquet, root_path
from pyg_base import encode, decode, passthru, is_str, as_list
from functools import partial

WRITERS = {_csv: csv_write , _npy: partial(npy_write, append = False), _npa: partial(npy_write, append = True), _parquet: parquet_write}

def as_reader(reader):
    if isinstance(reader, list):
        return sum([as_reader(r) for r in reader], [])
    elif reader is None or reader is True or reader == ():
        return [decode]
    elif reader is False or reader == 0:
        return [passthru]
    else:
        return [reader]

def as_writer(writer, kwargs = None):
    if isinstance(writer, list):
        return sum([as_writer(w) for w in writer], [])
    if writer is None or writer is True or writer == ():
        return [encode]
    elif writer is False or writer == 0:
        return [passthru]
    elif is_str(writer):
        for ext, w in WRITERS.items():
            if writer.endswith(ext):
                root = writer[:-len(ext)]                    
                if root:
                    if kwargs:
                        root = root_path(kwargs, root)
                    return [partial(w, root = root), encode]
                else:
                    return [w, encode]
        raise ValueError('We support only csv/npy/parquet writers and writer should look like: c:/somewhere/%name/%surname.csv or d:/archive/%country/%city/results.parquet or with .npy')
    else:
        return as_list(writer)

