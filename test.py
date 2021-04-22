import os
import subprocess
from memory_profiler import profile
import pyarrow.csv
import pyarrow
import pyarrow.feather as feather
import pyarrow.parquet as parquet

from benchmarks.csv_read_benchmark import CsvReadBenchmark
from benchmarks.file_benchmark import FileReadBenchmark


def csv():
    path = '/Users/elena/benchmarks/benchmarks/data/fanniemae_2016Q4.csv.gz'
    parse_options = pyarrow.csv.ParseOptions(delimiter="|")
    pyarrow.csv.read_csv(path, parse_options=parse_options)
    pyarrow.csv.read_csv(path, parse_options=parse_options)
    pyarrow.csv.read_csv(path, parse_options=parse_options)

    path = '/Users/elena/benchmarks/benchmarks/data/nyctaxi_2010-01.csv.gz'
    parse_options = pyarrow.csv.ParseOptions(delimiter=",")
    pyarrow.csv.read_csv(path, parse_options=parse_options)
    pyarrow.csv.read_csv(path, parse_options=parse_options)
    pyarrow.csv.read_csv(path, parse_options=parse_options)


def csv_v2():
    b = CsvReadBenchmark()
    kwargs = dict(iterations=3, drop_caches=True)
    for source in b.get_sources("ALL"):
        f = b._get_benchmark_function(
            source.source_path,
            source.csv_parse_options,
        )
        tags = b.get_tags(kwargs, source)
        b.benchmark(f, tags, kwargs)


def csv_v3():
    b = CsvReadBenchmark()
    kwargs = dict(iterations=1, drop_caches=True)
    sources = b.get_sources("ALL")
    source = sources[0]
    print(source.source_path)
    f = b._get_benchmark_function(
        source.source_path,
        source.csv_parse_options,
    )
    tags = b.get_tags(kwargs, source)
    b.benchmark(f, tags, kwargs)

    source = sources[1]
    print(source.source_path)
    f = b._get_benchmark_function(
        source.source_path,
        source.csv_parse_options,
    )
    tags = b.get_tags(kwargs, source)
    b.benchmark(f, tags, kwargs)


@profile
def file_read():
    b = FileReadBenchmark()
    sources = b.get_sources("ALL")

    source = sources[0]
    print(source.name)
    cases = b.get_cases(None, {"all": True})
    for case in cases:
        print(case)
        file_type, compression, output_type = case
        path = source.create_if_not_exists(file_type, compression)

        if file_type == "parquet" and output_type == "table":
            parquet.read_table(path, memory_map=False)
            parquet.read_table(path, memory_map=False)
            parquet.read_table(path, memory_map=False)
        elif file_type == "parquet" and output_type == "dataframe":
            parquet.read_table(path, memory_map=False).to_pandas()
            parquet.read_table(path, memory_map=False).to_pandas()
            parquet.read_table(path, memory_map=False).to_pandas()
        elif file_type == "feather" and output_type == "table":
            feather.read_table(path, memory_map=False)
            feather.read_table(path, memory_map=False)
            feather.read_table(path, memory_map=False)
        elif file_type == "feather" and output_type == "dataframe":
            feather.read_feather(path, memory_map=False)
            feather.read_table(path, memory_map=False)
            feather.read_table(path, memory_map=False)

    source = sources[1]
    print(source.name)
    cases = b.get_cases(None, {"all": True})
    for case in cases:
        print(case)
        file_type, compression, output_type = case
        path = source.create_if_not_exists(file_type, compression)

        if file_type == "parquet" and output_type == "table":
            parquet.read_table(path, memory_map=False)
            parquet.read_table(path, memory_map=False)
            parquet.read_table(path, memory_map=False)
        elif file_type == "parquet" and output_type == "dataframe":
            parquet.read_table(path, memory_map=False).to_pandas()
            parquet.read_table(path, memory_map=False).to_pandas()
            parquet.read_table(path, memory_map=False).to_pandas()
        elif file_type == "feather" and output_type == "table":
            feather.read_table(path, memory_map=False)
            feather.read_table(path, memory_map=False)
            feather.read_table(path, memory_map=False)
        elif file_type == "feather" and output_type == "dataframe":
            feather.read_feather(path, memory_map=False)
            feather.read_table(path, memory_map=False)
            feather.read_table(path, memory_map=False)


@profile
def file_read_v2():
    source = "ALL"
    case = None
    kwargs = {'iterations': 3, 'all': True, 'drop_caches': True, 'run_id': 'test', 'run_name': 'test',
              'file_type': 'feather', 'compression': 'lz4', 'output_type': 'dataframe', 'language': 'Python',
              'cpu_count': None, 'gc_collect': True, 'gc_disable': True}

    b = FileReadBenchmark()
    cases = b.get_cases(case, kwargs)

    for source in b.get_sources(source):
        tags = b.get_tags(kwargs, source)
        for case in cases:
            f = b._get_benchmark_function(source, case)
            result = b.benchmark(f, tags, kwargs, case)


@profile
def file_read_v3():
    source = "ALL"
    case = None
    kwargs = {'iterations': 3, 'all': True, 'drop_caches': True, 'run_id': 'test', 'run_name': 'test',
              'file_type': 'feather', 'compression': 'lz4', 'output_type': 'dataframe', 'language': 'Python',
              'cpu_count': None, 'gc_collect': True, 'gc_disable': True}

    b = FileReadBenchmark()
    sources = b.get_sources(source)

    source = sources[0]
    tags = b.get_tags(kwargs, source)

    case = ('parquet', 'uncompressed', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'uncompressed', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'lz4', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'lz4', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'zstd', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'zstd', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'snappy', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'snappy', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'uncompressed', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'uncompressed', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'lz4', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'lz4', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'zstd', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'zstd', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'uncompressed', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'uncompressed', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'lz4', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'lz4', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'zstd', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'zstd', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'snappy', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'snappy', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'uncompressed', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'uncompressed', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'lz4', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'lz4', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'zstd', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'zstd', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)

    source = sources[1]
    tags = b.get_tags(kwargs, source)

    case = ('parquet', 'uncompressed', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'uncompressed', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'lz4', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'lz4', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'zstd', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'zstd', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'snappy', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'snappy', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'uncompressed', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'uncompressed', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'lz4', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'lz4', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'zstd', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'zstd', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'uncompressed', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'uncompressed', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'lz4', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'lz4', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'zstd', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'zstd', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'snappy', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('parquet', 'snappy', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'uncompressed', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'uncompressed', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'lz4', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'lz4', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'zstd', 'table')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)
    case = ('feather', 'zstd', 'dataframe')
    f = b._get_benchmark_function(source, case)
    result = b.benchmark(f, tags, kwargs, case)


if __name__ == '__main__':
    file_read_v3()
