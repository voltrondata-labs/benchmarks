import copy

import pytest

from .. import _sources, file_benchmark
from ..tests import _asserts

FILE_READ_HELP = """
Usage: conbench file-read [OPTIONS] SOURCE

  Run file-read benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --file-type=parquet --compression=uncompressed --output-type=table
  --file-type=parquet --compression=uncompressed --output-type=dataframe
  --file-type=parquet --compression=snappy --output-type=table
  --file-type=parquet --compression=snappy --output-type=dataframe
  --file-type=feather --compression=uncompressed --output-type=table
  --file-type=feather --compression=uncompressed --output-type=dataframe
  --file-type=feather --compression=lz4 --output-type=table
  --file-type=feather --compression=lz4 --output-type=dataframe

  To run all combinations:
  $ conbench file-read --all=true

Options:
  --file-type [feather|parquet]
  --compression [lz4|snappy|uncompressed]
  --output-type [dataframe|table]
  --all BOOLEAN                   [default: False]
  --language [Python|R]
  --cpu-count INTEGER
  --iterations INTEGER            [default: 1]
  --drop-caches BOOLEAN           [default: False]
  --gc-collect BOOLEAN            [default: True]
  --gc-disable BOOLEAN            [default: True]
  --show-result BOOLEAN           [default: True]
  --show-output BOOLEAN           [default: False]
  --run-id TEXT                   Group executions together with a run id.
  --run-name TEXT                 Name of run (commit, pull request, etc).
  --help                          Show this message and exit.
"""

FILE_WRITE_HELP = """
Usage: conbench file-write [OPTIONS] SOURCE

  Run file-write benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --file-type=parquet --compression=uncompressed --input-type=table
  --file-type=parquet --compression=uncompressed --input-type=dataframe
  --file-type=parquet --compression=snappy --input-type=table
  --file-type=parquet --compression=snappy --input-type=dataframe
  --file-type=feather --compression=uncompressed --input-type=table
  --file-type=feather --compression=uncompressed --input-type=dataframe
  --file-type=feather --compression=lz4 --input-type=table
  --file-type=feather --compression=lz4 --input-type=dataframe

  To run all combinations:
  $ conbench file-write --all=true

Options:
  --file-type [feather|parquet]
  --compression [lz4|snappy|uncompressed]
  --input-type [dataframe|table]
  --all BOOLEAN                   [default: False]
  --language [Python|R]
  --cpu-count INTEGER
  --iterations INTEGER            [default: 1]
  --drop-caches BOOLEAN           [default: False]
  --gc-collect BOOLEAN            [default: True]
  --gc-disable BOOLEAN            [default: True]
  --show-result BOOLEAN           [default: True]
  --show-output BOOLEAN           [default: False]
  --run-id TEXT                   Group executions together with a run id.
  --run-name TEXT                 Name of run (commit, pull request, etc).
  --help                          Show this message and exit.
"""


read_benchmark = file_benchmark.FileReadBenchmark()
write_benchmark = file_benchmark.FileWriteBenchmark()
read_cases, read_case_ids = read_benchmark.cases, read_benchmark.case_ids
write_cases, write_case_ids = write_benchmark.cases, write_benchmark.case_ids
sources = [_sources.Source(s) for s in read_benchmark.sources_test]


def assert_run_write(run, index, case, source):
    result, output = run[index]
    assert_benchmark(result, case, source.name, "write", "input_type")
    assert output is None


def assert_run_read(run, index, case, source):
    result, output = run[index]
    assert_benchmark(result, case, source.name, "read", "output_type")
    if "pyarrow.Table" in str(output):
        _asserts.assert_table_output(source.name, output)
    else:
        _asserts.assert_dimensions_output(source.name, output)


def assert_run_write_r(run, index, case, source):
    result, output = run[index]
    assert_benchmark(result, case, source.name, "write", "input_type", language="R")
    assert _asserts.R_CLI in str(output)


def assert_run_read_r(run, index, case, source):
    result, output = run[index]
    assert_benchmark(result, case, source.name, "read", "output_type", language="R")
    assert _asserts.R_CLI in str(output)


def assert_benchmark(result, case, source, action, type_tag, language="Python"):
    munged = copy.deepcopy(result)
    name = "file-write" if action == "write" else "file-read"
    expected = {
        "name": name,
        "cpu_count": None,
        "dataset": source,
        "file_type": case[0],
        "compression": case[1],
        type_tag: case[2],
    }
    if language == "R":
        expected["language"] = "R"
    assert munged["tags"] == expected
    _asserts.assert_context(munged, language=language)


@pytest.mark.parametrize("case", read_cases, ids=read_case_ids)
def test_read(case):
    run = list(read_benchmark.run(sources, case, iterations=1))
    assert len(run) == 2
    for x in range(len(run)):
        assert_run_read(run, x, case, sources[x])


@pytest.mark.parametrize("case", write_cases, ids=write_case_ids)
def test_write(case):
    run = list(write_benchmark.run(sources, case, iterations=1))
    assert len(run) == 2
    for x in range(len(run)):
        assert_run_write(run, x, case, sources[x])


@pytest.mark.parametrize("case", read_cases, ids=read_case_ids)
def test_read_r(case):
    run = list(read_benchmark.run(sources, case, language="R"))
    assert len(run) == 2
    for x in range(len(run)):
        assert_run_read_r(run, x, case, sources[x])


@pytest.mark.parametrize("case", write_cases, ids=write_case_ids)
def test_write_r(case):
    run = list(write_benchmark.run(sources, case, language="R"))
    assert len(run) == 2
    for x in range(len(run)):
        assert_run_write_r(run, x, case, sources[x])


def test_write_cli():
    command = ["conbench", "file-write", "--help"]
    _asserts.assert_cli(command, FILE_WRITE_HELP)


def test_read_cli():
    command = ["conbench", "file-read", "--help"]
    _asserts.assert_cli(command, FILE_READ_HELP)
