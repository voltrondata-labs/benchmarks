import copy
import json

import pytest

from .. import _sources
from .. import file_benchmark
from ..tests._asserts import assert_context, assert_cli, R_CLI


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
  --file-type=feather --compression=zstd --output-type=table
  --file-type=feather --compression=zstd --output-type=dataframe

  To run all combinations:
  $ conbench file-read --all=true

Options:
  --file-type [feather|parquet]
  --compression [lz4|snappy|uncompressed|zstd]
  --output-type [dataframe|table]
  --all BOOLEAN                   [default: false]
  --language [Python|R]
  --cpu-count INTEGER
  --iterations INTEGER            [default: 1]
  --gc-collect BOOLEAN            [default: true]
  --gc-disable BOOLEAN            [default: true]
  --show-result BOOLEAN           [default: true]
  --show-output BOOLEAN           [default: false]
  --run-id TEXT                   Group executions together with a run id.
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
  --file-type=feather --compression=zstd --input-type=table
  --file-type=feather --compression=zstd --input-type=dataframe

  To run all combinations:
  $ conbench file-write --all=true

Options:
  --file-type [feather|parquet]
  --compression [lz4|snappy|uncompressed|zstd]
  --input-type [dataframe|table]
  --all BOOLEAN                   [default: false]
  --language [Python|R]
  --cpu-count INTEGER
  --iterations INTEGER            [default: 1]
  --gc-collect BOOLEAN            [default: true]
  --gc-disable BOOLEAN            [default: true]
  --show-result BOOLEAN           [default: true]
  --show-output BOOLEAN           [default: false]
  --run-id TEXT                   Group executions together with a run id.
  --help                          Show this message and exit.
"""


write_benchmark = file_benchmark.FileWriteBenchmark()
read_benchmark = file_benchmark.FileReadBenchmark()
fanniemae = _sources.Source("fanniemae_sample")
nyctaxi = _sources.Source("nyctaxi_sample")


# TODO: ability to test R benchmarks with sample sources
fanniemae_big = _sources.Source("fanniemae_2016Q4")
nyctaxi_big = _sources.Source("nyctaxi_2010-01")


def assert_run_write(run, index, case, source):
    result, output = run[index]
    assert_benchmark(result, case, source.name, "write", "input_type")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert output is None


def assert_run_read(run, index, case, source):
    result, output = run[index]
    assert_benchmark(result, case, source.name, "read", "output_type")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert (
        "pyarrow.Table" in str(output)
        or "[757 rows x 108 columns]" in str(output)
        or "[998 rows x 18 columns]" in str(output)
    )


def assert_run_write_r(run, index, case, source):
    result, output = run[index]
    assert_benchmark(result, case, source.name, "write", "input_type", language="R")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert R_CLI in str(output)


def assert_run_read_r(run, index, case, source):
    result, output = run[index]
    assert_benchmark(result, case, source.name, "read", "output_type", language="R")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert R_CLI in str(output)


def assert_benchmark(result, case, source, action, type_tag, language="Python"):
    munged = copy.deepcopy(result)
    name = "file-write" if action == "write" else "file-read"
    expected = {
        "name": name,
        "cpu_count": None,
        "dataset": source,
        "gc_collect": True,
        "gc_disable": True,
        "file_type": case[0],
        "compression": case[1],
        type_tag: case[2],
    }
    if language == "R":
        del expected["gc_collect"]
        del expected["gc_disable"]
        expected["language"] = "R"
    assert munged["tags"] == expected
    assert_context(munged, language=language)


@pytest.mark.parametrize("case", write_benchmark.cases, ids=write_benchmark.case_ids)
def test_write_one(case):
    [(result, output)] = write_benchmark.run(nyctaxi, case, iterations=1)
    assert_benchmark(result, case, nyctaxi.name, "write", "input_type")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert output is None


@pytest.mark.parametrize("case", read_benchmark.cases, ids=read_benchmark.case_ids)
def test_read_one(case):
    [(result, output)] = read_benchmark.run(nyctaxi, case, iterations=1)
    assert_benchmark(result, case, nyctaxi.name, "read", "output_type")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert "pyarrow.Table" in str(output) or "[998 rows x 18 columns]" in str(output)


@pytest.mark.parametrize("case", write_benchmark.cases, ids=write_benchmark.case_ids)
def test_write_all(case):
    run = list(write_benchmark.run("TEST", case, iterations=1))
    assert len(run) == 2
    assert_run_write(run, 0, case, fanniemae)
    assert_run_write(run, 1, case, nyctaxi)


@pytest.mark.parametrize("case", read_benchmark.cases, ids=read_benchmark.case_ids)
def test_read_all(case):
    run = list(read_benchmark.run("TEST", case, iterations=1))
    assert len(run) == 2
    assert_run_read(run, 0, case, fanniemae)
    assert_run_read(run, 1, case, nyctaxi)


NO_LZ4 = "arrowbench doesn't support compression=lz4 case"


def skip_lz4(case):
    _, compression, _ = case
    if compression == "lz4":
        pytest.skip(NO_LZ4)


@pytest.mark.slow
@pytest.mark.parametrize("case", write_benchmark.cases, ids=write_benchmark.case_ids)
def test_write_one_r(case):
    skip_lz4(case)
    name = nyctaxi_big.name
    [(result, output)] = write_benchmark.run(nyctaxi_big, case, language="R")
    assert_benchmark(result, case, name, "write", "input_type", language="R")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert R_CLI in str(output)


@pytest.mark.slow
@pytest.mark.parametrize("case", read_benchmark.cases, ids=read_benchmark.case_ids)
def test_read_one_r(case):
    skip_lz4(case)
    name = nyctaxi_big.name
    [(result, output)] = read_benchmark.run(nyctaxi_big, case, language="R")
    assert_benchmark(result, case, name, "read", "output_type", language="R")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert R_CLI in str(output)


@pytest.mark.slow
@pytest.mark.parametrize("case", write_benchmark.cases, ids=write_benchmark.case_ids)
def test_write_all_r(case):
    skip_lz4(case)
    # TODO: Change from "ALL" to "TEST" once R supports the samples
    run = list(write_benchmark.run("ALL", case, language="R"))
    assert len(run) == 2
    assert_run_write_r(run, 0, case, fanniemae_big)
    assert_run_write_r(run, 1, case, nyctaxi_big)


@pytest.mark.slow
@pytest.mark.parametrize("case", read_benchmark.cases, ids=read_benchmark.case_ids)
def test_read_all_r(case):
    skip_lz4(case)
    # TODO: Change from "ALL" to "TEST" once R supports the samples
    run = list(read_benchmark.run("ALL", case, language="R"))
    assert len(run) == 2
    assert_run_read_r(run, 0, case, fanniemae_big)
    assert_run_read_r(run, 1, case, nyctaxi_big)


def test_read_cli():
    command = ["conbench", "file-read", "--help"]
    assert_cli(command, FILE_READ_HELP)


def test_write_cli():
    command = ["conbench", "file-write", "--help"]
    assert_cli(command, FILE_WRITE_HELP)
