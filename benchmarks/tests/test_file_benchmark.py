import copy
import json

import pytest

from .. import _sources
from .. import file_benchmark
from ..tests._asserts import assert_context, assert_cli


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
def test_write_nyctaxi(case):
    [(result, output)] = write_benchmark.run(nyctaxi, case, iterations=1)
    assert_benchmark(result, case, nyctaxi.name, "write", "input_type")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert output is None


@pytest.mark.parametrize("case", read_benchmark.cases, ids=read_benchmark.case_ids)
def test_read_nyctaxi(case):
    [(result, output)] = read_benchmark.run(nyctaxi, case, iterations=1)
    assert_benchmark(result, case, nyctaxi.name, "read", "output_type")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert "pyarrow.Table" in str(output) or "[998 rows x 18 columns]" in str(output)


@pytest.mark.parametrize("case", write_benchmark.cases, ids=write_benchmark.case_ids)
def test_write_fanniemae(case):
    [(result, output)] = write_benchmark.run(fanniemae, case, iterations=1)
    assert_benchmark(result, case, fanniemae.name, "write", "input_type")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert output is None


@pytest.mark.parametrize("case", read_benchmark.cases, ids=read_benchmark.case_ids)
def test_read_fanniemae(case):
    [(result, output)] = read_benchmark.run(fanniemae, case, iterations=1)
    assert_benchmark(result, case, fanniemae.name, "read", "output_type")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert "pyarrow.Table" in str(output) or "[757 rows x 108 columns]" in str(output)


NO_LZ4 = "arrowbench doesn't support compression=lz4 case"
R_CLI = "The R Foundation for Statistical Computing"


def skip_lz4(case):
    _, compression, _ = case
    if compression == "lz4":
        pytest.skip(NO_LZ4)


@pytest.mark.slow
@pytest.mark.parametrize("case", write_benchmark.cases, ids=write_benchmark.case_ids)
def test_write_nyctaxi_r(case):
    skip_lz4(case)
    name = nyctaxi_big.name
    [(result, output)] = write_benchmark.run(nyctaxi_big, case, language="R")
    assert_benchmark(result, case, name, "write", "input_type", language="R")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert R_CLI in str(output)


@pytest.mark.slow
@pytest.mark.parametrize("case", read_benchmark.cases, ids=read_benchmark.case_ids)
def test_read_nyctaxi_r(case):
    skip_lz4(case)
    name = nyctaxi_big.name
    [(result, output)] = read_benchmark.run(nyctaxi_big, case, language="R")
    assert_benchmark(result, case, name, "read", "output_type", language="R")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert R_CLI in str(output)


@pytest.mark.slow
@pytest.mark.parametrize("case", write_benchmark.cases, ids=write_benchmark.case_ids)
def test_write_fanniemae_r(case):
    skip_lz4(case)
    name = fanniemae_big.name
    [(result, output)] = write_benchmark.run(fanniemae_big, case, language="R")
    assert_benchmark(result, case, name, "write", "input_type", language="R")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert R_CLI in str(output)


@pytest.mark.slow
@pytest.mark.parametrize("case", read_benchmark.cases, ids=read_benchmark.case_ids)
def test_read_fanniemae_r(case):
    skip_lz4(case)
    name = fanniemae_big.name
    [(result, output)] = read_benchmark.run(fanniemae_big, case, language="R")
    assert_benchmark(result, case, name, "read", "output_type", language="R")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert R_CLI in str(output)


def test_file_read_cli():
    command = ["conbench", "file-read", "--help"]
    assert_cli(command, FILE_READ_HELP)


def test_file_write_cli():
    command = ["conbench", "file-write", "--help"]
    assert_cli(command, FILE_WRITE_HELP)
