import copy
import json

import pytest

from .. import _sources
from .. import dataset_select_benchmark
from ..tests._asserts import assert_cli, assert_benchmark


HELP = """
Usage: conbench dataset-select [OPTIONS] SOURCE

  Run dataset-select benchmark.

Options:
  --cpu-count INTEGER
  --iterations INTEGER   [default: 1]
  --gc-collect BOOLEAN   [default: true]
  --gc-disable BOOLEAN   [default: true]
  --show-result BOOLEAN  [default: true]
  --show-output BOOLEAN  [default: false]
  --run-id TEXT          Group executions together with a run id.
  --help                 Show this message and exit.
"""


nyctaxi = _sources.Source("nyctaxi_multi_parquet_s3_repartitioned")
benchmark = dataset_select_benchmark.DatasetSelectBenchmark()


def assert_run(run, index, benchmark, source):
    result, output = run[index]
    assert_benchmark(result, source.name, benchmark.name)
    print(json.dumps(result, indent=4, sort_keys=True))
    assert "pyarrow.Table" in str(output)


def test_dataset_read_one():
    [(result, output)] = benchmark.run(nyctaxi, iterations=1)
    assert_benchmark(result, nyctaxi.name, benchmark.name)
    print(json.dumps(result, indent=4, sort_keys=True))
    assert "pyarrow.Table" in str(output)


def test_dataset_read_all():
    run = list(benchmark.run("TEST", iterations=1))
    assert len(run) == 1
    assert_run(run, 0, benchmark, nyctaxi)


def test_dataset_read_cli():
    command = ["conbench", "dataset-select", "--help"]
    assert_cli(command, HELP)
