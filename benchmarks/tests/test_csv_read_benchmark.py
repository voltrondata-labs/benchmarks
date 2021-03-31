import json

from .. import _sources
from .. import csv_read_benchmark
from ..tests._asserts import assert_benchmark, assert_cli


HELP = """
Usage: conbench csv-read [OPTIONS] SOURCE

  Run csv-read benchmark.

Options:
  --cpu-count INTEGER
  --iterations INTEGER   [default: 1]
  --gc-collect BOOLEAN   [default: true]
  --gc-disable BOOLEAN   [default: true]
  --show-result BOOLEAN  [default: true]
  --show-output BOOLEAN  [default: false]
  --run-id TEXT          Group executions together with a run id.
  --run-name TEXT        Name of run (commit, pull request, etc).
  --help                 Show this message and exit.
"""


fanniemae = _sources.Source("fanniemae_sample")
nyctaxi = _sources.Source("nyctaxi_sample")


def assert_run(run, index, benchmark, source):
    result, output = run[index]
    assert_benchmark(result, source.name, benchmark.name)
    assert "pyarrow.Table" in str(output)


def test_csv_read_one():
    benchmark = csv_read_benchmark.CsvReadBenchmark()
    [(result, output)] = benchmark.run(fanniemae, iterations=1)
    assert_benchmark(result, fanniemae.name, benchmark.name)
    assert "pyarrow.Table" in str(output)


def test_csv_read_all():
    benchmark = csv_read_benchmark.CsvReadBenchmark()
    run = list(benchmark.run("TEST", iterations=1))
    assert len(run) == 2
    assert_run(run, 0, benchmark, fanniemae)
    assert_run(run, 1, benchmark, nyctaxi)


def test_csv_read_cli():
    command = ["conbench", "csv-read", "--help"]
    assert_cli(command, HELP)
