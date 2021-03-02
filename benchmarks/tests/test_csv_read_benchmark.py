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
  --help                 Show this message and exit.
"""


fanniemae = _sources.Source("fanniemae_sample")
nyctaxi = _sources.Source("nyctaxi_sample")


def test_csv_read_fanniemae():
    benchmark = csv_read_benchmark.CsvReadBenchmark()
    [(result, output)] = benchmark.run(fanniemae, iterations=1)
    assert_benchmark(result, fanniemae.name, benchmark.name)
    print(json.dumps(result, indent=4, sort_keys=True))
    assert "pyarrow.Table" in str(output)


def test_csv_read_nyctaxi():
    benchmark = csv_read_benchmark.CsvReadBenchmark()
    [(result, output)] = benchmark.run(nyctaxi, iterations=1)
    assert_benchmark(result, nyctaxi.name, benchmark.name)
    print(json.dumps(result, indent=4, sort_keys=True))
    assert "pyarrow.Table" in str(output)


def test_csv_read_cli():
    command = ["conbench", "csv-read", "--help"]
    assert_cli(command, HELP)
