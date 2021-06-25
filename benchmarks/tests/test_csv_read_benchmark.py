from .. import _sources
from .. import csv_read_benchmark
from ..tests._asserts import assert_benchmark, assert_cli


HELP = """
Usage: conbench csv-read [OPTIONS] SOURCE

  Run csv-read benchmark.

Options:
  --cpu-count INTEGER
  --iterations INTEGER   [default: 1]
  --drop-caches BOOLEAN  [default: False]
  --gc-collect BOOLEAN   [default: True]
  --gc-disable BOOLEAN   [default: True]
  --show-result BOOLEAN  [default: True]
  --show-output BOOLEAN  [default: False]
  --run-id TEXT          Group executions together with a run id.
  --run-name TEXT        Name of run (commit, pull request, etc).
  --help                 Show this message and exit.
"""


def assert_run(run, index, benchmark, source):
    result, output = run[index]
    assert_benchmark(result, source.name, benchmark.name)
    assert "pyarrow.Table" in str(output)


def test_csv_read():
    benchmark = csv_read_benchmark.CsvReadBenchmark()
    sources = [_sources.Source(s) for s in benchmark.sources_test]
    run = list(benchmark.run(sources, iterations=1))
    assert len(run) == 2
    for x in range(len(run)):
        assert_run(run, x, benchmark, sources[x])


def test_csv_read_cli():
    command = ["conbench", "csv-read", "--help"]
    assert_cli(command, HELP)
