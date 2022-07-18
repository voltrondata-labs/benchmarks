from .. import _sources, dataset_filter_benchmark
from ..tests import _asserts

HELP = """
Usage: conbench dataset-filter [OPTIONS] SOURCE

  Run dataset-filter benchmark.

Options:
  --cpu-count INTEGER
  --iterations INTEGER   [default: 1]
  --drop-caches BOOLEAN  [default: false]
  --gc-collect BOOLEAN   [default: true]
  --gc-disable BOOLEAN   [default: true]
  --show-result BOOLEAN  [default: true]
  --show-output BOOLEAN  [default: false]
  --run-id TEXT          Group executions together with a run id.
  --run-name TEXT        Free-text name of run (commit ABC, pull request 123,
                         etc).
  --run-reason TEXT      Low-cardinality reason for run (commit, pull request,
                         manual, etc).
  --help                 Show this message and exit.
"""


def assert_run(run, index, benchmark, source):
    result, output = run[index]
    _asserts.assert_benchmark(result, source.name, benchmark.name)
    _asserts.assert_table_output(source.name, output)


def test_dataset_filter():
    benchmark = dataset_filter_benchmark.DatasetFilterBenchmark()
    sources = [_sources.Source(s) for s in benchmark.sources_test]
    run = list(benchmark.run(sources, iterations=1))
    assert len(run) == 1
    for x in range(len(run)):
        assert_run(run, x, benchmark, sources[x])


def test_dataset_filter_cli():
    command = ["conbench", "dataset-filter", "--help"]
    _asserts.assert_cli(command, HELP)
