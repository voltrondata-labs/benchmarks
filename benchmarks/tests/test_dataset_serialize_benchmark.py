import copy

import pytest

from .. import _sources, dataset_serialize_benchmark
from ..tests import _asserts

HELP = """
Usage: conbench dataset-serialize [OPTIONS] SOURCE

  Run dataset-serialize benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --selectivity=1pc --format=parquet
  --selectivity=1pc --format=arrow
  --selectivity=1pc --format=ipc
  --selectivity=1pc --format=feather
  --selectivity=1pc --format=csv
  --selectivity=10pc --format=parquet
  --selectivity=10pc --format=arrow
  --selectivity=10pc --format=ipc
  --selectivity=10pc --format=feather
  --selectivity=10pc --format=csv

  To run all combinations:
  $ conbench dataset-serialize --all=true

Options:
  --selectivity [10pc|1pc]
  --format [arrow|csv|feather|ipc|parquet]
  --all BOOLEAN                   [default: false]
  --cpu-count INTEGER
  --iterations INTEGER            [default: 1]
  --drop-caches BOOLEAN           [default: false]
  --gc-collect BOOLEAN            [default: true]
  --gc-disable BOOLEAN            [default: true]
  --show-result BOOLEAN           [default: true]
  --show-output BOOLEAN           [default: false]
  --run-id TEXT                   Group executions together with a run id.
  --run-name TEXT                 Free-text name of run (commit ABC, pull
                                  request 123, etc).
  --run-reason TEXT               Low-cardinality reason for run (commit, pull
                                  request, manual, etc).
  --help                          Show this message and exit.
"""


benchmark = dataset_serialize_benchmark.DatasetSerializeBenchmark()
sources = [_sources.Source(s) for s in benchmark.sources_test]
cases, case_ids = benchmark.cases, benchmark.case_ids


def assert_benchmark(result, case, source):
    munged = copy.deepcopy(result)

    expected = {
        "cpu_count": None,
        "dataset": source,
        "name": "dataset-serialize",
        "selectivity": case[0],
        "format": case[1],
    }

    assert munged["tags"] == expected
    _asserts.assert_info_and_context(munged)


def assert_run(run, index, case, source):
    result, output = run[index]
    assert_benchmark(result, case, source.name)
    assert output is None


@pytest.mark.parametrize("case", cases, ids=case_ids)
def test_dataset_serialize(case):
    run = list(benchmark.run(sources, case, iterations=1))
    for x in range(len(run)):
        assert_run(run, x, case, sources[x])


def test_dataset_serialize_cli():
    command = ["conbench", "dataset-serialize", "--help"]
    _asserts.assert_cli(command, HELP)
