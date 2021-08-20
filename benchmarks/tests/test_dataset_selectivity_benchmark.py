import copy

import pytest

from .. import _sources, dataset_selectivity_benchmark
from ..tests import _asserts

HELP = """
Usage: conbench dataset-selectivity [OPTIONS] SOURCE

  Run dataset-selectivity benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --selectivity=1%
  --selectivity=10%
  --selectivity=100%

  To run all combinations:
  $ conbench dataset-selectivity --all=true

Options:
  --selectivity [1%|10%|100%]
  --all BOOLEAN                [default: False]
  --cpu-count INTEGER
  --iterations INTEGER         [default: 1]
  --drop-caches BOOLEAN        [default: False]
  --gc-collect BOOLEAN         [default: True]
  --gc-disable BOOLEAN         [default: True]
  --show-result BOOLEAN        [default: True]
  --show-output BOOLEAN        [default: False]
  --run-id TEXT                Group executions together with a run id.
  --run-name TEXT              Name of run (commit, pull request, etc).
  --help                       Show this message and exit.
"""


benchmark = dataset_selectivity_benchmark.DatasetSelectivityBenchmark()
sources = [_sources.Source(s) for s in benchmark.sources_test]
cases, case_ids = benchmark.cases, benchmark.case_ids


def assert_benchmark(result, case, source):
    munged = copy.deepcopy(result)

    expected = {
        "cpu_count": None,
        "dataset": source,
        "name": "dataset-selectivity",
        "selectivity": case[0],
    }
    assert munged["tags"] == expected
    _asserts.assert_context(munged)


def assert_run(run, index, case, source):
    result, output = run[index]
    assert_benchmark(result, case, source.name)
    assert output > 0


@pytest.mark.parametrize("case", cases, ids=case_ids)
def test_dataset_selectivity(case):
    run = list(benchmark.run(sources, case, iterations=1))
    assert len(run) == 3
    for x in range(len(run)):
        assert_run(run, x, case, sources[x])


def test_dataset_selectivity_cli():
    command = ["conbench", "dataset-selectivity", "--help"]
    _asserts.assert_cli(command, HELP)
