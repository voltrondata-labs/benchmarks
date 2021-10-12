import copy

import pytest

from .. import _sources, dataset_read_benchmark
from ..tests import _asserts

HELP = """
Usage: conbench dataset-read [OPTIONS] SOURCE

  Run dataset-read benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --pre-buffer=true
  --pre-buffer=false

  To run all combinations:
  $ conbench dataset-read --all=true

Options:
  --pre-buffer [false|true]
  --all BOOLEAN              [default: false]
  --cpu-count INTEGER
  --iterations INTEGER       [default: 1]
  --drop-caches BOOLEAN      [default: false]
  --gc-collect BOOLEAN       [default: true]
  --gc-disable BOOLEAN       [default: true]
  --show-result BOOLEAN      [default: true]
  --show-output BOOLEAN      [default: false]
  --run-id TEXT              Group executions together with a run id.
  --run-name TEXT            Name of run (commit, pull request, etc).
  --help                     Show this message and exit.
"""


benchmark = dataset_read_benchmark.DatasetReadBenchmark()
sources = [_sources.Source(s) for s in benchmark.sources_test]
cases, case_ids = benchmark.cases, benchmark.case_ids


def assert_benchmark(result, case, source):
    munged = copy.deepcopy(result)

    legacy = {
        "name": "dataset-read",
        "dataset": source,
        "cpu_count": None,
        "async": True,
        "pre_buffer": None,
    }
    pre_buffer = {
        "name": "dataset-read",
        "dataset": source,
        "cpu_count": None,
        "async": True,
        "pre_buffer": case[0],
    }

    try:
        assert munged["tags"] == pre_buffer
    except AssertionError:
        assert munged["tags"] == legacy

    _asserts.assert_context(munged)


def assert_run(run, index, case, source):
    result, output = run[index]
    assert_benchmark(result, case, source.name)
    _asserts.assert_table_output(source.name, output, nyc_ts=True)


@pytest.mark.parametrize("case", cases, ids=case_ids)
def test_dataset_read(case):
    run = list(benchmark.run(sources, case, iterations=1))
    assert len(run) == 2
    for x in range(len(run)):
        assert_run(run, x, case, sources[x])


def test_dataset_read_cli():
    command = ["conbench", "dataset-read", "--help"]
    _asserts.assert_cli(command, HELP)
