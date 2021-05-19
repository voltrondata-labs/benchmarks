import copy

import pytest

from .. import _sources
from .. import dataset_read_benchmark
from ..tests._asserts import assert_cli, assert_context


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


nyctaxi = _sources.Source("nyctaxi_multi_parquet_s3_sample")
nyctaxi_ipc = _sources.Source("nyctaxi_multi_ipc_s3_sample")
benchmark = dataset_read_benchmark.DatasetReadBenchmark()
cases, case_ids = benchmark.cases, benchmark.case_ids


def assert_benchmark(result, case, source):
    munged = copy.deepcopy(result)

    # The async tag can be True or False, we just ensure it exists
    assert "async" in munged["tags"]
    use_async = munged["tags"]["async"]

    legacy = {
        "name": "dataset-read",
        "dataset": source,
        "cpu_count": None,
        "pre_buffer": None,
        "async": use_async,
    }
    pre_buffer = {
        "name": "dataset-read",
        "dataset": source,
        "cpu_count": None,
        "pre_buffer": case[0],
        "async": use_async,
    }

    try:
        assert munged["tags"] == pre_buffer
    except AssertionError:
        assert munged["tags"] == legacy

    assert_context(munged)


def assert_run(run, index, case, source):
    result, output = run[index]
    assert_benchmark(result, case, source.name)
    assert "pyarrow.Table" in str(output)


@pytest.mark.parametrize("case", cases, ids=case_ids)
def test_dataset_read(case):
    run = list(benchmark.run("TEST", case, iterations=1))
    assert len(run) == 2
    assert_run(run, 0, case, nyctaxi)
    assert_run(run, 1, case, nyctaxi_ipc)


def test_dataset_read_cli():
    command = ["conbench", "dataset-read", "--help"]
    assert_cli(command, HELP)
