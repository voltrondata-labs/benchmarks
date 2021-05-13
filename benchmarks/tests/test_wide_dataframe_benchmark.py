import copy

import pytest

from .. import wide_dataframe_benchmark
from ..tests._asserts import assert_cli, assert_context


HELP = """
Usage: conbench wide-dataframe [OPTIONS]

  Run wide-dataframe benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --use-legacy-dataset=true
  --use-legacy-dataset=false

  To run all combinations:
  $ conbench wide-dataframe --all=true

Options:
  --use-legacy-dataset [false|true]
  --all BOOLEAN                   [default: False]
  --cpu-count INTEGER
  --iterations INTEGER            [default: 1]
  --drop-caches BOOLEAN           [default: False]
  --gc-collect BOOLEAN            [default: True]
  --gc-disable BOOLEAN            [default: True]
  --show-result BOOLEAN           [default: True]
  --show-output BOOLEAN           [default: False]
  --run-id TEXT                   Group executions together with a run id.
  --run-name TEXT                 Name of run (commit, pull request, etc).
  --help                          Show this message and exit.
"""


benchmark = wide_dataframe_benchmark.WideDataframeBenchmark()
cases, case_ids = benchmark.cases, benchmark.case_ids


def assert_benchmark(result, case):
    munged = copy.deepcopy(result)
    assert munged["tags"] == {
        "name": "wide-dataframe",
        "cpu_count": None,
        "use_legacy_dataset": case[0],
    }
    assert_context(munged)


@pytest.mark.parametrize("case", cases, ids=case_ids)
def test_wide_dataframe(case):
    [(result, output)] = benchmark.run(case, iterations=1)
    assert_benchmark(result, case)
    assert "100 rows x 10000 columns" in str(output)


def test_wide_dataframe_cli():
    command = ["conbench", "wide-dataframe", "--help"]
    assert_cli(command, HELP)
