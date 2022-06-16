import copy

import pytest

from .. import partitioned_dataset_filter_benchmark
from ..tests import _asserts

HELP = """
Usage: conbench partitioned-dataset-filter [OPTIONS]

  Run partitioned-dataset-filter benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --query=vignette
  --query=payment_type_3
  --query=small_no_files
  --query=dims

  To run all combinations:
  $ conbench partitioned-dataset-filter --all=true

Options:
  --query [dims|payment_type_3|small_no_files|vignette]
  --all BOOLEAN                   [default: false]
  --iterations INTEGER            [default: 1]
  --drop-caches BOOLEAN           [default: false]
  --cpu-count INTEGER
  --show-result BOOLEAN           [default: true]
  --show-output BOOLEAN           [default: false]
  --run-id TEXT                   Group executions together with a run id.
  --run-name TEXT                 Free-text name of run (commit ABC, pull
                                  request 123, etc).
  --run-reason TEXT               Low-cardinality reason for run (commit, pull
                                  request, manual, etc).
  --help                          Show this message and exit.
"""


def assert_benchmark(result, source, name, case, language="Python"):
    munged = copy.deepcopy(result)
    expected = {
        "name": name,
        "dataset": source,
        "cpu_count": None,
        "query": case[0],
    }
    if language == "R":
        expected["language"] = "R"
    assert munged["tags"] == expected
    _asserts.assert_info_and_context(munged, language=language)


benchmark = partitioned_dataset_filter_benchmark.PartitionedDatasetFilterBenchmark()
cases, case_ids = benchmark.cases, benchmark.case_ids


@pytest.mark.parametrize("case", cases, ids=case_ids)
def test_partitioned_dataset_filter(case):
    pytest.skip("needs a test partitioned dataset")
    [(result, output)] = benchmark.run(case, iterations=1)
    assert_benchmark(result, "dataset-taxi-parquet", benchmark.name, case, language="R")
    assert _asserts.R_CLI in str(output)


def test_partitioned_dataset_filter_cli():
    command = ["conbench", "partitioned-dataset-filter", "--help"]
    _asserts.assert_cli(command, HELP)
