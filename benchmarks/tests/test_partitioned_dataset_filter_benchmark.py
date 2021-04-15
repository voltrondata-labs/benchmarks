import copy

import pytest

from .. import partitioned_dataset_filter_benchmark
from ..tests._asserts import assert_context, assert_cli, R_CLI


HELP = """
Usage: conbench partitioned-dataset-filter [OPTIONS]

  Run partitioned-dataset-filter benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --query=vignette
  --query=payment_type_3
  --query=small_no_files
  --query=count_rows

  To run all combinations:
  $ conbench partitioned-dataset-filter --all=true

Options:
  --query [count_rows|payment_type_3|small_no_files|vignette]
  --all BOOLEAN                   [default: false]
  --iterations INTEGER            [default: 1]
  --drop-caches BOOLEAN           [default: false]
  --cpu-count INTEGER
  --show-result BOOLEAN           [default: true]
  --show-output BOOLEAN           [default: false]
  --run-id TEXT                   Group executions together with a run id.
  --run-name TEXT                 Name of run (commit, pull request, etc).
  --help                          Show this message and exit.
"""


def assert_benchmark(result, source, name, case, language="Python"):
    munged = copy.deepcopy(result)
    expected = {
        "name": name,
        "dataset": source,
        "cpu_count": None,
    }
    if language == "R":
        expected["query"] = case[0]
        expected["language"] = "R"
    assert munged["tags"] == expected
    assert_context(munged, language=language)


benchmark = partitioned_dataset_filter_benchmark.PartitionedDatasetFilterBenchmark()


@pytest.mark.parametrize("case", benchmark.cases, ids=benchmark.case_ids)
def test_partitioned_dataset_filter_one(case):
    pytest.skip("needs a test partitioned dataset")
    benchmark = partitioned_dataset_filter_benchmark.PartitionedDatasetFilterBenchmark()
    [(result, output)] = benchmark.run(case, iterations=1)
    assert_benchmark(result, "dataset-taxi-parquet", benchmark.name, case, language="R")
    assert R_CLI in str(output)


def test_partitioned_dataset_filter_cli():
    command = ["conbench", "partitioned-dataset-filter", "--help"]
    assert_cli(command, HELP)
