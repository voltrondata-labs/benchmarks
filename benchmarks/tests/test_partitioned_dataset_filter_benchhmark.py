import copy
import json

import pytest

from .. import _sources
from .. import dataset_filter_benchmark
from ..tests._asserts import assert_benchmark, assert_context, R_CLI


HELP = """
Usage: conbench dataset-filter [OPTIONS] SOURCE

  Run dataset-taxi-parquet benchmark.

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


def assert_benchmark(result, source, name, case, language="Python"):
    munged = copy.deepcopy(result)
    expected = {
        "name": name,
        "dataset": source,
        "cpu_count": None,
        "gc_collect": True,
        "gc_disable": True,
    }
    if language == "R":
        del expected["gc_collect"]
        del expected["gc_disable"]
        expected["query"] = case[0]
        expected["language"] = "R"
    assert munged["tags"] == expected
    assert_context(munged, language=language)


benchmark = dataset_filter_benchmark.PartitionedDatasetFilterBenchmark()


@pytest.mark.parametrize("case", benchmark.cases, ids=benchmark.case_ids)
def test_partitioned_dataset_filter_one(case):
    pytest.skip("needs a test partitioned dataset")
    benchmark = dataset_filter_benchmark.PartitionedDatasetFilterBenchmark()
    [(result, output)] = benchmark.run(case, iterations=1)
    assert_benchmark(result, "dataset-taxi-parquet", benchmark.name, case, language="R")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert R_CLI in str(output)
