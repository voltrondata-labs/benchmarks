import copy

import pytest

from .. import filesystem_benchmark
from ..tests import _asserts

HELP = """
Usage: conbench recursive-get-file-info [OPTIONS]

  Run recursive-get-file-info benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --dataset-uri=s3://ursa-qa/wide-partition
  --dataset-uri=s3://ursa-qa/flat-partition

  To run all combinations:
  $ conbench recursive-get-file-info --all=true

Options:
  --dataset-uri [s3://ursa-qa/flat-partition|s3://ursa-qa/wide-partition]
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


benchmark = filesystem_benchmark.GetFileInfoBenchmark()
cases, case_ids = benchmark.cases, benchmark.case_ids


def assert_benchmark(result, case):
    munged = copy.deepcopy(result)
    assert munged["tags"] == {
        "name": "recursive-get-file-info",
        "cpu_count": None,
        "dataset_uri": case[0],
    }
    _asserts.assert_info_and_context(munged)


@pytest.mark.parametrize("case", cases, ids=case_ids)
def test_get_file_info(case):
    [(result, output)] = benchmark.run(case, iterations=1)
    assert_benchmark(result, case)


def test_get_file_info_cli():
    command = ["conbench", "recursive-get-file-info", "--help"]
    _asserts.assert_cli(command, HELP)
