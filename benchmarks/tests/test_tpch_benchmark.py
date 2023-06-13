import copy

import conbench.runner
import pytest

from .. import tpch_benchmark
from ..tests import _asserts

HELP = """
Usage: conbench tpch [OPTIONS]

  Run tpch benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --query-id=1 --scale-factor=1 --format=native
  --query-id=1 --scale-factor=1 --format=parquet
  --query-id=2 --scale-factor=1 --format=native
  --query-id=2 --scale-factor=1 --format=parquet
  --query-id=3 --scale-factor=1 --format=native
  --query-id=3 --scale-factor=1 --format=parquet
  --query-id=4 --scale-factor=1 --format=native
  --query-id=4 --scale-factor=1 --format=parquet
  --query-id=5 --scale-factor=1 --format=native
  --query-id=5 --scale-factor=1 --format=parquet
  --query-id=6 --scale-factor=1 --format=native
  --query-id=6 --scale-factor=1 --format=parquet
  --query-id=7 --scale-factor=1 --format=native
  --query-id=7 --scale-factor=1 --format=parquet
  --query-id=8 --scale-factor=1 --format=native
  --query-id=8 --scale-factor=1 --format=parquet
  --query-id=9 --scale-factor=1 --format=native
  --query-id=9 --scale-factor=1 --format=parquet
  --query-id=10 --scale-factor=1 --format=native
  --query-id=10 --scale-factor=1 --format=parquet
  --query-id=11 --scale-factor=1 --format=native
  --query-id=11 --scale-factor=1 --format=parquet
  --query-id=12 --scale-factor=1 --format=native
  --query-id=12 --scale-factor=1 --format=parquet
  --query-id=13 --scale-factor=1 --format=native
  --query-id=13 --scale-factor=1 --format=parquet
  --query-id=14 --scale-factor=1 --format=native
  --query-id=14 --scale-factor=1 --format=parquet
  --query-id=15 --scale-factor=1 --format=native
  --query-id=15 --scale-factor=1 --format=parquet
  --query-id=16 --scale-factor=1 --format=native
  --query-id=16 --scale-factor=1 --format=parquet
  --query-id=17 --scale-factor=1 --format=native
  --query-id=17 --scale-factor=1 --format=parquet
  --query-id=18 --scale-factor=1 --format=native
  --query-id=18 --scale-factor=1 --format=parquet
  --query-id=19 --scale-factor=1 --format=native
  --query-id=19 --scale-factor=1 --format=parquet
  --query-id=20 --scale-factor=1 --format=native
  --query-id=20 --scale-factor=1 --format=parquet
  --query-id=21 --scale-factor=1 --format=native
  --query-id=21 --scale-factor=1 --format=parquet
  --query-id=22 --scale-factor=1 --format=native
  --query-id=22 --scale-factor=1 --format=parquet

  To run all combinations:
  $ conbench tpch --all=true

Options:
  --query-id [1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22]
  --scale-factor [1]
  --format [native|parquet]
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


def assert_benchmark(result, name, language="Python"):
    munged = copy.deepcopy(result)
    expected = {
        "name": name,
        "cpu_count": None,
        "engine": "arrow",
        "memory_map": False,
        "query_id": "TPCH-01",
        "scale_factor": 1,
        "format": "native",
    }
    if language == "R":
        expected["language"] = "R"
    assert munged["tags"] == expected
    assert result["run_id"] == "some-run-id"
    assert result["batch_id"] == "some-run-id-1n"
    _asserts.assert_info_and_context(munged, language=language)


def test_benchmark_r():
    benchmark = tpch_benchmark.TpchBenchmark()
    run_id = "some-run-id"
    [(result, output)] = benchmark.run(iterations=1, run_id=run_id, scale_factor=1)
    assert_benchmark(result, benchmark.name, language="R")
    assert _asserts.R_CLI in str(output)


def test_cli():
    if (
        int(conbench.runner.machine_info(None)["memory_bytes"])
        > 1.1 * 32 * 1024 * 1024 * 1024
    ):
        pytest.skip("CLI options are different on machines with more than 32GB RAM")

    command = ["conbench", "tpch", "--help"]
    _asserts.assert_cli(command, HELP)
