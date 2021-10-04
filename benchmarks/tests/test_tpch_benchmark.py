import copy

from .. import tpch_benchmark
from ..tests import _asserts

HELP = """
Usage: conbench tpch [OPTIONS]

  Run tpch benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --query-id=1 --scale-factor=1 --format=native
  --query-id=1 --scale-factor=1 --format=parquet
  --query-id=1 --scale-factor=1 --format=feather
  --query-id=1 --scale-factor=10 --format=native
  --query-id=1 --scale-factor=10 --format=parquet
  --query-id=1 --scale-factor=10 --format=feather
  --query-id=2 --scale-factor=1 --format=native
  --query-id=2 --scale-factor=1 --format=parquet
  --query-id=2 --scale-factor=1 --format=feather
  --query-id=2 --scale-factor=10 --format=native
  --query-id=2 --scale-factor=10 --format=parquet
  --query-id=2 --scale-factor=10 --format=feather
  --query-id=3 --scale-factor=1 --format=native
  --query-id=3 --scale-factor=1 --format=parquet
  --query-id=3 --scale-factor=1 --format=feather
  --query-id=3 --scale-factor=10 --format=native
  --query-id=3 --scale-factor=10 --format=parquet
  --query-id=3 --scale-factor=10 --format=feather
  --query-id=4 --scale-factor=1 --format=native
  --query-id=4 --scale-factor=1 --format=parquet
  --query-id=4 --scale-factor=1 --format=feather
  --query-id=4 --scale-factor=10 --format=native
  --query-id=4 --scale-factor=10 --format=parquet
  --query-id=4 --scale-factor=10 --format=feather
  --query-id=5 --scale-factor=1 --format=native
  --query-id=5 --scale-factor=1 --format=parquet
  --query-id=5 --scale-factor=1 --format=feather
  --query-id=5 --scale-factor=10 --format=native
  --query-id=5 --scale-factor=10 --format=parquet
  --query-id=5 --scale-factor=10 --format=feather
  --query-id=6 --scale-factor=1 --format=native
  --query-id=6 --scale-factor=1 --format=parquet
  --query-id=6 --scale-factor=1 --format=feather
  --query-id=6 --scale-factor=10 --format=native
  --query-id=6 --scale-factor=10 --format=parquet
  --query-id=6 --scale-factor=10 --format=feather
  --query-id=7 --scale-factor=1 --format=native
  --query-id=7 --scale-factor=1 --format=parquet
  --query-id=7 --scale-factor=1 --format=feather
  --query-id=7 --scale-factor=10 --format=native
  --query-id=7 --scale-factor=10 --format=parquet
  --query-id=7 --scale-factor=10 --format=feather
  --query-id=8 --scale-factor=1 --format=native
  --query-id=8 --scale-factor=1 --format=parquet
  --query-id=8 --scale-factor=1 --format=feather
  --query-id=8 --scale-factor=10 --format=native
  --query-id=8 --scale-factor=10 --format=parquet
  --query-id=8 --scale-factor=10 --format=feather
  --query-id=9 --scale-factor=1 --format=native
  --query-id=9 --scale-factor=1 --format=parquet
  --query-id=9 --scale-factor=1 --format=feather
  --query-id=9 --scale-factor=10 --format=native
  --query-id=9 --scale-factor=10 --format=parquet
  --query-id=9 --scale-factor=10 --format=feather
  --query-id=10 --scale-factor=1 --format=native
  --query-id=10 --scale-factor=1 --format=parquet
  --query-id=10 --scale-factor=1 --format=feather
  --query-id=10 --scale-factor=10 --format=native
  --query-id=10 --scale-factor=10 --format=parquet
  --query-id=10 --scale-factor=10 --format=feather

  To run all combinations:
  $ conbench tpch --all=true

Options:
  --query-id [1|2|3|4|5|6|7|8|9|10]
  --scale-factor [1|10]
  --format [feather|native|parquet]
  --all BOOLEAN                   [default: False]
  --iterations INTEGER            [default: 1]
  --drop-caches BOOLEAN           [default: False]
  --cpu-count INTEGER
  --show-result BOOLEAN           [default: True]
  --show-output BOOLEAN           [default: False]
  --run-id TEXT                   Group executions together with a run id.
  --run-name TEXT                 Name of run (commit, pull request, etc).
  --help                          Show this message and exit.
"""


def assert_benchmark(result, name, language="Python"):
    munged = copy.deepcopy(result)
    expected = {
        "name": name,
        "cpu_count": None,
        "engine": "arrow",
        "memory_map": False,
        "query_id": 1,
        "scale_factor": 1,
        "format": "native",
    }
    if language == "R":
        expected["language"] = "R"
    assert munged["tags"] == expected
    _asserts.assert_context(munged, language=language)


def test_benchmark():
    benchmark = tpch_benchmark.TpchBenchmark()
    [(result, output)] = benchmark.run(iterations=1)
    assert_benchmark(result, benchmark.name, language="R")
    assert _asserts.R_CLI in str(output)


def test_cli():
    command = ["conbench", "tpch", "--help"]
    _asserts.assert_cli(command, HELP)
