import copy

from .. import tpch_benchmark
from ..tests import _asserts

HELP = """
Usage: conbench tpch [OPTIONS]

  Run tpch benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --query-num=1 --scale=1 --format=native
  --query-num=1 --scale=1 --format=parquet
  --query-num=1 --scale=1 --format=feather
  --query-num=1 --scale=10 --format=native
  --query-num=1 --scale=10 --format=parquet
  --query-num=1 --scale=10 --format=feather
  --query-num=2 --scale=1 --format=native
  --query-num=2 --scale=1 --format=parquet
  --query-num=2 --scale=1 --format=feather
  --query-num=2 --scale=10 --format=native
  --query-num=2 --scale=10 --format=parquet
  --query-num=2 --scale=10 --format=feather
  --query-num=3 --scale=1 --format=native
  --query-num=3 --scale=1 --format=parquet
  --query-num=3 --scale=1 --format=feather
  --query-num=3 --scale=10 --format=native
  --query-num=3 --scale=10 --format=parquet
  --query-num=3 --scale=10 --format=feather
  --query-num=4 --scale=1 --format=native
  --query-num=4 --scale=1 --format=parquet
  --query-num=4 --scale=1 --format=feather
  --query-num=4 --scale=10 --format=native
  --query-num=4 --scale=10 --format=parquet
  --query-num=4 --scale=10 --format=feather
  --query-num=5 --scale=1 --format=native
  --query-num=5 --scale=1 --format=parquet
  --query-num=5 --scale=1 --format=feather
  --query-num=5 --scale=10 --format=native
  --query-num=5 --scale=10 --format=parquet
  --query-num=5 --scale=10 --format=feather
  --query-num=6 --scale=1 --format=native
  --query-num=6 --scale=1 --format=parquet
  --query-num=6 --scale=1 --format=feather
  --query-num=6 --scale=10 --format=native
  --query-num=6 --scale=10 --format=parquet
  --query-num=6 --scale=10 --format=feather

  To run all combinations:
  $ conbench tpch --all=true

Options:
  --query-num [1|2|3|4|5|6]
  --scale [1|10]
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
        "mem_map": False,
        "query_num": 1,
        "scale": 1,
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
