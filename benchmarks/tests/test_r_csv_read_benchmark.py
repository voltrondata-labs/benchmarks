import copy

from .. import r_csv_read_benchmark
from ..tests import _asserts

HELP = """
Usage: conbench r-csv-read [OPTIONS]

  Run r-csv-read benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --source=fanniemae_2016Q4 --compression=uncompressed --output=arrow_table
  --source=fanniemae_2016Q4 --compression=uncompressed --output=data_frame
  --source=fanniemae_2016Q4 --compression=gzip --output=arrow_table
  --source=fanniemae_2016Q4 --compression=gzip --output=data_frame
  --source=nyctaxi_2010-01 --compression=uncompressed --output=arrow_table
  --source=nyctaxi_2010-01 --compression=uncompressed --output=data_frame
  --source=nyctaxi_2010-01 --compression=gzip --output=arrow_table
  --source=nyctaxi_2010-01 --compression=gzip --output=data_frame
  --source=chi_traffic_2020_Q1 --compression=uncompressed --output=arrow_table
  --source=chi_traffic_2020_Q1 --compression=uncompressed --output=data_frame
  --source=chi_traffic_2020_Q1 --compression=gzip --output=arrow_table
  --source=chi_traffic_2020_Q1 --compression=gzip --output=data_frame

  To run all combinations:
  $ conbench r-csv-read --all=true

Options:
  --source [chi_traffic_2020_Q1|fanniemae_2016Q4|nyctaxi_2010-01]
  --compression [gzip|uncompressed]
  --output [arrow_table|data_frame]
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


def assert_benchmark(result, name, language="Python"):
    munged = copy.deepcopy(result)
    expected = {
        "name": name,
        "cpu_count": None,
        "source": "fanniemae_2016Q4",
        "reader": "arrow",
        "compression": "uncompressed",
        "output": "arrow_table",
    }
    if language == "R":
        expected["language"] = "R"
    assert munged["tags"] == expected
    assert result["run_id"] == "some-run-id"
    assert result["batch_id"] == "some-run-id-fanniemae_2016Q4-uncompressed-arrow_table"
    _asserts.assert_info_and_context(munged, language=language)


def test_benchmark_r():
    benchmark = r_csv_read_benchmark.RCsvReadBenchmark()
    run_id = "some-run-id"
    [(result, output)] = benchmark.run(iterations=1, run_id=run_id)
    assert_benchmark(result, benchmark.name, language="R")
    assert _asserts.R_CLI in str(output)


def test_cli():
    command = ["conbench", "r-csv-read", "--help"]
    _asserts.assert_cli(command, HELP)
