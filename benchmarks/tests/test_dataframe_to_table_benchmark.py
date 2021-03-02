import json

from .. import _sources
from .. import dataframe_to_table_benchmark
from ..tests._asserts import assert_benchmark, assert_cli


HELP = """
Usage: conbench dataframe-to-table [OPTIONS] SOURCE

  Run dataframe-to-table benchmark.

Options:
  --language [Python|R]
  --cpu-count INTEGER
  --iterations INTEGER   [default: 1]
  --gc-collect BOOLEAN   [default: true]
  --gc-disable BOOLEAN   [default: true]
  --show-result BOOLEAN  [default: true]
  --show-output BOOLEAN  [default: false]
  --run-id TEXT          Group executions together with a run id.
  --help                 Show this message and exit.
"""


chi_traffic = _sources.Source("chi_traffic_sample")


def test_dataframe_to_table_chi_traffic():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    [(result, output)] = benchmark.run(chi_traffic, iterations=1)
    assert_benchmark(result, chi_traffic.name, benchmark.name)
    print(json.dumps(result, indent=4, sort_keys=True))
    assert "pyarrow.Table" in str(output)


def test_dataframe_to_table_cli():
    command = ["conbench", "dataframe-to-table", "--help"]
    assert_cli(command, HELP)
