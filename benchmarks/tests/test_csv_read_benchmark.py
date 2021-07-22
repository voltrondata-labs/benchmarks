import copy
import pytest

from .. import _sources
from .. import csv_read_benchmark
from ..tests._asserts import assert_context, assert_cli


HELP = """
Usage: conbench csv-read [OPTIONS] SOURCE

  Run csv-read benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --streaming=streaming --compression=uncompressed
  --streaming=file --compression=uncompressed
  --streaming=streaming --compression=gzip
  --streaming=file --compression=gzip

  To run all combinations:
  $ conbench csv-read --all=true

Options:
  --streaming [file|streaming]
  --compression [gzip|uncompressed]
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


benchmark = csv_read_benchmark.CsvReadBenchmark()
sources = [_sources.Source(s) for s in benchmark.sources_test]
cases, case_ids = benchmark.cases, benchmark.case_ids


def assert_run(run, index, case, source):
    result, output = run[index]
    munged = copy.deepcopy(result)
    expected = {
        "name": "csv-read",
        "dataset": source.name,
        "cpu_count": None,
        "streaming": case[0],
        "compression": case[1],
    }
    assert munged["tags"] == expected
    assert_context(munged)
    assert "pyarrow.Table" in str(output)


@pytest.mark.parametrize("case", cases, ids=case_ids)
def test_csv_read(case):
    run = list(benchmark.run(sources, case, iterations=1))
    assert len(run) == 2
    for x in range(len(run)):
        assert_run(run, x, case, sources[x])


def test_csv_read_cli():
    command = ["conbench", "csv-read", "--help"]
    assert_cli(command, HELP)
