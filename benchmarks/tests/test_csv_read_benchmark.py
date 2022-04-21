import copy

import pytest

from .. import _sources, csv_read_benchmark
from ..tests import _asserts

HELP = """
Usage: conbench csv-read [OPTIONS] SOURCE

  Run csv-read benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --streaming=file --compression=gzip --output=arrow_table
  --streaming=file --compression=gzip --output=data_frame
  --streaming=file --compression=uncompressed --output=arrow_table
  --streaming=file --compression=uncompressed --output=data_frame
  --streaming=streaming --compression=gzip --output=arrow_table
  --streaming=streaming --compression=uncompressed --output=arrow_table

  To run all combinations:
  $ conbench csv-read --all=true

Options:
  --streaming [file|streaming]
  --compression [gzip|uncompressed]
  --output [arrow_table|data_frame]
  --all BOOLEAN                   [default: false]
  --language [Python|R]
  --cpu-count INTEGER
  --iterations INTEGER            [default: 1]
  --drop-caches BOOLEAN           [default: false]
  --gc-collect BOOLEAN            [default: true]
  --gc-disable BOOLEAN            [default: true]
  --show-result BOOLEAN           [default: true]
  --show-output BOOLEAN           [default: false]
  --run-id TEXT                   Group executions together with a run id.
  --run-name TEXT                 Name of run (commit, pull request, etc).
  --help                          Show this message and exit.
"""


benchmark = csv_read_benchmark.CsvReadBenchmark()
sources = [_sources.Source(s) for s in benchmark.sources_test]
cases, case_ids = benchmark.cases, benchmark.case_ids


def assert_benchmark(result, case, source, language):
    munged = copy.deepcopy(result)
    name = "csv-read"
    expected = {
        "name": name,
        "dataset": source,
        "cpu_count": None,
        **benchmark._case_to_param_dict(case=case),
    }
    if language == "R":
        expected["language"] = "R"
    assert munged["tags"] == expected
    _asserts.assert_info_and_context(munged, language=language)


def assert_run_py(run, index, case, source):
    if "data_frame" not in case:
        result, output = run[index]
        assert_benchmark(
            result=result, case=case, source=source.name, language="Python"
        )
        _asserts.assert_table_output(source.name, output)


def assert_run_r(run, index, case, source):
    if "streaming" not in case:
        result, output = run[index]
        assert_benchmark(result=result, case=case, source=source.name, language="R")
        assert _asserts.R_CLI in str(output)


@pytest.mark.parametrize("case", cases, ids=case_ids)
def test_csv_read_py(case):
    if case in benchmark.valid_python_cases:
        run = list(benchmark.run(sources, case, iterations=1))
        assert len(run) == 2
        for x in range(len(run)):
            assert_run_py(run=run, index=x, case=case, source=sources[x])


@pytest.mark.parametrize("case", cases, ids=case_ids)
def test_csv_read_r(case):
    if case in benchmark.valid_r_cases:
        run = list(benchmark.run(sources, case, language="R"))
        assert len(run) == 2
        for x in range(len(run)):
            assert_run_r(run=run, index=x, case=case, source=sources[x])


def test_csv_read_cli():
    command = ["conbench", "csv-read", "--help"]
    _asserts.assert_cli(command, HELP)
