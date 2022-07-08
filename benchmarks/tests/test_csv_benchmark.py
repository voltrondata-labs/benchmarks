import pytest

from .. import _sources, csv_benchmark
from . import _asserts


class TestCsvBenchmark:
    def assert_benchmark(self, result, case, source, language):
        expected = {
            "name": self.benchmark.name,
            "dataset": source,
            "cpu_count": None,
            **self.benchmark._case_to_param_dict(case=case),
        }
        if language == "R":
            expected["language"] = "R"
        assert result.tags == expected
        _asserts.assert_info_and_context(result, language=language)

    def assert_run_py(self, run, index, case, source):
        if "data_frame" not in case:
            result = run[index]
            self.assert_benchmark(
                result=result, case=case, source=source.name, language="Python"
            )
            if self.benchmark.name == "csv-read":
                _asserts.assert_table_output(source.name, result.output)

    def assert_run_r(self, run, index, case, source):
        if "streaming" not in case:
            result = run[index]
            self.assert_benchmark(
                result=result, case=case, source=source.name, language="R"
            )
            assert _asserts.R_CLI in str(result.output)


class TestCsvReadBenchmark(TestCsvBenchmark):
    benchmark = csv_benchmark.CsvReadBenchmark()
    sources = [_sources.Source(s) for s in benchmark.sources_test]
    cases, case_ids = benchmark.cases, benchmark.case_ids

    HELP = """
  Usage: conbench csv-read [OPTIONS] SOURCE

  Run csv-read benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --streaming=file --compression=gzip --output-format=arrow_table
  --streaming=file --compression=gzip --output-format=data_frame
  --streaming=file --compression=uncompressed --output-format=arrow_table
  --streaming=file --compression=uncompressed --output-format=data_frame
  --streaming=streaming --compression=gzip --output-format=arrow_table
  --streaming=streaming --compression=uncompressed --output-format=arrow_table

  To run all combinations:
  $ conbench csv-read --all=true

Options:
  --streaming [file|streaming]
  --compression [gzip|uncompressed]
  --output-format [arrow_table|data_frame]
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
  --run-name TEXT                 Free-text name of run (commit ABC, pull
                                  request 123, etc).
  --run-reason TEXT               Low-cardinality reason for run (commit, pull
                                  request, manual, etc).
  --help                          Show this message and exit.
    """

    @pytest.mark.parametrize("case", cases, ids=case_ids)
    def test_csv_read_py(self, case):
        if case in self.benchmark.valid_python_cases:
            run = list(self.benchmark.run(self.sources, case, iterations=1))
            assert len(run) == 2
            for x in range(len(run)):
                self.assert_run_py(run=run, index=x, case=case, source=self.sources[x])

    @pytest.mark.parametrize("case", cases, ids=case_ids)
    def test_csv_read_r(self, case):
        if case in self.benchmark.valid_r_cases:
            run = list(self.benchmark.run(self.sources, case, language="R"))
            assert len(run) == 2
            for x in range(len(run)):
                self.assert_run_r(run=run, index=x, case=case, source=self.sources[x])

    def test_csv_read_cli(self):
        command = ["conbench", "csv-read", "--help"]
        _asserts.assert_cli(command, self.HELP)


class TestCsvWriteBenchmark(TestCsvBenchmark):
    benchmark = csv_benchmark.CsvWriteBenchmark()
    sources = [_sources.Source(s) for s in benchmark.sources_test]
    cases, case_ids = benchmark.cases, benchmark.case_ids

    HELP = """
    Usage: conbench csv-write [OPTIONS] SOURCE

  Run csv-write benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --streaming=file --compression=gzip --input=arrow_table
  --streaming=file --compression=uncompressed --input=arrow_table
  --streaming=file --compression=uncompressed --input=data_frame
  --streaming=streaming --compression=gzip --input=arrow_table
  --streaming=streaming --compression=uncompressed --input=arrow_table

  To run all combinations:
  $ conbench csv-write --all=true

Options:
  --streaming [file|streaming]
  --compression [gzip|uncompressed]
  --input [arrow_table|data_frame]
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
  --run-name TEXT                 Free-text name of run (commit ABC, pull
                                  request 123, etc).
  --run-reason TEXT               Low-cardinality reason for run (commit, pull
                                  request, manual, etc).
  --help                          Show this message and exit.
    """

    @pytest.mark.parametrize("case", cases, ids=case_ids)
    def test_csv_write_py(self, case):
        if case in self.benchmark.valid_python_cases:
            run = list(self.benchmark.run(self.sources, case, iterations=1))
            assert len(run) == 2
            for x in range(len(run)):
                self.assert_run_py(run=run, index=x, case=case, source=self.sources[x])

    @pytest.mark.parametrize("case", cases, ids=case_ids)
    def test_csv_write_r(self, case):
        if case in self.benchmark.valid_r_cases:
            run = list(self.benchmark.run(self.sources, case, language="R"))
            assert len(run) == 2
            for x in range(len(run)):
                self.assert_run_r(run=run, index=x, case=case, source=self.sources[x])

    def test_csv_write_cli(self):
        command = ["conbench", "csv-write", "--help"]
        _asserts.assert_cli(command, self.HELP)
