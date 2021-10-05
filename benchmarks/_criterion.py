import collections
import csv
import os
import pathlib

from conbench.machine_info import github_info

from benchmarks import _benchmark


def _result_in_seconds(row):
    # sample_measured_value - The value of the measurement for this sample.
    # Note that this is the measured value for the whole sample, not the
    # time-per-iteration To calculate the time-per-iteration, use
    # sample_measured_value/iteration_count
    # -- https://bheisler.github.io/criterion.rs/book/user_guide/csv_output.html
    count = int(row["iteration_count"])
    sample = float(row["sample_measured_value"])
    return sample / count / 10 ** 9


def _parse_benchmark_group(row):
    parts = row["group"].split(",")
    if len(parts) > 1:
        suite, name = parts[0], ", ".join(parts[1:])
    else:
        suite, name = row["group"], row["group"]
    return suite, name


def _read_results(src_dir):
    results = collections.defaultdict(lambda: collections.defaultdict(list))
    path = pathlib.Path(os.path.join(src_dir, "target", "criterion"))
    for path in list(path.glob("**/new/raw.csv")):
        with open(path) as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                suite, name = _parse_benchmark_group(row)
                results[suite][name].append(_result_in_seconds(row))
    return results


class CriterionBenchmark(_benchmark.ExternalRepository):
    external, iterations = True, None
    options = {"src_dir": {"type": str}}
    exclude = ["datafusion-micro", "rust-micro"]  # TODO: turn off for now

    def run(self, **kwargs):
        src_dir = kwargs["src_dir"]
        self._cargo_bench(src_dir)
        results = _read_results(src_dir)
        for suite in results:
            self.conbench.mark_new_batch()
            for name, data in results[suite].items():
                yield self._record_result(suite, name, data, kwargs)

    def _cargo_bench(self, source_dir):
        os.chdir(source_dir)
        self.execute_command(["cargo", "bench"])

    def _record_result(self, suite, name, data, options):
        tags = {"suite": suite}
        result = {"data": data, "unit": "s"}
        context = {"benchmark_language": "Rust"}
        github = github_info()
        return self.record(
            result,
            name,
            tags,
            context,
            github,
            options,
        )
