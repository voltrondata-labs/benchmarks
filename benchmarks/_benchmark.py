import json
import os
import shutil
import subprocess

import conbench.runner
import pyarrow


class Benchmark(conbench.runner.Benchmark):
    def __init__(self):
        self.conbench = conbench.runner.Conbench()
        self.arrow_info = self._arrow_info()
        self.run_info = self._run_info(self.arrow_info)

    def benchmark(self, f, extra_tags, options, case=None):
        cpu_count = options.get("cpu_count", None)
        if cpu_count is not None:
            pyarrow.set_cpu_count(cpu_count)
        tags, context = self._get_tags_and_context(case, extra_tags)
        benchmark, output = self.conbench.benchmark(
            f, self.name, tags, context, self.run_info, options
        )
        self.conbench.publish(benchmark)
        return benchmark, output

    def record(
        self,
        result,
        extra_tags,
        extra_context,
        options,
        case=None,
        name=None,
        output=None,
    ):
        if name is None:
            name = self.name
        tags, context = self._get_tags_and_context(case, extra_tags)
        context.update(**extra_context)
        benchmark, output = self.conbench.record(
            result, name, tags, context, self.run_info, options, output
        )
        self.conbench.publish(benchmark)
        return benchmark, output

    def get_tags(self, source, cpu_count):
        info = {"cpu_count": cpu_count}
        return {**source.tags, **info}

    def _get_tags_and_context(self, case, extra_tags):
        context = {**self.arrow_info}
        tags = {**extra_tags}
        if case:
            tags.update(dict(zip(self.fields, case)))
        return tags, context

    def _run_info(self, arrow_info):
        return {
            "repository": "https://github.com/apache/arrow",
            "commit": arrow_info["arrow_git_revision"],
        }

    def _arrow_info(self):
        if pyarrow.__version__ > "0.17.1":
            build_info = pyarrow.cpp_build_info
            return {
                "arrow_version": build_info.version,
                "arrow_compiler_id": build_info.compiler_id,
                "arrow_compiler_version": build_info.compiler_version,
                "arrow_compiler_flags": build_info.compiler_flags,
                "arrow_git_revision": build_info.git_id,
            }

        return {
            "arrow_version": pyarrow.__version__,
            "arrow_compiler_id": None,
            "arrow_compiler_version": None,
            "arrow_compiler_flags": None,
            "arrow_git_revision": None,
        }


class BenchmarkR:
    def r_benchmark(self, command, extra_tags, options, case=None):
        result, output = self._get_benchmark_result(command)
        return self._record_result(
            result,
            extra_tags,
            case,
            options,
            output,
        )

    def r_cpu_count(self, options):
        cpu_count = options.get("cpu_count", None)
        return "NULL" if cpu_count is None else cpu_count

    def _get_benchmark_result(self, command):
        shutil.rmtree("results", ignore_errors=True)
        command = ["R", "-e", command]
        result = subprocess.run(command, capture_output=True, check=True)
        output = result.stdout.decode("utf-8").strip()
        result_path = self._get_results_path()
        with open(result_path) as json_file:
            data = json.load(json_file)
        return data, output

    def _get_results_path(self):
        for file in os.listdir(f"results/{self.r_name}"):
            return os.path.join(f"results/{self.r_name}", file)

    def _record_result(self, result, tags, case, options, output):
        tags["language"] = "R"
        context = {
            "benchmark_language": "R",
            "benchmark_language_version": "TODO",
        }

        # The benchmark measurement and execution time happen to be
        # the same in this case: both are execution time in seconds.
        # (since data == times, just record an empty list for times)
        data = [row["real"] for row in result["result"]]
        values = {
            "data": data,
            "unit": "s",
            "times": [],
            "time_unit": "s",
        }

        return self.record(
            values,
            tags,
            context,
            options,
            case=case,
            output=output,
        )
