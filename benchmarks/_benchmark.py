import datetime
import json
import logging
import os
import shutil
import subprocess

import click
import conbench.runner
import pyarrow

from benchmarks import _sources


logging.basicConfig(format="%(levelname)s: %(message)s")


def _now_formatted():
    now = datetime.datetime.now(datetime.timezone.utc)
    return now.isoformat()


@conbench.runner.register_list
class BenchmarkList(conbench.runner.BenchmarkList):
    def list(self, classes):
        """List of benchmarks to run for all cases & all sources."""

        def add(benchmarks, parts, flags, exclude):
            command = " ".join(parts)
            if command not in exclude:
                benchmarks.append({"command": command, "flags": flags})

        benchmarks = []
        for name, benchmark in classes.items():
            if name.startswith("example"):
                continue

            instance, parts = benchmark(), [name]

            exclude = getattr(benchmark, "exclude", [])
            if "source" in getattr(benchmark, "arguments", []):
                parts.append("ALL")

            iterations = getattr(instance, "iterations", 3)
            parts.append(f"--iterations={iterations}")

            if instance.cases:
                parts.append("--all=true")

            flags = getattr(instance, "flags", {})

            if getattr(instance, "r_only", False):
                flags["language"] = "R"
                add(benchmarks, parts, flags, exclude)
            else:
                if "language" not in flags:
                    flags["language"] = "Python"
                add(benchmarks, parts, flags, exclude)

                if hasattr(instance, "r_name"):
                    flags_ = flags.copy()
                    flags_["language"] = "R"
                    parts.append("--language=R")
                    add(benchmarks, parts, flags_, exclude)

        return sorted(benchmarks, key=lambda k: k["command"])


def _handle_error(e, name, tags, context, command=None):
    output = None
    tags["name"] = name
    error = {
        "timestamp": _now_formatted(),
        "tags": tags,
        "context": context,
        "error": str(e),
    }
    if command is not None:
        error["command"] = command
    logging.exception(json.dumps(error))
    return error, output


class Benchmark(conbench.runner.Benchmark):
    def __init__(self):
        self.conbench = conbench.runner.Conbench()
        self.arrow_info = self._arrow_info()
        self.run_info = self._run_info(self.arrow_info)
        self.r_info = None

    def benchmark(self, f, extra_tags, options, case=None):
        cpu_count = options.get("cpu_count", None)
        if cpu_count is not None:
            pyarrow.set_cpu_count(cpu_count)
        tags, context = self._get_tags_and_context(case, extra_tags)
        try:
            benchmark, output = self.conbench.benchmark(
                f, self.name, tags, context, self.run_info, options
            )
            self.conbench.publish(benchmark)
        except Exception as e:
            benchmark, output = _handle_error(e, self.name, tags, context)
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

    def get_sources(self, source):
        if not isinstance(source, _sources.Source):
            if source == "ALL":
                return [_sources.Source(s) for s in self.sources]
            if source == "TEST":
                return [_sources.Source(s) for s in self.sources_test]
            return [_sources.Source(source)]
        return [source]

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

    def _r_info(self):
        version, arrow_version = None, None

        r = "cat(version[['version.string']], '\n')"
        command = ["R", "-s", "-q", "-e", r]
        result = subprocess.run(command, capture_output=True)
        if result.returncode == 0:
            version = result.stdout.decode("utf-8").strip()

        r = "packageVersion('arrow')"
        command = ["R", "-s", "-q", "-e", r]
        result = subprocess.run(command, capture_output=True)
        if result.returncode == 0:
            output = result.stdout.decode("utf-8").strip()
            arrow_version = output.split("[1] ")[1].strip("‘").strip("’")

        return {
            "version": version,
            "arrow_version": arrow_version,
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
        try:
            result, output = self._get_benchmark_result(command)
            return self._record_result(
                result,
                extra_tags,
                case,
                options,
                output,
            )
        except Exception as e:
            tags, context = self._get_tags_and_context(case, extra_tags)
            tags["language"] = "R"
            return _handle_error(e, self.name, tags, context, command)

    def r_cpu_count(self, options):
        cpu_count = options.get("cpu_count", None)
        return "NULL" if cpu_count is None else cpu_count

    def _get_benchmark_result(self, command):
        shutil.rmtree("results", ignore_errors=True)
        command = ["R", "-e", command]
        result = subprocess.run(command, capture_output=True)
        output = result.stdout.decode("utf-8").strip()
        error = result.stderr.decode("utf-8").strip()
        if result.returncode != 0:
            raise Exception(error)
        result_path = self._get_results_path()
        with open(result_path) as json_file:
            data = json.load(json_file)
        return data, output

    def _get_results_path(self):
        for file in os.listdir(f"results/{self.r_name}"):
            return os.path.join(f"results/{self.r_name}", file)

    def _record_result(self, result, tags, case, options, output):
        tags["language"] = "R"
        if self.r_info is None:
            self.r_info = self._r_info()

        context = {
            "benchmark_language": "R",
            "benchmark_language_version": self.r_info["version"],
            "arrow_version_r": self.r_info["arrow_version"],
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
