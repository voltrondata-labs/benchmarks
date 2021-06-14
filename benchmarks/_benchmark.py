import datetime
import json
import logging
import os
import shutil

import conbench.runner
import pyarrow

from benchmarks import _sources


logging.basicConfig(format="%(levelname)s: %(message)s")


def _now_formatted():
    now = datetime.datetime.now(datetime.timezone.utc)
    return now.isoformat()


def github_info(arrow_info):
    return {
        "repository": "https://github.com/apache/arrow",
        "commit": arrow_info["arrow_git_revision"],
    }


def arrow_info():
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


class Benchmark(conbench.runner.Benchmark):
    arguments = []
    options = {"cpu_count": {"type": int}}

    def __init__(self):
        self.conbench = conbench.runner.Conbench()
        self._github_info = None
        self._arrow_info = None
        self._arrow_info_r = None

    @property
    def arrow_info(self):
        if self._arrow_info is None:
            self._arrow_info = arrow_info()
        return self._arrow_info

    @property
    def arrow_info_r(self):
        if self._arrow_info_r is None:
            self._arrow_info_r = self._get_arrow_info_r()
        return self._arrow_info_r

    @property
    def github_info(self):
        if self._github_info is None:
            self._github_info = github_info(self.arrow_info)
        return self._github_info

    def benchmark(self, f, extra_tags, options, case=None):
        cpu_count = options.get("cpu_count", None)
        if cpu_count is not None:
            pyarrow.set_cpu_count(cpu_count)
        tags, context = self._get_tags_and_context(case, extra_tags)
        try:
            benchmark, output = self.conbench.benchmark(
                f,
                self.name,
                tags=tags,
                context=context,
                github=self.github_info,
                options=options,
            )
        except Exception as e:
            benchmark, output = self._handle_error(e, self.name, tags, context)
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
            result,
            name,
            tags=tags,
            context=context,
            github=self.github_info,
            options=options,
            output=output,
        )
        return benchmark, output

    def get_sources(self, source):
        if isinstance(source, _sources.Source):
            return [source]
        if source == "ALL":
            return [_sources.Source(s) for s in self.sources]
        if source == "TEST":
            return [_sources.Source(s) for s in self.sources_test]

        available_sources = [
            "ALL",
            "TEST",
            *self.sources,
            *self.sources_test,
        ]
        if source not in available_sources:
            msg = f"Source can only be one of {available_sources}."
            raise Exception(msg)

        return [_sources.Source(source)]

    def get_tags(self, options, source=None):
        info = {"cpu_count": options.get("cpu_count", None)}
        if source:
            return {**source.tags, **info}
        else:
            return info

    def _get_tags_and_context(self, case, extra_tags):
        context = {**self.arrow_info}
        context.pop("arrow_git_revision")
        tags = {**extra_tags}
        if case:
            tags.update(dict(zip(self.fields, case)))
        return tags, context

    def _handle_error(self, e, name, tags, context, r_command=None):
        output = None
        tags["name"] = name
        error = {
            "timestamp": _now_formatted(),
            "tags": tags,
            "context": context,
            "error": str(e),
        }
        if r_command is not None:
            error["command"] = r_command
        else:
            context.update(self.conbench.python_info)
        logging.exception(json.dumps(error))
        return error, output


class BenchmarkR(Benchmark):
    arguments = []
    options = {
        "iterations": {"default": 1, "type": int},
        "drop_caches": {"type": bool, "default": "false"},
        "cpu_count": {"type": int},
    }

    def r_benchmark(self, command, extra_tags, options, case=None):
        tags, context = self._get_tags_and_context(case, extra_tags)
        tags, context = self._add_r_tags_and_context(tags, context)
        data, iterations = [], options.get("iterations", 1)

        for _ in range(iterations):
            if options.get("drop_caches", False):
                self.conbench.sync_and_drop_caches()
            try:
                result, output = self._get_benchmark_result(command)
                data.extend([row["real"] for row in result["result"]])
            except Exception as e:
                return self._handle_error(e, self.name, tags, context, command)

        return self.record(
            {"data": data, "unit": "s"},
            tags,
            context,
            options,
            case=case,
            output=output,
        )

    def r_cpu_count(self, options):
        cpu_count = options.get("cpu_count", None)
        return "NULL" if cpu_count is None else cpu_count

    def _get_benchmark_result(self, command):
        shutil.rmtree("results", ignore_errors=True)
        output, error = self.conbench.execute_r_command(command, quiet=False)
        try:
            result_path = self._get_results_path()
            with open(result_path) as json_file:
                data = json.load(json_file)
        except FileNotFoundError:
            raise Exception(error)
        return data, output

    def _get_results_path(self):
        for file in os.listdir(f"results/{self.r_name}"):
            return os.path.join(f"results/{self.r_name}", file)

    def _add_r_tags_and_context(self, tags, context):
        tags["language"] = "R"
        r_context = self.conbench.r_info
        r_context["arrow_version_r"] = self.arrow_info_r["arrow_version"]
        context.update(r_context)
        return tags, context

    def _get_arrow_info_r(self):
        r = "packageVersion('arrow')"
        output, _ = self.conbench.execute_r_command(r)
        version = output.split("[1] ")[1].strip("‘").strip("’")
        return {"arrow_version": version}


class BenchmarkPythonR(BenchmarkR):
    arguments = []
    options = {
        "language": {"type": str, "choices": ["Python", "R"]},
        "cpu_count": {"type": int},
    }


@conbench.runner.register_list
class BenchmarkList(conbench.runner.BenchmarkList):
    def list(self, classes):
        """List of benchmarks to run for all cases & all sources."""

        def add(benchmarks, parts, flags, exclude):
            if (
                flags["language"] != "C++"
                and flags["language"] != "Java"
                and "--drop-caches=true" not in parts
            ):
                parts.append("--drop-caches=true")
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
