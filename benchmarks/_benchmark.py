import datetime
import functools
import json
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import conbench.runner
import pyarrow

from benchmarks import _sources

logging.basicConfig(format="%(levelname)s: %(message)s")


def _now_formatted() -> str:
    now = datetime.datetime.now(datetime.timezone.utc)
    return now.isoformat()


def github_info(arrow_info: Dict[str, Any]) -> Dict[str, Any]:
    pr_number_env = os.getenv("BENCHMARKABLE_PR_NUMBER", "")
    is_pr = os.getenv("RUN_REASON", "") == "pull request"
    pr_number = int(pr_number_env) if pr_number_env and is_pr else None

    return {
        "repository": "https://github.com/apache/arrow",
        "pr_number": pr_number,
        "commit": arrow_info["arrow_git_revision"],
    }


def arrow_info() -> Dict[str, Any]:
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
        super().__init__()

    @functools.cached_property
    def arrow_info(self) -> Dict[str, Any]:
        return arrow_info()

    @functools.cached_property
    def arrow_info_r(self) -> Dict[str, Any]:
        return self._get_arrow_info_r()

    @functools.cached_property
    def github_info(self) -> Dict[str, Any]:
        return github_info(self.arrow_info)

    def benchmark(
        self,
        f,
        extra_tags: Dict[str, Any],
        options: Dict[str, Any],
        case: Optional[tuple] = None,
    ):
        cpu_count = options.get("cpu_count", None)
        if cpu_count is not None:
            pyarrow.set_cpu_count(cpu_count)
        tags, info, context = self._get_tags_info_context(case, extra_tags)
        try:
            benchmark, output = self.conbench.benchmark(
                f,
                self.name,
                tags=tags,
                info=info,
                context=context,
                github=self.github_info,
                options=options,
                publish=os.environ.get("DRY_RUN") is None,
            )
        except Exception as e:
            benchmark, output = self._handle_error(e, self.name, tags, info, context)
        return benchmark, output

    def record(
        self,
        result,
        extra_tags: Dict[str, Any],
        extra_info,
        extra_context,
        options: Dict[str, Any],
        case: Optional[tuple] = None,
        name: Optional[str] = None,
        output=None,
    ):
        if name is None:
            name = self.name
        tags, info, context = self._get_tags_info_context(case, extra_tags)
        info.update(**extra_info)
        context.update(**extra_context)
        benchmark, output = self.conbench.record(
            result,
            name,
            tags=tags,
            info=info,
            context=context,
            github=self.github_info,
            options=options,
            output=output,
            publish=os.environ.get("DRY_RUN") is None,
        )
        return benchmark, output

    def execute_command(self, command, capture_output=True):
        try:
            print(command)
            result = subprocess.run(command, capture_output=capture_output, check=True)
        except subprocess.CalledProcessError as e:
            print(e.stderr.decode("utf-8"))
            raise e
        # Some benchmarks (e.g., java-micro) produce 12GB+ of stdout that can't be loaded into memory
        # on benchmark machines with 16GB of RAM and without Swap
        if capture_output:
            return result.stdout.decode("utf-8"), result.stderr.decode("utf-8")

    def get_sources(self, source: Union[list, _sources.Source, str]) -> list:
        if isinstance(source, list):
            return source
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

    def version_case(self, case: tuple) -> Optional[int]:
        """
        A method that takes a case and returns a version int

        :param case: A tuple with a value for each argument of the benchmark's parameters

        Overwrite this method to version cases. Incrementing versions will break case history
        because `case_version` will be appended to tags, which are considered in determining
        history. Returning `None` corresponds to no versioning.
        """
        return None

    def get_tags(
        self, options: Dict[str, Any], source: Optional[_sources.Source] = None
    ) -> Dict[str, Any]:
        cpu_tag = {"cpu_count": options.get("cpu_count", None)}
        if source:
            return {**source.tags, **cpu_tag}
        else:
            return cpu_tag

    def _get_tags_info_context(
        self, case: tuple, extra_tags: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        info = {**self.arrow_info}
        info.pop("arrow_git_revision")

        context = {"arrow_compiler_flags": info.pop("arrow_compiler_flags")}

        tags = {**extra_tags}

        if case:
            case_tags = dict(zip(self.fields, case))
            for key in case_tags:
                if key not in tags:
                    tags[key] = case_tags[key]

            case_version = self.version_case(case)
            if case_version:
                tags["case_version"] = case_version

        return tags, info, context

    def _handle_error(
        self,
        e: Exception,
        name: str,
        tags: Dict[str, Any],
        info: Dict[str, Any],
        context: Dict[str, Any],
        r_command: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], None]:
        output = None
        tags["name"] = name
        error = {
            "timestamp": _now_formatted(),
            "tags": tags,
            "info": info,
            "context": context,
            "error": str(e),
        }
        if r_command is not None:
            error["command"] = r_command
        logging.exception(json.dumps(error))
        return error, output


class BenchmarkR(Benchmark):
    arguments = []
    options = {
        "iterations": {"type": int, "default": 1},
        "drop_caches": {"type": bool, "default": "false"},
        "cpu_count": {"type": int},
    }

    def r_benchmark(
        self,
        command: str,
        extra_tags: Dict[str, Any],
        options: Dict[str, Any],
        case: Optional[tuple] = None,
    ):
        tags, info, context = self._get_tags_info_context(case, extra_tags)
        self._add_r_tags_info_context(tags, info, context)
        data = []
        case_version = None
        iterations = options.get("iterations", 1)

        for _ in range(iterations):
            if options.get("drop_caches", False):
                self.conbench.sync_and_drop_caches()
            try:
                result, output = self._get_benchmark_result(command)
                if "stats" in result:
                    # new arrowbench output aligned with conbench
                    data += result["stats"]["data"]
                else:
                    # legacy arrowbench output
                    data.extend([row["real"] for row in result["result"]])
                if not case_version and "case_version" in result["tags"]:
                    case_version = result["tags"]["case_version"]
            except Exception as e:
                return self._handle_error(e, self.name, tags, info, context, command)

        if case_version:
            tags["case_version"] = case_version

        return self.record(
            {"data": data, "unit": "s"},
            tags,
            info,
            context,
            options,
            case=case,
            output=output,
        )

    def r_cpu_count(self, options: Dict[str, Any]):
        cpu_count = options.get("cpu_count", None)
        return "NULL" if cpu_count is None else cpu_count

    def _get_benchmark_result(self, command: str) -> Tuple[Dict[str, Any], str]:
        shutil.rmtree("results", ignore_errors=True)
        output, error = self.conbench.execute_r_command(command, quiet=False)

        result_path = self._get_results_path()
        if not result_path:
            raise Exception(error)
        with open(result_path) as json_file:
            data = json.load(json_file)

        return data, output

    def _get_results_path(self) -> str:
        # R benchmark name can match object name (`r_name`) or Python name (`.name`)
        for path in [Path("results", self.r_name), Path("results", self.name)]:
            for file in path.resolve().glob("*"):
                return file

    def _add_r_tags_info_context(
        self, tags: Dict[str, Any], info: Dict[str, Any], context: Dict[str, Any]
    ) -> None:
        tags["language"] = "R"
        r_info, r_context = self.conbench.get_r_info_and_context()
        info.update(**r_info)
        context.update(**r_context)
        info["arrow_version_r"] = self.arrow_info_r["arrow_version"]

    def _get_arrow_info_r(self) -> Dict[str, str]:
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
    def list(self, classes: Dict[str, Benchmark]) -> List[Benchmark]:
        """List of benchmarks to run for all cases & all sources."""

        def add(
            benchmarks, parts: List[str], flags: Dict[str, Any], exclude: List[str]
        ) -> None:
            if (
                flags["language"] != "C++"
                and flags["language"] != "Java"
                and flags["language"] != "JavaScript"
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
            if iterations:
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
