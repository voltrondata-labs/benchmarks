import conbench.runner

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class SimpleBenchmark(_benchmark.Benchmark):
    """Example benchmark with no source, cases, or options."""

    name = "example-simple"
    arguments, options = [], {}

    def run(self, **kwargs):
        tags = self.get_tags(kwargs)
        tags["year"] = "2020"
        f = self._get_benchmark_function()
        yield self.benchmark(f, tags, kwargs)

    def _get_benchmark_function(self):
        return lambda: "hello!"


@conbench.runner.register_benchmark
class ExternalBenchmark(_benchmark.Benchmark):
    """Example benchmark that just records external results."""

    external = True
    name = "example-external"
    arguments, options = [], {}

    def run(self, **kwargs):
        tags = {"year": "2020"}
        context = {"benchmark_language": "C++"}

        # external results from somewhere
        # (an API call, command line execution, etc)
        result = {
            "data": [100, 200, 300],
            "unit": "i/s",
            "times": [0.100, 0.200, 0.300],
            "time_unit": "s",
        }

        yield self.record(
            result,
            tags,
            context,
            kwargs,
            output=result["data"],
        )


@conbench.runner.register_benchmark
class WithoutPythonBenchmark(_benchmark.BenchmarkR):
    """Example R benchmark that doesn't have a Python equivalent."""

    external, r_only = True, True
    name, r_name = "example-R-only", "placebo"
    arguments = []

    def run(self, **kwargs):
        tags = self.get_tags(kwargs)
        tags["year"] = "2020"
        command = self._get_r_command(kwargs)
        yield self.r_benchmark(command, tags, kwargs)

    def _get_r_command(self, options):
        return (
            f"library(arrowbench); "
            f"run_one(arrowbench:::{self.r_name}, "
            f"cpu_count={self.r_cpu_count(options)})"
        )


@conbench.runner.register_benchmark
class CasesBenchmark(_benchmark.Benchmark):
    """Example benchmark with a source, cases, and an option (count)."""

    name = "example-cases"
    valid_cases = (
        ("color", "fruit"),
        ("pink", "apple"),
        ("yellow", "apple"),
        ("green", "apple"),
        ("yellow", "orange"),
        ("pink", "orange"),
    )
    arguments = ["source"]
    options = {"count": {"default": 1, "type": int}}

    def run(self, source, case=None, count=1, **kwargs):
        cases = self.get_cases(case, kwargs)
        for source in self.get_sources(source):
            tags = self._get_tags(source, count)
            for case in cases:
                f = self._get_benchmark_function(source, count, case)
                yield self.benchmark(f, tags, kwargs, case)

    def _get_benchmark_function(self, source, count, case):
        return lambda: count * f"{source.name}, {case}"

    def _get_tags(self, source, count):
        info = {"count": count}
        return {**source.tags, **info}


@conbench.runner.register_benchmark
class SimpleBenchmarkException(_benchmark.Benchmark):
    name = "example-simple-exception"

    def run(self, **kwargs):
        tags = {"year": "2020"}
        f = self._get_benchmark_function()
        yield self.benchmark(f, tags, kwargs)

    def _get_benchmark_function(self):
        return lambda: 100 / 0


@conbench.runner.register_benchmark
class BenchmarkExceptionR(_benchmark.BenchmarkR):
    name, r_name = "example-R-only-exception", "foo"

    def run(self, **kwargs):
        tags = {"year": "2020"}
        command = self._get_r_command()
        yield self.r_benchmark(command, tags, kwargs)

    def _get_r_command(self):
        return f"library(arrowbench); run_one(arrowbench:::{self.r_name})"


@conbench.runner.register_benchmark
class BenchmarkExceptionNoResultR(_benchmark.BenchmarkR):
    name, r_name = "example-R-only-no-result", "placebo"

    def run(self, **kwargs):
        tags = {"year": "2020"}
        command = self._get_r_command()
        yield self.r_benchmark(command, tags, kwargs)

    def _get_r_command(self):
        return f"library(arrowbench); run_one(arrowbench:::{self.r_name}, error_type=1)"


@conbench.runner.register_benchmark
class CasesBenchmarkException(_benchmark.Benchmark):
    name = "example-cases-exception"
    valid_cases = (
        ("color", "fruit"),
        ("pink", "apple"),
        ("yellow", "orange"),
    )

    def run(self, case=None, **kwargs):
        tags = {"year": "2020"}
        cases = self.get_cases(case, kwargs)
        for case in cases:
            f = self._get_benchmark_function()
            yield self.benchmark(f, tags, kwargs, case)

    def _get_benchmark_function(self):
        return lambda: 100 / 0
