import conbench.runner

from benchmarks import _benchmark, _sources


@conbench.runner.register_benchmark
class SimpleBenchmark(_benchmark.Benchmark):
    """Example benchmark with no source, cases, or options.

    $ conbench example-simple --help
    Usage: conbench example-simple [OPTIONS]

      Run example-simple benchmark.

    Options:
      --iterations INTEGER   [default: 1]
      --gc-collect BOOLEAN   [default: true]
      --gc-disable BOOLEAN   [default: true]
      --show-result BOOLEAN  [default: true]
      --show-output BOOLEAN  [default: false]
      --run-id TEXT          Group executions together with a run id.
      --help                 Show this message and exit.
    """

    name = "example-simple"

    def run(self, **kwargs):
        tags = {"year": "2020"}
        f = self._get_benchmark_function()
        yield self.benchmark(f, tags, kwargs)

    def _get_benchmark_function(self):
        return lambda: "hello!"


@conbench.runner.register_benchmark
class RecordExternalBenchmark(_benchmark.Benchmark):
    """Example benchmark that just records external results.

    $ conbench example-external --help
    Usage: conbench example-external [OPTIONS]

      Run example-external benchmark.

    Options:
      --show-result BOOLEAN  [default: true]
      --show-output BOOLEAN  [default: false]
      --run-id TEXT          Group executions together with a run id.
      --help                 Show this message and exit.
    """

    name = "example-external"
    external = True

    def run(self, **kwargs):
        tags = {"year": "2020"}
        context = {"benchmark_language": "C++"}

        # external results from somewhere
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
class CasesBenchmark(_benchmark.Benchmark):
    """Example benchmark with a source, cases, and an option (count).

    $ conbench example-cases --help
    Usage: conbench example-cases [OPTIONS] SOURCE

      Run example-cases benchmark(s).

      For each benchmark option, the first option value is the default.

      Valid benchmark combinations:
      --color=pink --fruit=apple
      --color=yellow --fruit=apple
      --color=green --fruit=apple
      --color=yellow --fruit=orange
      --color=pink --fruit=orange

      To run all combinations:
      $ conbench example-cases --all=true

    Options:
      --all BOOLEAN                [default: false]
      --color [pink|yellow|green]
      --fruit [apple|orange]
      --count INTEGER              [default: 1]
      --iterations INTEGER         [default: 1]
      --gc-collect BOOLEAN         [default: true]
      --gc-disable BOOLEAN         [default: true]
      --show-result BOOLEAN        [default: true]
      --show-output BOOLEAN        [default: false]
      --run-id TEXT                Group executions together with a run id.
      --help                       Show this message and exit.
    """

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
        if not isinstance(source, _sources.Source):
            source = _sources.Source(source)

        cases = self.get_cases(case, kwargs)
        tags = self._get_tags(source, count)
        for case in cases:
            f = self._get_benchmark_function(source, count, case)
            yield self.benchmark(f, tags, kwargs, case)

    def _get_benchmark_function(self, source, count, case):
        return lambda: count * f"{source.name}, {case}"

    def _get_tags(self, source, count):
        info = {"count": count}
        return {**source.tags, **info}
