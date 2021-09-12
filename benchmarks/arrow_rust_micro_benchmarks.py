import conbench.runner

from benchmarks import _criterion


@conbench.runner.register_benchmark
class RecordArrowRustMicroBenchmarks(_criterion.CriterionBenchmark):
    name = "arrow-rust"
    description = "Record Arrow Rust micro benchmark results."
    flags = {
        "language": "Rust",
        "repository": "https://github.com/apache/arrow-rs",
    }
