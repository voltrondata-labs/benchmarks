import conbench.runner

from benchmarks import _criterion


@conbench.runner.register_benchmark
class RecordArrowRustMicroBenchmarks(_criterion.CriterionBenchmark):
    name = "rust-micro"
    description = "Run Arrow Rust micro benchmarks."
    flags = {
        "language": "Rust",
        "repository": "https://github.com/apache/arrow-rs",
    }
