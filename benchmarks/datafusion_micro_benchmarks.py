import conbench.runner

from benchmarks import _criterion


@conbench.runner.register_benchmark
class RecordArrowRustMicroBenchmarks(_criterion.CriterionBenchmark):
    name = "datafusion-micro"
    description = "Run Arrow Datafusion micro benchmarks."
    flags = {
        "language": "Rust",
        "repository": "https://github.com/apache/arrow-datafusion",
    }
