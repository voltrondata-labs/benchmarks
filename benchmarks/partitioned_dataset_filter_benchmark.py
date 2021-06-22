import conbench.runner

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class PartitionedDatasetFilterBenchmark(_benchmark.BenchmarkR):
    """Read and filter a partitioned dataset."""

    external, r_only = True, True
    name, r_name = "partitioned-dataset-filter", "dataset_taxi_parquet"
    valid_cases = (
        ["query"],
        ["vignette"],
        ["payment_type_3"],
        ["small_no_files"],
        ["count_rows"],
    )

    def run(self, case=None, **kwargs):
        for case in self.get_cases(case, kwargs):
            tags = self.get_tags(kwargs)
            tags["dataset"] = "dataset-taxi-parquet"
            command = self._get_r_command(kwargs, case)
            yield self.r_benchmark(command, tags, kwargs, case)

    def _get_r_command(self, options, case):
        return (
            f"library(arrowbench); "
            f"run_one({self.r_name}, "
            f"cpu_count={self.r_cpu_count(options)}, "
            f'query="{case[0]}")'
        )
