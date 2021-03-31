import conbench.runner

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class PartitionedDatasetFilterBenchmark(_benchmark.Benchmark, _benchmark.BenchmarkR):
    """
    Read and filter a partitioned dataset.

        PartitionedDatasetFilterBenchmark().run(<case>, options...)

    Parameters
    ----------
    case : str
        A case name from the benchmark (one of: "vignette",
        "payment_type_3", "small_no_files")
    cpu_count : int, optional
        Set the number of threads to use in parallel operations (arrow).
    iterations : int, default 1
        Number of times to run the benchmark.
    run_id : str, optional
        Group executions together with a run id.
    run_name : str, optional
        Name of run (commit, pull request, etc).

    Returns
    -------
    (result, output) : sequence
        result : The benchmark result.
        output : The output from the benchmarked function.
    """

    external, r_only = True, True
    name, r_name = "partitioned-dataset-filter", "dataset_taxi_parquet"
    valid_cases = (
        ["query"],
        ["vignette"],
        ["payment_type_3"],
        ["small_no_files"],
    )
    options = {
        "iterations": {"default": 3, "type": int},
        "cpu_count": {"type": int},
    }

    def run(self, case=None, **kwargs):
        cases = self.get_cases(case, kwargs)
        for case in cases:
            tags = {
                "dataset": "dataset-taxi-parquet",
                "cpu_count": kwargs.get("cpu_count"),
            }
            command = self._get_r_command(kwargs, case)
            yield self.r_benchmark(command, tags, kwargs, case)

    def _get_r_command(self, options, case):
        return (
            f"library(arrowbench); "
            f"run_one({self.r_name}, "
            f'n_iter={options.get("iterations", 1)}, '
            f"cpu_count={self.r_cpu_count(options)}, "
            f'query="{case[0]}")'
        )
