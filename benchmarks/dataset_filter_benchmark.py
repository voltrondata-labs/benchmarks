import conbench.runner
import pyarrow.dataset

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class DatasetFilterBenchmark(_benchmark.Benchmark):
    """Read and filter a dataset."""

    name = "dataset-filter"
    arguments = ["source"]
    sources = ["nyctaxi_2010-01"]
    sources_test = ["nyctaxi_sample"]
    options = {"cpu_count": {"type": int}}

    def run(self, source, cpu_count=None, **kwargs):
        for source in self.get_sources(source):
            path = source.create_if_not_exists("parquet", "lz4")
            dataset = pyarrow.dataset.dataset(path, format="parquet")
            f = self._get_benchmark_function(dataset)
            tags = self.get_tags(source, cpu_count)
            yield self.benchmark(f, tags, kwargs)

    def _get_benchmark_function(self, dataset):
        # for this filter to work, source must be one of:
        #    nyctaxi_sample  --or--  nyctaxi_2010-01
        vendor = pyarrow.dataset.field("vendor_id")
        count = pyarrow.dataset.field("passenger_count")
        return lambda: dataset.to_table(filter=(vendor == "DDS") & (count > 3))


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
            f"run_one(arrowbench:::{self.r_name}, "
            f'n_iter={options.get("iterations", 1)}, '
            f"cpu_count={self.r_cpu_count(options)}, "
            f'query="{case[0]}")'
        )
