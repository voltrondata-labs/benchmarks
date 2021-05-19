import conbench.runner
import pyarrow
import pyarrow.dataset
import pyarrow.fs

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class DatasetReadBenchmark(_benchmark.Benchmark):
    """Read many S3 parquet files into an arrow table."""

    name = "dataset-read"
    valid_cases = (
        ["pre_buffer"],
        ["true"],
        ["false"],
    )
    sources = ["nyctaxi_multi_parquet_s3", "nyctaxi_multi_ipc_s3"]
    sources_test = ["nyctaxi_multi_parquet_s3_sample", "nyctaxi_multi_ipc_s3_sample"]

    # TODO: set --iterations=1 for now to avoid OOM in EC2
    iterations = 1
    flags = {"cloud": True}

    def run(self, source, case=None, **kwargs):
        cases = self.get_cases(case, kwargs)
        for source in self.get_sources(source):
            tags = self.get_tags(kwargs, source)
            format_str = self._get_format_str(source)
            schema = self._get_schema(source, format_str)
            s3 = pyarrow.fs.S3FileSystem(region=source.region)
            for case in cases:
                (pre_buffer,) = case
                legacy, format_ = self._get_format(pre_buffer, format_str)
                case = (None,) if legacy else case
                dataset = pyarrow.dataset.FileSystemDataset.from_paths(
                    source.paths,
                    schema=schema,
                    format=format_,
                    filesystem=s3,
                )
                supports_async = self._get_supports_async(dataset, format_str)
                tags["async"] = supports_async
                f = self._get_benchmark_function(dataset, supports_async)
                yield self.benchmark(f, tags, kwargs, case)
                if legacy:
                    break  # no need to run the null legacy case twice

    def _get_supports_async(self, dataset, format_str):
        # TODO (ARROW-11843): Remove this check, parquet will support async
        if format_str == "parquet":
            return False
        try:
            dataset.scanner(use_async=True)
            return True
        except TypeError:
            return False

    def _get_benchmark_function(self, dataset, supports_async):
        if supports_async:
            return lambda: dataset.to_table(use_async=True)
        else:
            return lambda: dataset.to_table()

    def _get_schema(self, source, format_str):
        # TODO: FileSystemDataset.from_paths() can't currently discover
        # the schema, but pyarrow.dataset.dataset() can. Ideally, we
        # would just be able to omit the schema in FileSystemDataset.
        path = "s3://" + source.paths[0]
        return pyarrow.dataset.dataset(path, format=format_str).schema

    def _get_format_str(self, source):
        if "ipc" in source.name:
            return "ipc"
        else:
            return "parquet"

    def _get_format(self, pre_buffer, format_str):
        if format_str == "ipc":
            return False, pyarrow.dataset.IpcFileFormat()
        # not using actual booleans... see hacks.py in conbench
        pre_buffer = True if pre_buffer == "true" else False
        parquet_format = pyarrow.dataset.ParquetFileFormat
        try:
            return False, parquet_format(pre_buffer=pre_buffer)
        except TypeError:
            return True, parquet_format()
