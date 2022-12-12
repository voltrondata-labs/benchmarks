import itertools
import logging
import os
import time
import uuid

import conbench.runner
import pyarrow
import pyarrow.dataset as ds
from pyarrow.dataset import Dataset

from benchmarks import _benchmark

log = logging.getLogger(__name__)


@conbench.runner.register_benchmark
class DatasetSerializeBenchmark(_benchmark.Benchmark):
    """Serialize dataset to various file formats (parquet, arrow, ...).

    The idea is that this transcoding happens in a streaming like fashion where
    `ds.dataset()` yields a consumable abstraction over a disk-backed data set.

    pyarrow.dataset.write_dataset() then consumes that, writing to disk in a
    specific serialization format.

    Do not actually write to a disk-backed filesystem, but instead to a
    RAM-backed one to not make the timing sensitive to network or disk I/O.

    Note: does reading the input data set really happen from disk while
    `pyarrow.dataset.write_dataset()` is doing its thing? Then this benchmark
    might be strongly affected by the disk/network read I/O performance.
    """

    name = "dataset-serialize"

    arguments = ["source"]

    sources = [
        "nyctaxi_multi_parquet_s3",
        "nyctaxi_multi_ipc_s3",
        "chi_traffic_2020_Q1",
    ]

    sources_test = [
        "nyctaxi_multi_parquet_s3_sample",
        "nyctaxi_multi_ipc_s3_sample",
        "chi_traffic_sample",
    ]

    _params = {
        "selectivity": ("1pc", "10pc"),
        "format": (
            "parquet",
            "arrow",
            "ipc",
            "feather",
            "csv",
        ),
    }

    valid_cases = [tuple(_params.keys())] + list(
        itertools.product(*[v for v in _params.values()])
    )

    # print(f"valid_cases: {valid_cases}")

    filters = {
        "nyctaxi_multi_parquet_s3": {
            "1pc": ds.field("pickup_longitude") < -74.013451,  # 561384
            "10pc": ds.field("pickup_longitude") < -74.002055,  # 5615432
            "100pc": None,  # 56154689
        },
        "nyctaxi_multi_ipc_s3": {
            "1pc": ds.field("pickup_longitude") < -74.014053,  # 596165
            "10pc": ds.field("pickup_longitude") < -74.002708,  # 5962204
            "100pc": None,  # 59616487
        },
        "chi_traffic_2020_Q1": {
            "1pc": ds.field("END_LONGITUDE") < -87.807262,  # 124530
            "10pc": ds.field("END_LONGITUDE") < -87.7624,  # 1307565
            "100pc": None,  # 13038291
        },
        **dict.fromkeys(
            ["nyctaxi_multi_parquet_s3_sample", "nyctaxi_multi_ipc_s3_sample"],
            {
                "1pc": ds.field("pickup_longitude") < -74.0124,  # 20
                "10pc": ds.field("pickup_longitude") < -74.00172,  # 200
                "100pc": None,  # 2000
            },
        ),
        "chi_traffic_sample": {
            "1pc": ds.field("END_LONGITUDE") < -87.80726,  # 10
            "10pc": ds.field("END_LONGITUDE") < -87.76148,  # 100
            "100pc": None,  # 1000
        },
    }

    def _create_tmpdir_in_ramdisk(self, case: tuple):
        # Build simple prefix string to facilitate correlating directory names
        # to test cases.
        pfx = "-".join(c.lower()[:9] for c in case)
        dirpath = os.path.join("/dev/shm", pfx + "-" + str(uuid.uuid4()))
        os.makedirs(dirpath, exist_ok=False)
        return dirpath

    def _get_dataset_for_source(self, source) -> Dataset:

        return pyarrow.dataset.dataset(
            source.source_paths,
            schema=pyarrow.dataset.dataset(
                source.source_paths[0], format=source.format_str
            ).schema,
            format=source.format_str,
        )

    def run(self, source, case=None, **kwargs):

        cases = self.get_cases(case, kwargs)

        for source in self.get_sources(source):

            log.info("source %s: download, if required", source.name)
            source.download_source_if_not_exists()
            tags = self.get_tags(kwargs, source)

            t0 = time.monotonic()
            source_ds = self._get_dataset_for_source(source)
            log.info(
                "constructed Dataset object for source in %.4f s", time.monotonic() - t0
            )

            for case in cases:
                yield self.benchmark(
                    f=self._get_benchmark_function(case, source.name, source_ds),
                    extra_tags=tags,
                    options=kwargs,
                    case=case,
                )

    def _get_benchmark_function(self, case, source_name: str, source_ds: Dataset):

        (selectivity, serialization_format) = case

        log.info("case %s: create directory", case)
        dirpath = self._create_tmpdir_in_ramdisk(case)
        log.info("directory created, path: %s", dirpath)

        # Use a Scanner() object to transparently filter the source dataset
        # upon consumption.
        t0 = time.monotonic()
        filtered_ds = pyarrow.dataset.Scanner.from_dataset(
            source_ds, filter=self.filters[source_name][selectivity]
        )
        log.info("constructed Scanner object for case in %.4f s", time.monotonic() - t0)

        return lambda: pyarrow.dataset.write_dataset(
            data=filtered_ds, format=serialization_format, base_dir=dirpath
        )
