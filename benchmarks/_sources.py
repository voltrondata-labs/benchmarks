import functools
import os
import pathlib
from enum import Enum

import pyarrow
import pyarrow.csv
import pyarrow.feather as feather
import pyarrow.parquet as parquet
import requests

this_dir = os.path.dirname(os.path.abspath(__file__))
local_data_dir = os.path.join(this_dir, "data")
data_dir = os.getenv("BENCHMARKS_DATA_DIR", local_data_dir)
temp_dir = os.path.join(data_dir, "temp")


def _local(name):
    """Sources for unit testing, committed to benchmarks/data."""
    return os.path.join(local_data_dir, name)


def _source(name):
    """Sources downloaded from S3 and otherwise untouched."""
    return os.path.join(data_dir, name)


def _temp(name):
    """Sources generated from the canonical sources."""
    return os.path.join(temp_dir, name)


def munge_compression(c, file_type):
    if file_type == "parquet":
        c = "NONE" if c == "uncompressed" else c
    elif file_type == "csv" and c == "uncompressed":
        return None
    return c.lower() if file_type == "feather" else c.upper()


class SourceFormat(Enum):
    CSV = "csv"
    PARQUET = "parquet"
    FEATHER = "feather"


# This is a reconstructed schema; it may not be completely accurate, but should
# be serviceably so. For more information on variables, see the sources below
# available from Fannie Mae. You may need to create an account first here:
# https://datadynamics.fanniemae.com/data-dynamics/#/reportMenu;category=HP
#
# Data dictionary: https://capitalmarkets.fanniemae.com/resources/file/credit-risk/xls/crt-file-layout-and-glossary.xlsx
# Names from R code here: https://capitalmarkets.fanniemae.com/media/document/zip/FNMA_SF_Loan_Performance_r_Primary.zip
fannie_mae_schema = pyarrow.schema(
    [
        pyarrow.field("LOAN_ID", pyarrow.string()),
        # date. Monthly reporting period
        pyarrow.field("ACT_PERIOD", pyarrow.string()),
        pyarrow.field("SERVICER", pyarrow.string()),
        pyarrow.field("ORIG_RATE", pyarrow.float64()),
        pyarrow.field("CURRENT_UPB", pyarrow.float64()),
        pyarrow.field("LOAN_AGE", pyarrow.int32()),
        pyarrow.field("REM_MONTHS", pyarrow.int32()),
        pyarrow.field("ADJ_REM_MONTHS", pyarrow.int32()),
        # maturity date
        pyarrow.field("MATR_DT", pyarrow.string()),
        # Metropolitan Statistical Area code
        pyarrow.field("MSA", pyarrow.string()),
        # Int of months, but `X` is a valid value. New versions pad with `0`/`X` to two characters
        pyarrow.field("DLQ_STATUS", pyarrow.string()),
        pyarrow.field("RELOCATION_MORTGAGE_INDICATOR", pyarrow.string()),
        # 0-padded 2 digit ints representing categorical levels, e.g. "01" -> "Prepaid or Matured"
        pyarrow.field("Zero_Bal_Code", pyarrow.string()),
        pyarrow.field("ZB_DTE", pyarrow.string()),  # date
        pyarrow.field("LAST_PAID_INSTALLMENT_DATE", pyarrow.string()),
        pyarrow.field("FORECLOSURE_DATE", pyarrow.string()),
        pyarrow.field("DISPOSITION_DATE", pyarrow.string()),
        pyarrow.field("FORECLOSURE_COSTS", pyarrow.float64()),
        pyarrow.field("PROPERTY_PRESERVATION_AND_REPAIR_COSTS", pyarrow.float64()),
        pyarrow.field("ASSET_RECOVERY_COSTS", pyarrow.float64()),
        pyarrow.field("MISCELLANEOUS_HOLDING_EXPENSES_AND_CREDITS", pyarrow.float64()),
        pyarrow.field("ASSOCIATED_TAXES_FOR_HOLDING_PROPERTY", pyarrow.float64()),
        pyarrow.field("NET_SALES_PROCEEDS", pyarrow.float64()),
        pyarrow.field("CREDIT_ENHANCEMENT_PROCEEDS", pyarrow.float64()),
        pyarrow.field("REPURCHASES_MAKE_WHOLE_PROCEEDS", pyarrow.float64()),
        pyarrow.field("OTHER_FORECLOSURE_PROCEEDS", pyarrow.float64()),
        pyarrow.field("NON_INTEREST_BEARING_UPB", pyarrow.float64()),
        # all null
        pyarrow.field("MI_CANCEL_FLAG", pyarrow.string()),
        pyarrow.field("RE_PROCS_FLAG", pyarrow.string()),
        # all null
        pyarrow.field("LOAN_HOLDBACK_INDICATOR", pyarrow.string()),
        pyarrow.field("SERV_IND", pyarrow.string()),
    ]
)

nyctaxi_schema = pyarrow.schema(
    [
        pyarrow.field("vendor_id", pyarrow.string()),
        pyarrow.field("pickup_datetime", pyarrow.timestamp("ns")),
        pyarrow.field("dropoff_datetime", pyarrow.timestamp("ns")),
        pyarrow.field("passenger_count", pyarrow.int64()),
        pyarrow.field("trip_distance", pyarrow.float64()),
        pyarrow.field("pickup_longitude", pyarrow.float64()),
        pyarrow.field("pickup_latitude", pyarrow.float64()),
        pyarrow.field("rate_code", pyarrow.int64()),
        pyarrow.field("store_and_fwd_flag", pyarrow.float64()),
        pyarrow.field("dropoff_longitude", pyarrow.float64()),
        pyarrow.field("dropoff_latitude", pyarrow.float64()),
        pyarrow.field("payment_type", pyarrow.string()),
        pyarrow.field("fare_amount", pyarrow.float64()),
        pyarrow.field("surcharge", pyarrow.float64()),
        pyarrow.field("mta_tax", pyarrow.float64()),
        pyarrow.field("tip_amount", pyarrow.float64()),
        pyarrow.field("tolls_amount", pyarrow.float64()),
        pyarrow.field("total_amount", pyarrow.float64()),
    ]
)


STORE = {
    "fanniemae_sample": {
        "path": _local("fanniemae_sample.csv"),
        "sep": "|",
        "header": None,
        "schema": fannie_mae_schema,
        "format": SourceFormat.CSV,
    },
    "nyctaxi_sample": {
        "path": _local("nyctaxi_sample.csv"),
        "sep": ",",
        "header": 0,
        "schema": nyctaxi_schema,
        "format": SourceFormat.CSV,
    },
    "chi_traffic_sample": {
        "path": _local("chi_traffic_sample.parquet"),
        "format": SourceFormat.PARQUET,
    },
    "fanniemae_2016Q4": {
        "path": _source("fanniemae_2016Q4.csv.gz"),
        "source": "https://ursa-qa.s3.amazonaws.com/fanniemae_loanperf/2016Q4.csv.gz",
        "sep": "|",
        "header": None,
        "schema": fannie_mae_schema,
        "format": SourceFormat.CSV,
    },
    "nyctaxi_2010-01": {
        "path": _source("nyctaxi_2010-01.csv.gz"),
        "source": "https://ursa-qa.s3.amazonaws.com/nyctaxi/yellow_tripdata_2010-01.csv.gz",
        "sep": ",",
        "header": 0,
        "schema": nyctaxi_schema,
        "format": SourceFormat.CSV,
    },
    "chi_traffic_2020_Q1": {
        "path": _source("chi_traffic_2020_Q1.parquet"),
        "source": "https://ursa-qa.s3.amazonaws.com/chitraffic/chi_traffic_2020_Q1.parquet",
        "format": SourceFormat.PARQUET,
    },
    "type_strings": {
        "path": _source("type_strings.parquet"),
        "source": "https://ursa-qa.s3.amazonaws.com/single_types/type_strings.parquet",
        "format": SourceFormat.PARQUET,
    },
    "type_dict": {
        "path": _source("type_dict.parquet"),
        "source": "https://ursa-qa.s3.amazonaws.com/single_types/type_dict.parquet",
        "format": SourceFormat.PARQUET,
    },
    "type_integers": {
        "path": _source("type_integers.parquet"),
        "source": "https://ursa-qa.s3.amazonaws.com/single_types/type_integers.parquet",
        "format": SourceFormat.PARQUET,
    },
    "type_floats": {
        "path": _source("type_floats.parquet"),
        "source": "https://ursa-qa.s3.amazonaws.com/single_types/type_floats.parquet",
        "format": SourceFormat.PARQUET,
    },
    "type_nested": {
        "path": _source("type_nested.parquet"),
        "source": "https://ursa-qa.s3.amazonaws.com/single_types/type_nested.parquet",
        "format": SourceFormat.PARQUET,
    },
    "type_simple_features": {
        "path": _source("type_simple_features.parquet"),
        "source": "https://ursa-qa.s3.amazonaws.com/single_types/type_simple_features.parquet",
        "format": SourceFormat.PARQUET,
    },
    "nyctaxi_multi_parquet_s3": {
        "download": False,
        "paths": [
            "ursa-labs-taxi-data/2009/01/data.parquet",
            "ursa-labs-taxi-data/2009/02/data.parquet",
            "ursa-labs-taxi-data/2009/03/data.parquet",
            "ursa-labs-taxi-data/2009/04/data.parquet",
        ],
        "region": "us-east-2",
        "schema": nyctaxi_schema,
        "format": SourceFormat.PARQUET,
    },
    "nyctaxi_multi_ipc_s3": {
        "download": False,
        "paths": [
            "ursa-labs-taxi-data-ipc/2013/01/data.feather",
            "ursa-labs-taxi-data-ipc/2013/02/data.feather",
            "ursa-labs-taxi-data-ipc/2013/03/data.feather",
            "ursa-labs-taxi-data-ipc/2013/04/data.feather",
        ],
        "region": "us-east-2",
        "schema": nyctaxi_schema,
        "format": SourceFormat.FEATHER,
    },
    "nyctaxi_multi_parquet_s3_sample": {
        "download": False,
        "paths": [
            "ursa-labs-taxi-data-sample/2009/02/data.parquet",
            "ursa-labs-taxi-data-sample/2009/01/data.parquet",
        ],
        "region": "us-east-2",
        "schema": nyctaxi_schema,
        "format": SourceFormat.PARQUET,
    },
    "nyctaxi_multi_ipc_s3_sample": {
        "download": False,
        "paths": [
            "ursa-labs-taxi-data-sample-ipc/2009/02/data.feather",
            "ursa-labs-taxi-data-sample-ipc/2009/01/data.feather",
        ],
        "region": "us-east-2",
        "schema": nyctaxi_schema,
        "format": SourceFormat.FEATHER,
    },
    "nyctaxi_multi_parquet_s3_repartitioned": {
        "download": False,
        "paths": [
            f"ursa-labs-taxi-data-repartitioned-10k/{year}/{month:02}/{part:04}/data.parquet"
            for year in range(2009, 2020)
            for month in range(1, 13)
            for part in range(101)
            if not (year == 2019 and month > 6)  # Data ends in 2019/06
            and not (year == 2010 and month == 3)  # Data is missing in 2010/03
        ],
        "region": "us-east-2",
        "schema": nyctaxi_schema,
        "format": SourceFormat.PARQUET,
    },
}

# diana apologies for this hot mess
EXPECTED_SIZES = {
    "chi_traffic_2020_Q1.parquet": 182895135,
    "chi_traffic_sample.parquet": 116984,
    "fanniemae_2016Q4.csv.gz": 262125134,
    "fanniemae_sample.csv": 87619,
    "nyctaxi_2010-01.csv.gz": 591876633,
    "nyctaxi_sample.csv": 182665,
    "type_dict.parquet": 2890770,
    "type_floats.parquet": 23851672,
    "type_integers.parquet": 15882666,
    "type_nested.parquet": 130538033,
    "type_simple_features.parquet": 28637722,
    "type_strings.parquet": 87174822,
    "fanniemae_2016Q4.gzip.csv": 278668126,
    "fanniemae_2016Q4.lz4.feather": 817112994,
    "fanniemae_2016Q4.snappy.parquet": 153999953,
    "fanniemae_2016Q4.uncompressed.csv": 2652731759,
    "fanniemae_2016Q4.uncompressed.feather": 4686393634,
    "fanniemae_2016Q4.uncompressed.parquet": 570952947,
    "fanniemae_sample.gzip.csv": 12390,
    "fanniemae_sample.lz4.feather": 44442,
    "fanniemae_sample.snappy.parquet": 18743,
    "fanniemae_sample.uncompressed.csv": 97592,
    "fanniemae_sample.uncompressed.feather": 250938,
    "fanniemae_sample.uncompressed.parquet": 24696,
    "nyctaxi_2010-01.gzip.csv": 503844947,
    "nyctaxi_2010-01.lz4.feather": 1175111122,
    "nyctaxi_2010-01.lz4.parquet": 735273049,
    "nyctaxi_2010-01.snappy.parquet": 754527953,
    "nyctaxi_2010-01.uncompressed.csv": 2005778964,
    "nyctaxi_2010-01.uncompressed.feather": 2505803578,
    "nyctaxi_2010-01.uncompressed.parquet": 1246083270,
    "nyctaxi_2010-01.uncompressed.parquet.schema": 14386,
    "nyctaxi_sample.gzip.csv": 34506,
    "nyctaxi_sample.lz4.feather": 90738,
    "nyctaxi_sample.lz4.parquet": 76861,
    "nyctaxi_sample.snappy.parquet": 71533,
    "nyctaxi_sample.uncompressed.csv": 133440,
    "nyctaxi_sample.uncompressed.feather": 180018,
    "nyctaxi_sample.uncompressed.parquet": 103892,
}


def bytes_fmt(value):
    if value is None:
        return None
    if value == 0:
        return "0"
    if value < 1024**2:
        return "small"
    if value < 1024**3:
        return "{:.0f} Mi".format(value / 1024**2)
    if value < 1024**4:
        return "{:.1f} Gi".format(value / 1024**3)
    else:
        return "{:.1f} Ti".format(value / 1024**4)


class Source:
    """Example source store on disk:

    data
    ├── chi_traffic_sample.parquet
    ├── fanniemae_sample.csv
    ├── nyctaxi_2010-01.csv.gz
    ├── nyctaxi_sample.csv
    └── temp
        ├── fanniemae_sample.zstd.feather
        ├── nyctaxi_2010-01.snappy.parquet
        └── nyctaxi_sample.snappy.parquet
    └── ursa-labs-taxi-data-sample
        └── 2009
            ├── 01
            │   └── data.parquet
            └── 02
                └── data.parquet
    ├── type_dict.parquet
    ├── type_floats.parquet
    └── type_integers.parquet

    Files in the "data/" folder are canonical source files used for
    benchmarking.

    Files in the "data/temp/" folder are the result of running
    benchmarks, and are derived from the canonical source files.

    If a source file isn't initially found in the data folder on disk,
    it will be downloaded from the source location (like S3) and
    placed in the data folder for subsequent benchmark runs.
    """

    def __init__(self, name):
        self.name = name
        self.store = STORE[self.name]
        self._table = None
        if self.store.get("download", True):
            self.download_source_if_not_exists()

    @property
    def tags(self):
        return {"dataset": self.name}

    @property
    def paths(self):
        return self.store.get("paths", [])

    @property
    def region(self):
        return self.store.get("region")

    @property
    def csv_parse_options(self):
        return pyarrow.csv.ParseOptions(delimiter=self.store["sep"])

    @property
    def csv_read_options(self):
        if self.store["header"] is None:
            if "schema" in self.store:
                column_names = self.store["schema"].names
                autogenerate_column_names = False
            else:
                column_names = None
                autogenerate_column_names = True

            return pyarrow.csv.ReadOptions(
                autogenerate_column_names=autogenerate_column_names,
                column_names=column_names,
            )
        return None

    @property
    def csv_convert_options(self):
        return pyarrow.csv.ConvertOptions(column_types=self.store.get("schema"))

    @property
    def source_path(self):
        """A path in the benchmarks data/ folder.

        For example:
            data/nyctaxi_2010-01.csv.gz
        """
        return self.store.get("path")

    @property
    def source_paths(self):
        if self.paths:
            return [_source(path) for path in self.paths]
        elif self.source_path:
            return [self.source_path]
        else:
            return []

    @property
    def format_str(self):
        return self.store.get("format").value

    def temp_path(self, file_type, compression):
        """A path in the benchmarks data/temp/ folder.

        For example:
            data/temp/nyctaxi_sample.snappy.parquet

        If the data/temp/ folder does not exist, it will be created.
        """
        pathlib.Path(temp_dir).mkdir(exist_ok=True)
        return pathlib.Path(_temp(f"{self.name}.{compression}.{file_type}"))

    def create_if_not_exists(self, file_type, compression):
        """Used to create files for benchmarking based on the canonical
        source files found in the benchmarks data folder.

        For example:
            source = _source.Source("nyctaxi_sample")
            source.create_if_not_exists("parquet", "snappy")

        Will create the following file:
            data/temp/nyctaxi_sample.snappy.parquet

        Using the following source file:
            data/nyctaxi_sample.csv
        """
        path = self.temp_path(file_type, compression)
        if self._if_path_does_not_exist_or_not_expected_file_size(path):
            if file_type == "feather":
                self._feather_write(self.table, path, compression)
            elif file_type == "parquet":
                self._parquet_write(self.table, path, compression)
            elif file_type == "csv":
                self._csv_write(self.table, path, compression)
            self._assert_expected_file_size(path)
        return path

    @functools.cached_property
    def dataframe(self):
        # this takes ~ 7 seconds for fanniemae_2016Q4
        return self.table.to_pandas()

    @functools.cached_property
    def table(self):
        path = self.temp_path("feather", "lz4")
        if path.exists():
            # this takes ~ 3 seconds for fanniemae_2016Q4
            self._table = feather.read_table(path, memory_map=False)
        else:
            self._table = pyarrow.csv.read_csv(
                self.store["path"],
                read_options=self.csv_read_options,
                parse_options=self.csv_parse_options,
                convert_options=self.csv_convert_options,
            )
        return self._table

    def _get_object_url(self, idx=0):
        if self.paths:
            s3_url = pathlib.Path(self.paths[idx])
            return (
                "https://"
                + s3_url.parts[0]
                + ".s3."
                + self.region
                + ".amazonaws.com/"
                + os.path.join(*s3_url.parts[1:])
            )

        return self.store.get("source")

    def download_source_if_not_exists(self):
        for idx, p in enumerate(self.source_paths):
            path = pathlib.Path(p)
            if self._if_path_does_not_exist_or_not_expected_file_size(path):
                path.parent.mkdir(parents=True, exist_ok=True)
                source = self.store.get("source")
                if not source:
                    source = self._get_object_url(idx)
                r = requests.get(source)
                open(path, "wb").write(r.content)
                self._assert_expected_file_size(path)

    def _if_path_does_not_exist_or_not_expected_file_size(self, path):
        expected = bytes_fmt(EXPECTED_SIZES.get(path.name))
        return not path.exists() or bytes_fmt(os.path.getsize(path)) != expected

    def _assert_expected_file_size(self, path):
        expected = EXPECTED_SIZES.get(path.name)
        expected_formatted = bytes_fmt(expected)
        actual = os.path.getsize(path)
        actual_formatted = bytes_fmt(actual)
        debug = [
            path.name,
            expected_formatted,
            actual_formatted,
            expected,
            actual,
        ]
        skip = ["data.parquet", "data.feather"]  # TODO
        if path.name not in skip:
            assert expected_formatted == actual_formatted, debug

    def _csv_write(self, table, path, compression):
        # Note: this will write a comma separated csv with a header, even if
        # the original source file lacked a header and was pipe delimited.
        compression = munge_compression(compression, "csv")
        out_stream = pyarrow.output_stream(path, compression=compression)
        pyarrow.csv.write_csv(table, out_stream)

    def _feather_write(self, table, path, compression):
        compression = munge_compression(compression, "feather")
        feather.write_feather(table, path, compression=compression)

    def _parquet_write(self, table, path, compression):
        compression = munge_compression(compression, "parquet")
        parquet.write_table(table, path, compression=compression)
