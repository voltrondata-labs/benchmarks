import copy
import subprocess

R_CLI = "The R Foundation for Statistical Computing"

NYCTAXI_TABLE = """pyarrow.Table
vendor_id: string
pickup_datetime: timestamp[ns]
dropoff_datetime: timestamp[ns]
passenger_count: int64
trip_distance: double
pickup_longitude: double
pickup_latitude: double
rate_code: int64
store_and_fwd_flag: string
dropoff_longitude: double
dropoff_latitude: double
payment_type: string
fare_amount: double
surcharge: double
mta_tax: double
tip_amount: double
tolls_amount: double
total_amount: double"""

NYCTAXI_TABLE_TIMESTAMP = """pyarrow.Table
vendor_id: string
pickup_at: timestamp[us]
dropoff_at: timestamp[us]
passenger_count: int8
trip_distance: float
pickup_longitude: float
pickup_latitude: float
rate_code_id: null
store_and_fwd_flag: string
dropoff_longitude: float
dropoff_latitude: float
payment_type: string
fare_amount: float
extra: float
mta_tax: float
tip_amount: float
tolls_amount: float
total_amount: float"""


# TODO: why null vs string?
_TEMP = (
    NYCTAXI_TABLE_TIMESTAMP
    + """
year: dictionary<values=int32, indices=int32, ordered=0>
month: dictionary<values=int32, indices=int32, ordered=0>
part: dictionary<values=int32, indices=int32, ordered=0>"""
)
NYCTAXI_TABLE_SELECT = _TEMP.replace(
    "rate_code_id: null",
    "rate_code_id: string",
)

FANNIEMAE_TABLE = """pyarrow.Table
LOAN_ID: string
ACT_PERIOD: string
SERVICER: string
ORIG_RATE: double
CURRENT_UPB: double
LOAN_AGE: int32
REM_MONTHS: int32
ADJ_REM_MONTHS: int32
MATR_DT: string
MSA: string
DLQ_STATUS: string
RELOCATION_MORTGAGE_INDICATOR: string
Zero_Bal_Code: string
ZB_DTE: string
LAST_PAID_INSTALLMENT_DATE: string
FORECLOSURE_DATE: string
DISPOSITION_DATE: string
FORECLOSURE_COSTS: double
PROPERTY_PRESERVATION_AND_REPAIR_COSTS: double
ASSET_RECOVERY_COSTS: double
MISCELLANEOUS_HOLDING_EXPENSES_AND_CREDITS: double
ASSOCIATED_TAXES_FOR_HOLDING_PROPERTY: double
NET_SALES_PROCEEDS: double
CREDIT_ENHANCEMENT_PROCEEDS: double
REPURCHASES_MAKE_WHOLE_PROCEEDS: double
OTHER_FORECLOSURE_PROCEEDS: double
NON_INTEREST_BEARING_UPB: double
MI_CANCEL_FLAG: string
RE_PROCS_FLAG: string
LOAN_HOLDBACK_INDICATOR: string
SERV_IND: string"""


def assert_table_output(source, output, nyc_ts=False, nyc_select=False):
    if source.startswith("nyctaxi"):
        if nyc_ts:
            text = NYCTAXI_TABLE_TIMESTAMP
        elif nyc_select:
            text = NYCTAXI_TABLE_SELECT
        else:
            text = NYCTAXI_TABLE
    else:
        text = FANNIEMAE_TABLE

    out = str(output)

    assert text in out


def assert_dimensions_output(source, output):
    out = str(output)
    if source.startswith("nyctaxi"):
        assert "[998 rows x 18 columns]" in out
    else:
        assert "[100 rows x 31 columns]" in out


def assert_benchmark(result, source, name, language="Python"):
    munged = copy.deepcopy(result)
    expected = {
        "name": name,
        "dataset": source,
        "cpu_count": None,
    }
    if language == "R":
        expected["language"] = "R"
    assert munged["tags"] == expected
    assert_info_and_context(munged, language=language)


def assert_info_and_context(munged, language="Python"):
    assert "name" in munged["tags"]
    assert "cpu_count" in munged["tags"]
    assert list(munged["context"].keys()) == [
        "benchmark_language",
    ]
    if language == "Python":
        assert list(munged["info"].keys()) == [
            "arrow_version",
            "arrow_compiler_id",
            "arrow_compiler_version",
            "arrow_compiler_flags",
            "benchmark_language_version",
        ]
    elif language == "R":
        assert list(munged["info"].keys()) == [
            "arrow_version",
            "arrow_compiler_id",
            "arrow_compiler_version",
            "arrow_compiler_flags",
            "benchmark_language_version",
            "arrow_version_r",
        ]
    else:
        assert list(munged["info"].keys()) == [
            "arrow_version",
            "arrow_compiler_id",
            "arrow_compiler_version",
            "arrow_compiler_flags",
        ]
    del munged["info"]["arrow_compiler_flags"]
    if language == "Python":
        version = munged["info"].pop("benchmark_language_version")
        assert version.startswith("Python")
        assert munged["context"] == {"benchmark_language": "Python"}
    elif language == "R":
        version = munged["info"].pop("benchmark_language_version")
        assert version.startswith("R version")
        assert munged["context"] == {"benchmark_language": "R"}
    elif language == "C++":
        assert munged["context"] == {"benchmark_language": "C++"}
    elif language == "Java":
        assert munged["context"] == {"benchmark_language": "Java"}
    elif language == "JavaScript":
        assert munged["context"] == {"benchmark_language": "JavaScript"}


def get_cli_output(command):
    result = subprocess.run(command, capture_output=True, check=True)
    return result.stdout.decode("utf-8").strip().replace("\x08", "")


def assert_cli(command, expected):
    actual = get_cli_output(command)
    assert actual == expected.strip()
