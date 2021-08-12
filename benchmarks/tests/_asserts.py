import copy
import subprocess


R_CLI = "The R Foundation for Statistical Computing"

NYC_TAXI_TABLE = """pyarrow.Table
vendor_id: string
pickup_datetime: string
dropoff_datetime: string
passenger_count: int64
trip_distance: double
pickup_longitude: double
pickup_latitude: double
rate_code: int64
store_and_fwd_flag: double
dropoff_longitude: double
dropoff_latitude: double
payment_type: string
fare_amount: double
surcharge: double
mta_tax: double
tip_amount: double
tolls_amount: double
total_amount: double"""

FANNIEMAE_TABLE = """pyarrow.Table
0: int64
1: string
2: string
3: double
4: double
5: int64
6: int64
7: int64
8: string
9: int64
10: string
11: string
12: double
13: string
14: double
15: double
16: double
17: double
18: double
19: double
20: double
21: double
22: double
23: double
24: double
25: double
26: double
27: double
28: string
29: double
30: string"""


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
    assert_context(munged, language=language)


def assert_context(munged, language="Python"):
    context = list(munged["context"].keys())
    assert "arrow_version" in context
    assert "arrow_git_revision" not in context
    if language == "R":
        assert "arrow_version_r" in munged["context"]
    for c in context:
        if c.startswith("arrow"):
            munged["context"].pop(c)
    assert "benchmark_language" in munged["context"]
    if language == "Python":
        version = munged["context"].pop("benchmark_language_version")
        assert version.startswith("Python")
        assert munged["context"] == {"benchmark_language": "Python"}
    elif language == "R":
        version = munged["context"].pop("benchmark_language_version")
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
