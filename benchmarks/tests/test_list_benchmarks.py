import json
import os

from .. import _benchmark, _example_benchmarks
from ..tests import _asserts


this_dir = os.path.abspath(os.path.dirname(__file__))
benchmarks_file = os.path.join(this_dir, "..", "..", "benchmarks.json")


def test_list():
    simple = _example_benchmarks.SimpleBenchmark
    external = _example_benchmarks.ExternalBenchmark
    r_only = _example_benchmarks.WithoutPythonBenchmark
    cases = _example_benchmarks.CasesBenchmark
    classes = {
        "simple": simple,
        "external": external,
        "cases": cases,
        "R-only": r_only,
        "example": cases,  # should be skipped
    }
    actual = _benchmark.BenchmarkList().list(classes)
    expected = [
        {
            "command": "R-only --iterations=3 --drop-caches=true",
            "flags": {"language": "R"},
        },
        {
            "command": "cases --iterations=3 --all=true --drop-caches=true",
            "flags": {"language": "Python"},
        },
        {
            "command": "external --iterations=3 --drop-caches=true",
            "flags": {"language": "Python"},
        },
        {
            "command": "simple --iterations=3 --drop-caches=true",
            "flags": {"language": "Python"},
        },
    ]
    assert actual == expected


def test_list_cli():
    command = ["conbench", "list"]

    with open(benchmarks_file) as f:
        benchmarks = json.dumps(json.load(f), indent=2)

    try:
        _asserts.assert_cli(command, benchmarks)
    except AssertionError:
        output = _asserts.get_cli_output(command)
        with open(benchmarks_file, "w") as f:
            f.write(output)
            f.write("\n")
            msg = (
                "Warning benchmarks.json was re-generated. "
                "Confirm the diff and commit it. "
                "This test should pass if you now run it again. "
            )
        assert False, msg
