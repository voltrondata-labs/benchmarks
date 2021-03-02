import copy
import os
import subprocess


this_dir = os.path.dirname(os.path.abspath(__file__))
benchmarks_dir = os.path.join(this_dir, "..")


def assert_benchmark(result, source, name):
    munged = copy.deepcopy(result)
    assert munged["tags"] == {
        "name": name,
        "dataset": source,
        "cpu_count": None,
        "gc_collect": True,
        "gc_disable": True,
    }
    assert_context(munged)


def assert_context(munged, language="Python"):
    context = list(munged["context"].keys())
    assert "arrow_version" in context
    for c in context:
        if c.startswith("arrow"):
            munged["context"].pop(c)
    assert "benchmark_language" in munged["context"]
    if language == "Python":
        version = munged["context"].pop("benchmark_language_version")
        assert version.startswith("Python")
        assert munged["context"] == {"benchmark_language": "Python"}


def assert_cli(command, expected):
    os.chdir(benchmarks_dir)
    result = subprocess.run(command, capture_output=True, check=True)
    actual = result.stdout.decode("utf-8").strip().replace("\x08", "")
    assert actual == expected.strip(), actual
