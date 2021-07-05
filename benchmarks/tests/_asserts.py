import copy
import subprocess


R_CLI = "The R Foundation for Statistical Computing"


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
