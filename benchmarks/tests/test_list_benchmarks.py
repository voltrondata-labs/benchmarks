from ..tests._asserts import assert_cli


LIST = """
[
  {
    "command": "cpp-micro --iterations=1",
    "flags": {
      "language": "C++"
    }
  },
  {
    "command": "csv-read ALL --iterations=3",
    "flags": {
      "language": "Python"
    }
  },
  {
    "command": "dataframe-to-table ALL --iterations=3",
    "flags": {
      "language": "Python"
    }
  },
  {
    "command": "dataset-filter ALL --iterations=3",
    "flags": {
      "language": "Python"
    }
  },
  {
    "command": "dataset-read ALL --iterations=1 --all=true",
    "flags": {
      "cloud": true,
      "language": "Python"
    }
  },
  {
    "command": "file-read ALL --iterations=3 --all=true",
    "flags": {
      "language": "Python"
    }
  },
  {
    "command": "file-read ALL --iterations=3 --all=true --language=R",
    "flags": {
      "language": "R"
    }
  },
  {
    "command": "file-write ALL --iterations=3 --all=true",
    "flags": {
      "language": "Python"
    }
  },
  {
    "command": "file-write ALL --iterations=3 --all=true --language=R",
    "flags": {
      "language": "R"
    }
  },
  {
    "command": "wide-dataframe --iterations=3 --all=true",
    "flags": {
      "language": "Python"
    }
  }
]
"""


def test_list_cli():
    command = ["conbench", "list"]
    assert_cli(command, LIST)
