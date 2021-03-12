from ..tests._asserts import assert_cli


LIST = """
[
  {
    "command": "cpp-micro --iterations=1",
    "flags": {}
  },
  {
    "command": "csv-read ALL --iterations=3",
    "flags": {}
  },
  {
    "command": "dataframe-to-table ALL --iterations=3",
    "flags": {}
  },
  {
    "command": "dataset-filter ALL --iterations=3",
    "flags": {}
  },
  {
    "command": "dataset-read ALL --all=true --iterations=1",
    "flags": {
      "cloud": true
    }
  },
  {
    "command": "file-read ALL --all=true --iterations=3",
    "flags": {}
  },
  {
    "command": "file-read ALL --all=true --iterations=3 --language=R",
    "flags": {}
  },
  {
    "command": "file-write ALL --all=true --iterations=3",
    "flags": {}
  },
  {
    "command": "file-write ALL --all=true --iterations=3 --language=R",
    "flags": {}
  },
  {
    "command": "wide-dataframe --all=true --iterations=3",
    "flags": {}
  }
]
"""


def test_list_cli():
    command = ["conbench", "list"]
    assert_cli(command, LIST)
