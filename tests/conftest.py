import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield s
    s.stop()
