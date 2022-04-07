from pytest import fixture
from pyspark.sql import Row, SparkSession
from unittest.mock import patch
from . import build_fruits_job

@fixture
def spark():
    return SparkSession.builder.getOrCreate()


@patch(f'{build_fruits_job.__name__}.save_foods')
@patch(f'{build_fruits_job.__name__}.load_colors')
@patch(f'{build_fruits_job.__name__}.load_foods')
def test_foods_are_calculated(load_foods, load_colors, save_foods, spark):
    load_foods.return_value = spark.createDataFrame([
        Row(id=1, name='Apple', color_id=3),
    ])
    load_colors.return_value = spark.createDataFrame([
        Row(id=3, name='Red'),
    ])

    results = build_fruits_job.create_foods_dataset()
    results.show()