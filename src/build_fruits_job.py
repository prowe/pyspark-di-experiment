from .framework import run_with_local_spark, data_source, data_sink, data_calculator
from os import path
from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.getOrCreate()

@data_source
def load_colors() -> DataFrame:
    print('loading colors')
    return spark.read.csv(f'{path.dirname(__file__)}/test_data/colors.csv', header=True)


@data_source
def load_foods() -> DataFrame:
    print('loading foods')
    return spark.read.csv(f'{path.dirname(__file__)}/test_data/foods.csv', header=True)


@data_sink
def save_foods(foods: DataFrame):
    print('Saving')


@data_calculator(sink = save_foods)
def create_foods_dataset() -> DataFrame:
    foods = load_foods()
    colors = load_colors()

    return foods \
        .join(colors, on=foods.color_id == colors.id) \
        .select([
            foods.id, 
            foods.name,
            colors.id.alias('color_id'), 
            colors.name.alias('color_name')
        ])


if __name__ == '__main__':
    print('In main')
    create_foods_dataset()
