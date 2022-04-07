from framework import run_with_local_spark, data_source, data_sink, data_calculator
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

@data_source
def load_colors(): 
    return 'color'


@data_source
def load_foods():
    return 'food'


@data_sink
def save_foods(foods):
    print('Saving', foods)
    pass


@data_calculator(sink = save_foods)
def create_foods_dataset():
    foods = load_foods()
    colors = load_colors()
    return foods + colors


if __name__ == '__main__':
    print('In main')
    create_foods_dataset()
