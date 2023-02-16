#!/usr/bin/env python3
# -*_ encoding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class parquet_data:


    def __init__(self):
         self._spark = SparkSession.builder.appName("get_csv_developer_parquet").getOrCreate()
         self._csv_path_diesel = r"/home/ramon/projeto_raizen/data/csv_data/diesel.csv"
         self._csv_path_derivative = r"/home/ramon/projeto_raizen/data/csv_data/derivative.csv"
         self._parquet_path_diesel = r"/home/ramon/projeto_raizen/data/parquet_data/diesel.parquet"
         self._data_csv = None
         self.__repr__()


    def __repr__(self):
        return repr("This is class: %s" % (__class__.__name__))


    def read_csv(self):
        self._data_csv = self._spark.read.option("delimiter", ",")\
              .option("header", "False")\
              .csv(self._csv_path_diesel)
        return self._data_csv.show(5, truncate=False)
                                                                                                                           

    def write_parquet(self):
        self._data_csv.write.parquet(self._parquet_path_diesel)
        return "saved parquet file"


if (__name__ != "__main__"):
    pass
else:
    objeto = parquet_data()
    objeto.read_csv()
    objeto.write_parquet()
