from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date 
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import last
import numpy as np 
import pandas as pd 
import os 

def transform_finance_data ():
    input_path = 'data/raw/yahoo_finance_data.csv'
    output_path = 'data/processed/yahoo_finance_data.csv'
    spark = SparkSession.builder.appName("CleanDataYahooFinance").getOrCreate()
    df = spark.read.option("header",True).option("inferSchema",True).csv(input_path)
    #Chuyen kieu du lieu 
    df = df.withColumn("Date", to_date(col("Date")))\
            .withColumn("Open", col("Open").cast(DoubleType()))\
            .withColumn("High", col("High").cast(DoubleType()))\
            .withColumn("Low", col("Low").cast(DoubleType()))\
            .withColumn("Close", col("Close").cast(DoubleType()))\
            .withColumn("Volume", col("Volume").cast(DoubleType()))
    #Missing Value 
    df = df.dropna(subset = ["Date","Close"])
    #Duplicate Value 
    df = df.dropDuplicates(["Date","Ticker"])
    df = df.filter((col("Close") >=0) & (col("Volume")>=0))
    # fill missing value 
    df.write.mode("overwrite").csv(output_path,header=True)
    print("success")
    spark.stop()

if __name__=="__main__":
    transform_finance_data()