from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date 
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import last, lag, log,avg
import numpy as np 
import pandas as pd 
import os 

def create_spark():
    return SparkSession.builder.appName("Transform_Finance_Data").getOrCreate()

def read_data(spark, path):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)

def clean_finance_data (df):
    #cast datatye 
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
    print("success")
    return df

def calculate_log_return (df):
    window_spec = Window.partitionBy("Ticker").orderBy("Date")
    df = df.withColumn("prev_close",lag("Close").over(window_spec))
    df = df.withColumn("log_return",log(col("Close")/col("prev_close")))
    return df

def calculate_moving_average (df,window_size = 7):
    window_spec = Window.partitionBy("Ticker").orderBy("Date").rowsBetween(-(window_size -1),0)
    df = df.withColumn(f"MA", avg("Close").over(window_spec))
    return df 

def calculate_rate_of_change(df, n = 5):
    window_spec = Window.partitionBy("Ticker").orderBy("Date")
    prev_close = lag("Close",n).over(window_spec)
    df = df.withColumn(f"ROC",(((col("Close") -prev_close)/prev_close))*100)
    return df

def transform_finance_data():
    input_path = 'data/raw/yahoo_finance_data.csv'
    output_path = 'data/processed/yahoo_finance_data.csv'
    spark = create_spark()
    df = read_data(spark,input_path)
    df = clean_finance_data(df)
    df = calculate_log_return(df)
    df = calculate_moving_average(df)
    df = calculate_rate_of_change(df,n=5)   
    df.write.mode("overwrite").csv(output_path,header=True)
    spark.stop()

if __name__=="__main__":
    transform_finance_data()