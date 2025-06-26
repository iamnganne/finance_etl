from pyspark.sql import SparkSession
import findspark

def load_finance_data():
    spark = SparkSession.builder.appName("Load_Finance_Data").getOrCreate()
    df = spark.read.option("header",True).csv("data/processed/data/processed/yahoo_finance_data_final.csv")
    df.write.format("jdbc").option("url","jdbc:postgresql://localhost:5432/mydatabase")\
            .option("dbtable","mytable")\
            .option("user","airflow")\
            .option("password","airflow")\
            .mode("overwrite").save()
    spark.stop()

if __name__ == "__main__":
    load_to_postgre()
