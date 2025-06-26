from pyspark.sql import SparkSession

def load_finance_data():
        spark = SparkSession.builder.appName("Load_Finance_Data")\
                    .config("spark.jars","file:///D:/finance_etl/jars/postgresql-42.2.27.jar")\
                    .getOrCreate()
        df = spark.read.option("header",True).csv("data/processed/yahoo_finance_data_final.csv")
        df.write.format("jdbc").option("url","jdbc:postgresql://localhost:5432/airflow")\
                .option("dbtable","finance")\
                .option("user","airflow")\
                .option("password","airflow")\
                .option("driver", "org.postgresql.Driver")\
                .mode("overwrite").save()
        spark.stop()
if __name__ == "__main__":
    load_finance_data()