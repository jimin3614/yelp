
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
import requests 
import mysql.connector
import os 
from dotenv import load_dotenv
import json
import pandas as pd
from sqlalchemy import create_engine


spark = SparkSession.builder.appName("yelp_spark_etl_app").getOrCreate()

def extract_transform(offset):
    load_dotenv() 

    url = "https://api.yelp.com/v3/businesses/search?"

    param = {
        "sort_by": "best_match",
        "limit": 50, 
        "offset": offset,
        "location": "New York City",
        "term": "restaurants" 
    }
    headers = {
        "accept": "application/json",
        "Authorization": os.getenv("AUTHORIZATION")
    }
   
    response = requests.get(url, params = param, headers=headers)

    if response.status_code == 200:
        data = response.json()
        data = data["businesses"]
        

        # read JSON data to Spark dataframe
        # json.dumps converts JSON data to string type
        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(record) for record in data]))

        # select only necessary fields
        df = df.select("name", "price", "rating", "review_count", "categories", "coordinates")
        
        # flatten ARRAY (List) or MAP using explode() function
        exploded_df = df.withColumn("categories", explode("categories").alias("cateogry"))

        # change column name e.g. categories.alias -> alias, categories.title -> title
        exploded_df = exploded_df.withColumn("alias", col("categories.alias")).withColumn("title", col("categories.title"))
        exploded_df = exploded_df.withColumn("latitude", col("coordinates.latitude")).withColumn("longitude", col("coordinates.longitude"))

        # drop a column
        exploded_df = exploded_df.drop("categories")
        exploded_df = exploded_df.drop("coordinates")

        # display dataframe data 
        exploded_df.show()

        # display schema
        exploded_df.printSchema()

        return exploded_df
        
    else:
        print("Error: ", response.status_code)
    
        return


#Loading the data to MySQL
def load(transformed_df):
    load_dotenv()
    pandas_df = transformed_df.toPandas()

    # MySQL connection properties
    # mysql_url = "mysql+pymysql://root:jimin06251992@localhost:3306/jimin_yelp"

    user = os.getenv("USER")
    password = os.getenv("PASSWORD")
    host = os.getenv("HOST")
    database = os.getenv("DATABASE")
    port_number = os.getenv("PORT_NUMBER")
    mysql_url = f"mysql+pymysql://{user}:{password}@{host}:{port_number}/{database}"

    # Create SQLAlchemy engine
    engine = create_engine(mysql_url)

    # Insert pandas DataFrame into MySQL
    pandas_df.to_sql(name=os.getenv("TABLE_NAME"), con=engine, if_exists='append', index=False)



if __name__ == "__main__":
    offset = 1

    for i in range(20): #offset limit to 1000 
        if i == 0:
            data = extract_transform(offset)
        else:
            offset +=50
            data = extract_transform(offset)

        transformed_df = extract_transform(offset)
        
        load(transformed_df)

