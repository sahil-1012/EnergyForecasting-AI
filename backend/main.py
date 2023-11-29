from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pymongo import MongoClient
from flask import Flask, jsonify, request
from flask_cors import CORS  # Import CORS from flask_cors
import json

from dotenv import load_dotenv
import os

load_dotenv()
MONGO_URL = os.getenv("MONGO_URL")

# # Set up Spark configuration
conf = SparkConf().setAppName("MongoDBConnector").setMaster("local")
conf.set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
spark = SparkSession.builder.config(conf=conf).getOrCreate()


# # Set MongoDB connection properties
mongo_client = MongoClient(MONGO_URL)


# # Create a Flask web server
app = Flask(__name__)
CORS(app)

# # Define a route to serve the data as JSON
# @app.route("/api/getTotalConsumption", methods=["GET"])
# def get_data():
#     database_name = "BDT"
#     collection_name = "totalConsumption"
#     print(MONGO_URL + "/" + database_name + "." + collection_name)

#     try:
#         # # Read data from MongoDB collection into a PySpark DataFrame
#         data_df = (
#             spark.read.format("com.mongodb.spark.sql.DefaultSource")
#             .option("uri", MONGO_URL + "/" + database_name + "." + collection_name)
#             .load()
#         )

#         # # Convert PySpark DataFrame to JSON
#         json_data = data_df.toJSON().collect()
#         data_list = [json.loads(record) for record in json_data]

#         return jsonify(data_list)

#     except Exception as e:
#         error_message = f"An unexpected error occurred: {e}"
#         return (
#             jsonify({"error": error_message}),
#             500,
#         )


from pyspark.sql.functions import explode, col

@app.route("/api/getTotalConsumption", methods=["GET"])
def get_data():
    database_name = "BDT"
    collection_name = "totalConsumption"
    year = request.args.get('year', default=None, type=int)
    
    try:
        # Read data from MongoDB collection into a PySpark DataFrame
        data_df = (
            spark.read.format("com.mongodb.spark.sql.DefaultSource")
            .option("uri", MONGO_URL + "/" + database_name + "." + collection_name)
            .load()
        )

        # Drop the "_id" column
        data_df = data_df.drop("_id")

        # Explode the "data" array to create a new row for each element in the array
        exploded_df = data_df.select("country", explode("data").alias("data"))

        # Filter data for the year 2023
        filtered_data_df = exploded_df.filter(col("data.year") == year)

        # Select relevant columns and alias them
        result_df = filtered_data_df.selectExpr("country", "data.consumption as value")

         # Sort the data by consumption in descending order
        sorted_data_df = result_df.orderBy(col("value").desc())

        # Limit the data to the top 10 countries
        top_10_data_df = sorted_data_df.limit(10)

        # Convert PySpark DataFrame to JSON
        json_data = top_10_data_df.toJSON().collect()
        data_list = [json.loads(record) for record in json_data]

        return jsonify(data_list)

    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        return jsonify({"error": error_message}), 500


@app.route("/add_data", methods=["POST"])
def add_entry():
    database_name = "temp"
    collection_name = "tump"
    collection = mongo_client[database_name][collection_name]

    try:
        # # Get data from the request
        data = request.get_json()

        # # Convert the data to a PySpark DataFrame
        schema = (
            spark.read.format("com.mongodb.spark.sql.DefaultSource")
            .option("uri", MONGO_URL + database_name + "." + collection_name)
            .load()
            .schema
        )
        data_df = spark.createDataFrame([data], schema=schema)

        # # Append the DataFrame to the MongoDB collection
        data_df.write.format("com.mongodb.spark.sql.DefaultSource").mode(
            "append"
        ).option("uri", MONGO_URL + database_name + "." + collection_name).save()

        return jsonify({"message": "Entry added successfully"}), 201

    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        return jsonify({"error": error_message}), 500


# # Run the Flask application
if __name__ == "__main__":
    app.run(port=4000, debug=True)


# # Stop Spark session
spark.stop()
