from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pymongo import MongoClient

app = Flask(__name__)

# Set up Spark configuration
conf = SparkConf().setAppName("MongoDBConnector").setMaster("local")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Set MongoDB connection properties
mongo_uri = ""
mongo_client = MongoClient(mongo_uri)

# Access the database and specify the collection
database_name = "test"  # Replace with your actual database name
collection_name = "awards"  # Replace with your actual collection name
database = mongo_client[database_name]
collection = database[collection_name]


@app.route("/add_data", methods=["POST"])
def add_data():
    try:
        # Get JSON data from the request
        json_data = request.get_json()

        # Convert JSON data to a Spark DataFrame
        data_df = spark.createDataFrame([json_data])
        print(data_df)

        # # Write data to MongoDB collection
        # Write data to MongoDB collection
        data_df.write.format("mongo").mode("append").option(
            "uri", mongo_uri + database_name + "." + collection_name
        ).save()

        return jsonify({"status": "success", "message": "Data added successfully"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


if __name__ == "__main__":
    app.run()
