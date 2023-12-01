import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

MONGO_URL = os.getenv("MONGO_URL")

try:
    # Connect to MongoDB
    client = MongoClient(MONGO_URL)
    db = client["BDT"]
    collection = db["carbonEmission"]

    # Read CSV file into a pandas DataFrame
    df = pd.read_csv("./backend/old/carbonEmission.csv")

    # Convert DataFrame to a list of dictionaries (one dictionary per row)
    data_list = []
    for _, row in df.iterrows():
        country_data = {
            "country": row["Countries"],
            "data": [
                {"year": int(col), "emission": row[col]}
                for col in df.columns[1:]
                if col.isdigit()
            ],
        }
        data_list.append(country_data)

    # Insert data into MongoDB collection
    collection.insert_many(data_list)

    print("Data submitted successfully!")

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close the MongoDB connection
    if client:
        client.close()
