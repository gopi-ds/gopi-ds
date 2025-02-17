import argparse
import pymongo
from bson import ObjectId
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime

def get_mongo_client(uri):
    """Establish connection to MongoDB."""
    return pymongo.MongoClient(uri)

def fetch_document_counts(db, collection, start_date, end_date):
    """
    Retrieves document counts grouped by month based on _id timestamp.

    :param db: MongoDB database name
    :param collection: MongoDB collection name
    :param start_date: Start date (YYYY-MM)
    :param end_date: End date (YYYY-MM)
    :return: DataFrame containing document counts per month
    """
    pipeline = [
        {
            "$match": {
                "_id": {
                    "$gte": ObjectId.from_datetime(start_date),
                    "$lte": ObjectId.from_datetime(end_date)
                }
            }
        },
        {
            "$project": {
                "yearMonth": {
                    "$dateToString": {"format": "%Y-%m", "date": {"$toDate": "$_id"}}
                }
            }
        },
        {
            "$group": {
                "_id": "$yearMonth",
                "doc_count": {"$sum": 1}
            }
        },
        {"$sort": {"_id": 1}}
    ]

    result = list(db[collection].aggregate(pipeline))
    return pd.DataFrame(result)

def plot_growth(df, collection):
    """
    Plots the document count growth over time.

    :param df: DataFrame containing the aggregated document counts
    :param collection: Collection name for labeling
    """
    if df.empty:
        print("No data found for the given period.")
        return

    df["_id"] = pd.to_datetime(df["_id"], format="%Y-%m")
    df = df.sort_values("_id")

    plt.figure(figsize=(12, 6))
    plt.plot(df["_id"], df["doc_count"], marker="o", linestyle="-", label=f"{collection} Growth")

    plt.xlabel("Date")
    plt.ylabel("Document Count")
    plt.title(f"MongoDB Document Growth Over Time ({collection})")
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid()

    # Show the graph
    plt.show()

def main():
    parser = argparse.ArgumentParser(description="MongoDB Growth Analysis")
    parser.add_argument("--uri", required=True, help="MongoDB connection URI")
    parser.add_argument("--db", required=True, help="Database name")
    parser.add_argument("--collection", required=True, help="Collection name")
    parser.add_argument("--start_date", required=True, help="Start date (YYYY-MM)")
    parser.add_argument("--end_date", required=True, help="End date (YYYY-MM)")

    args = parser.parse_args()

    client = get_mongo_client(args.uri)
    db = client[args.db]

    start_date = datetime.strptime(args.start_date, "%Y-%m")
    end_date = datetime.strptime(args.end_date, "%Y-%m")

    df = fetch_document_counts(db, args.collection, start_date, end_date)
    plot_growth(df, args.collection)

if __name__ == "__main__":
    main()
