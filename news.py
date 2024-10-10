import requests
import pandas as pd
from datetime import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# API keys
MEDIASTACK_API_KEY = ""

uri = ""


def fetch_mediastack_news():
    url = f"http://api.mediastack.com/v1/news?access_key={MEDIASTACK_API_KEY}&languages=en"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()['data']
        for item in data:
            item['source'] = 'Mediastack API'
        return data
    else:
        print(f"Failed to fetch data from Mediastack API. Status code: {response.status_code}")
        return []
    

def process_news_item(item):
    # Rename fields to a common schema
    item['published_at'] = item.pop('published', None)

    # add a timestamp for when we scraped the data
    item['scraped_at'] = datetime.now()

    return item

def save_to_mongodb(data, collection):
    try:
        result = collection.insert_many(data)
        print(f"Inserted {len(result.inserted_ids)} documents into the mongodb")
    except Exception as e:
        print(f"An error occurred while inserting into mongodb: {e}")


def main():
    # fetch news from metadata API
    mediastack_news = fetch_mediastack_news()

    if not mediastack_news:
        print("No news data fetched. Exiting.")
        return

    # combine and process the news items
    processed_news = [process_news_item(item) for item in mediastack_news]

    # connect to mongodb atlas
    try:
        client = MongoClient(uri, server_api=ServerApi('1'))
        client.admin.command('ping')
        print("Successfully connected to MongoDB!")

        # select database and collection
        db = client['news']
        collection = db['articles']

        # Save the processed news to mongodb
        save_to_mongodb(processed_news, collection)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # close the connection
        client.close()

if __name__ == "__main__":
    main()