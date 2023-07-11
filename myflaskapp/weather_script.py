import os
import requests
import zipfile
import pandas as pd
import numpy as np
import sqlite3
from dotenv import load_dotenv
from io import BytesIO
import logging
import time
import apscheduler
import sys
#from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler

import logging
logging.basicConfig(level=logging.DEBUG, filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
logging.info('Starting script...')


# This will set up default console logging so you can see potential issues with APScheduler


# Load environment variables
load_dotenv()
API_KEY = os.getenv('CLIMATELY_API_KEY')

# Setting up logging
logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

def fetch_data():
    url = f"http://api.climately.com/v1/{API_KEY}/current/all.zip"

    try:
        response = requests.get(url, stream=True)

        # If the GET request is successful, the status code will be 200
        if response.status_code == 200:
            zipped_data = zipfile.ZipFile(BytesIO(response.content))
            data_file = zipped_data.open('current.csv')
            data_df = pd.read_csv(data_file)
            return data_df

        else:
            logging.error('Failed to fetch data. Status code: %s', response.status_code)

    except requests.exceptions.RequestException as e:
        logging.error('Failed to fetch data. Error: %s', e)

    return None

def calculate_wet_bulb(tempc, humidity):
    term_1 = tempc * np.arctan(0.151977 * (humidity + 8.313659)**0.5)
    term_2 = np.arctan(tempc + humidity)
    term_3 = np.arctan(humidity - 1.676331)
    term_4 = 0.00391838 * (humidity)**1.5 * np.arctan(0.023101 * humidity) - 4.686035
    return term_1 + term_2 - term_3 + term_4

def check_and_create_table(conn):
    cursor = conn.cursor()

    # Check if the table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='weather_data';")
    if cursor.fetchone() is None:
        # If the table does not exist, create it
        cursor.execute('''
            CREATE TABLE weather_data(
                stationid TEXT,
                tempc REAL,
                humidity REAL,
                wet_bulb_temp REAL,
                feelsc REAL
                lat REAL,
                lon REAL,
                timestamp TIMESTAMP
            )
        ''')
        conn.commit()
    else:
        # If the table exists, check if it has the lat and lon columns
        cursor.execute("PRAGMA table_info(weather_data)")
        columns = [info[1] for info in cursor.fetchall()]
        if 'lat' not in columns or 'lon' not in columns:
            # If the columns do not exist, add them
            cursor.execute("ALTER TABLE weather_data ADD COLUMN lat REAL")
            cursor.execute("ALTER TABLE weather_data ADD COLUMN lon REAL")
            conn.commit()
    
    cursor.close()


def process_data(data_df):
    try:
        # Add a new column to the dataframe for the Wet Bulb temperature
        data_df['wet_bulb_temp'] = calculate_wet_bulb(data_df['tempc'], data_df['humidity'])

        # Sort the dataframe by the Wet Bulb temperature in descending order
        data_df = data_df.sort_values(by='wet_bulb_temp', ascending=False)

        # Extract the top 50 rows
        top_50_df = data_df.head(50)

        # Load the stations data
        stations_df = pd.read_csv('stations.csv')

        # Merge the top_50_df with stations data to add latitude and longitude
        top_50_df = pd.merge(top_50_df, stations_df, on='stationid', how='left')

        return top_50_df

    except Exception as e:
        logging.error('Failed to process data. Error: %s', e)
    
    return None


def store_data(top_50_df):
    try:
        # Create a connection to the SQLite database
        conn = sqlite3.connect('./myflaskapp/weather_data.db')

        # Check and create table if necessary
        check_and_create_table(conn)

        # Write the dataframe to the SQLite database
        top_50_df.to_sql('weather_data', conn, if_exists='append', index=False)
        top_50_df['timestamp'] = pd.Timestamp.now()  # Add a timestamp column

        # Commit the transaction and close the connection
        conn.commit()
        conn.close()

    except sqlite3.Error as e:
        logging.error('Failed to store data. Error: %s', e)

def main():
    print("Starting the data fetching and processing...")

    # Fetch the data from the Climately API
    data_df = fetch_data()

    if data_df is not None:
        print("Data fetching completed. Now processing data...")
        # Process the data to calculate the Wet Bulb temperature and identify the top 50 temperatures
        top_50_df = process_data(data_df)

        if top_50_df is not None:
            print("Data processing completed. Now storing data...")
            # Store the top 50 Wet Bulb temperatures in the database
            store_data(top_50_df)
            print("Data storage completed.")
        else:
            print("Data processing failed.")
    else:
        print("Data fetching failed.")

    print("Next data fetch in 1 hour...")
    for i in range(3600, 0, -1):
        sys.stdout.write("\r")
        sys.stdout.write("{:2d} seconds remaining.".format(i))
        sys.stdout.flush()
        time.sleep(1)

    sys.stdout.write("\rComplete!            \n")

if __name__ == "__main__":
    main()
