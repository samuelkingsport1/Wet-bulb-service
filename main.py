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

from apscheduler.schedulers.background import BackgroundScheduler

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

def calculate_wet_bulb(temp, rh):
    term_1 = temp * np.arctan(0.151977 * (rh + 8.313659)**0.5)
    term_2 = np.arctan(temp + rh)
    term_3 = np.arctan(rh - 1.676331)
    term_4 = 0.00391838 * (rh)**1.5 * np.arctan(0.023101 * rh) - 4.686035
    return term_1 + term_2 - term_3 + term_4

def process_data(data_df):
    try:
        # Add a new column to the dataframe for the Wet Bulb temperature
        data_df['wet_bulb_temp'] = calculate_wet_bulb(data_df['temp'], data_df['rh'])

        # Sort the dataframe by the Wet Bulb temperature in descending order
        data_df = data_df.sort_values(by='wet_bulb_temp', ascending=False)

        # Extract the top 50 rows
        top_50_df = data_df.head(50)

        return top_50_df

    except Exception as e:
        logging.error('Failed to process data. Error: %s', e)
    
    return None

def store_data(top_50_df):
    try:
        # Create a connection to the SQLite database
        conn = sqlite3.connect('weather_data.db')

        # Write the dataframe to the SQLite database
        top_50_df.to_sql('weather_data', conn, if_exists='append', index=False)

        # Commit the transaction and close the connection
        conn.commit()
        conn.close()

    except sqlite3.Error as e:
        logging.error('Failed to store data. Error: %s', e)

def main():
    # Fetch the data from the Climately API
    data_df = fetch_data()

    if data_df is not None:
        # Process the data to calculate the Wet Bulb temperature and identify the top 50 temperatures
        top_50_df = process_data(data_df)

        if top_50_df is not None:
            # Store the top 50 Wet Bulb temperatures in the database
            store_data(top_50_df)

if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.add_job(main, 'interval', hours=1)
    scheduler.start()
    
    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()