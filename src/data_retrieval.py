import requests
import pandas as pd
import zipfile
import io

def retrieve_weather_data(api_key):
    url = f"http://api.climately.com/v1/{api_key}/current/all.zip"
    response = requests.get(url)

    if response.status_code == 200:
        # Assuming the response is in ZIP format, extract the CSV file
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_filename = z.namelist()[0]
            with z.open(csv_filename) as f:
                df = pd.read_csv(f)
        
        # Perform any necessary data cleaning or manipulation
        
        return df
    
    else:
        print("Error retrieving weather data:", response.status_code)
        return None
