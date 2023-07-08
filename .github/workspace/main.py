from apscheduler.schedulers.blocking import BlockingScheduler
from data_retrieval import retrieve_weather_data
from data_processing import calculate_wet_bulb_temperature

# Create a scheduler
scheduler = BlockingScheduler()

# Define the job function
def retrieve_and_process_data():
    # Retrieve weather data
    weather_data = retrieve_weather_data("YOUR_API_KEY")

    # Check if data retrieval was successful
    if weather_data is not None:
        # Calculate Wet Bulb temperature
        weather_data["WetBulb"] = weather_data.apply(lambda row: calculate_wet_bulb_temperature(row["Temperature"], row["Humidity"]), axis=1)
        
        # Sort the dataframe by Wet Bulb temperature
        sorted_data = weather_data.sort_values(by="WetBulb", ascending=False)
        
        # Select top 50 values
        top_50_data = sorted_data.head(50)
        
        # Display the results
        print(top_50_data)

        #Delete the data.csv file
        os.remove("data.csv")

# Schedule the job to run every hour at a specific minute past the hour
scheduler.add_job(retrieve_and_process_data, 'cron', minute='15')

# Start the scheduler
scheduler.start()
