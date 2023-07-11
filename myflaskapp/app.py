from flask import Flask, jsonify, render_template
import sqlite3
from flask_cors import CORS


app = Flask(__name__)
CORS(app)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/weather_data', methods=['GET'])
def get_weather_data():
    # Create a connection to the SQLite database
    conn = sqlite3.connect('weather_data.db')

    # Create a cursor object
    cur = conn.cursor()

    # Execute the query
    cur.execute("SELECT stationid, wet_bulb_temp, lat, lon FROM weather_data")

    # Fetch all rows from the last executed query
    rows = cur.fetchall()

    # Close the connection
    conn.close()

    # Prepare data for JSON response
    data = [{"stationid": row[0], "wet_bulb_temp": row[1], "lat": row[2], "lon": row[3]} for row in rows]

    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)
