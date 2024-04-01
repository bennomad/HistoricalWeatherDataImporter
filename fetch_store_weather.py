import openmeteo_requests
import requests_cache
from retry_requests import retry
import mysql.connector
from mysql.connector import Error
from timezonefinder import TimezoneFinder
import pytz
from pandas import date_range, to_datetime, Timedelta


# Database connection parameters
db_config = {
    'host': 'localhost',
    'user': 'admin',
    'password': 'password',
    'database': 'weather_data'
}

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

def fetch_weather_data():
    # Open-Meteo API parameters for 1980 at specific coordinates
    params = {
        "latitude": 48.1372,
        "longitude": 11.5755,
        "start_date": "1980-01-01",
        "end_date": "1980-12-31",
        "hourly": "temperature_2m",
    }
    responses = openmeteo.weather_api("https://archive-api.open-meteo.com/v1/archive", params=params)

    # Process first location's response
    response = responses[0]
    hourly = response.Hourly()
    temperatures = hourly.Variables(0).ValuesAsNumpy()

    # Use timezonefinder to find the timezone for the provided coordinates
    tf = TimezoneFinder()
    timezone_str = tf.timezone_at(lat=params['latitude'], lng=params['longitude'])

    # Use pytz to work with the found timezone
    timezone = pytz.timezone(timezone_str)
    # Generate a date range for the hourly data
    times = date_range(
        start=to_datetime(hourly.Time(), unit="s", utc=True),
        end=to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )
    # Adjust times from UTC to the local timezone, accounting for DST
    times_local = times.tz_convert(timezone_str)
    # Filter for noon temperatures
    noon_temperatures = []
    for time, temp in zip(times_local, temperatures):
        if time.hour == 14:  # Looking for 2 PM in local time
            # Convert time back to naive datetime in the local timezone for storage
            local_naive_time = time.tz_localize(None)
            noon_temperatures.append((local_naive_time, temp))
    return noon_temperatures

def ensure_date_is_unique_key(cursor):
    # Check if 'date' column is a UNIQUE KEY
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_catalog = kcu.constraint_catalog
        AND tc.constraint_schema = kcu.constraint_schema
        AND tc.constraint_name = kcu.constraint_name
        WHERE tc.constraint_type = 'UNIQUE'
        AND tc.table_schema = DATABASE()  # Use the current database
        AND tc.table_name = 'temperatures'
        AND kcu.column_name = 'date';
    """)
    result = cursor.fetchone()
    is_unique = result[0] > 0

    # If 'date' column is not a UNIQUE KEY, add it
    if not is_unique:
        cursor.execute("""
            ALTER TABLE temperatures
            ADD UNIQUE(date);
        """)
        print("Added UNIQUE constraint to 'date' column to avoid adding identical data to the database.")

def store_weather_data(noon_temperatures):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        ensure_date_is_unique_key(cursor)
        for time, api_temperature in noon_temperatures:
            db_temperature = float(api_temperature)
            # Using ON DUPLICATE KEY UPDATE to update temperature if the date already exists
            query = """
                       INSERT INTO temperatures (date, temperature) 
                       VALUES (%s, %s) 
                       ON DUPLICATE KEY UPDATE temperature = VALUES(temperature)
                   """
            cursor.execute(query, (time.date(), db_temperature))
        connection.commit()
        print(f"Successfully stored noon temperatures.")
    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def main():
    noon_temperatures = fetch_weather_data()
    store_weather_data(noon_temperatures)

if __name__ == "__main__":
    main()