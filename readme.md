# Weather Data Importer

Fetch and store historical weather data, specifically temperatures at 2 PM local time for each day in 1980 at specified coordinates, into a MySQL database.

## Setup Instructions

### Prerequisites
- Python 3.x
- MySQL

### Installation

1. Clone the repository:
   - Run: `git clone https://github.com/yourusername/weather-data-importer.git`
   - Then cd into the directory: `cd weather-data-importer`

2. Set up and activate a virtual environment:
- Windows: `python -m venv venv && venv\Scripts\activate`
- Unix/MacOS: `python3 -m venv venv && source venv/bin/activate`

3. Install dependencies:
`pip install openmeteo_requests requests_cache retry_requests mysql-connector-python timezonefinder pytz pandas`

4. Database Setup:
   - Create a MySQL database and a table for the temperature data:
```
CREATE DATABASE weather_data;
USE weather_data;
CREATE TABLE temperatures (
  id INT AUTO_INCREMENT PRIMARY KEY,
  date DATE NOT NULL,
  temperature DECIMAL(5, 2) NOT NULL
);
```

5. Update Configuration:
   - Adjust the script with your MySQL database credentials, coordinate settings, and date range as needed.

6. Run the Script:
`python3 fetch_store_weather.py`
