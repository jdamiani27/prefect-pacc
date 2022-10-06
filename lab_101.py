from typing import Any

import requests
from prefect import flow, task
from requests import Response


@task
def fetch_weather(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast"
    weather = requests.get(
        base_url, params={"latitude": lat, "longitude": lon, "hourly": "temperature_2m"}
    )
    return weather


@task
def display_weather(weather: Response):
    print(weather.json())


@flow
def weather_pipeline():
    weather = fetch_weather.map(
        lat=[35.86, 35.87905971398759, 40.737482434149456],
        lon=[-78.88, -78.92572154637823, -73.99771873383233],
    )
    display_weather.map(weather)


if __name__ == "__main__":
    weather_pipeline()
