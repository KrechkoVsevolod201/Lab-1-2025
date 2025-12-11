import requests
import json
from datetime import datetime, timedelta
from prefect import task
from minio import Minio
import io
from prefect.tasks import exponential_backoff

@task(
    retries=4,
    retry_delay_seconds=exponential_backoff(backoff_factor=2),
    retry_jitter_factor=1
)
def fetch_weather_forecast_with_backoff(city: str, lat: float, lon: float) -> dict:

    # Извлекает прогноз погоды на завтра для указанного города
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ["temperature_2m", "precipitation", "wind_speed_10m", "wind_direction_10m"],
        "start_date": tomorrow,
        "end_date": tomorrow,
        "timezone": "auto"
    }
    
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    data['city'] = city
    data['fetched_at'] = datetime.now().isoformat()
    
    return data

@task(retries=2, retry_delay_seconds=5)
def save_to_minio(data: dict, city: str) -> str:
    """
    Сохраняет сырой JSON-ответ в MinIO
    """
    client = Minio(
        "localhost:9002",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    
    bucket_name = "weather-data"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    object_name = f"raw/{city}_{timestamp}.json"
    
    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
    json_stream = io.BytesIO(json_bytes)
    
    client.put_object(
        bucket_name,
        object_name,
        json_stream,
        length=len(json_bytes),
        content_type='application/json'
    )
    
    return object_name
