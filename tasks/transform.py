import pandas as pd
from datetime import datetime
from prefect import task, get_run_logger  # ← Добавьте get_run_logger


@task
def transform_to_hourly_with_logging(data: dict) -> pd.DataFrame:
    """
    Преобразует почасовые данные в формат для weather_hourly
    """
    logger = get_run_logger()
    logger.info(f"Начало трансформации данных для города {data['city']}")
    
    try:
        hourly_data = data['hourly']
        city = data['city']
        
        df = pd.DataFrame({
            'city': [city] * len(hourly_data['time']),
            'timestamp': pd.to_datetime(hourly_data['time']),
            'temperature': hourly_data['temperature_2m'],
            'precipitation': hourly_data['precipitation'],
            'wind_speed': hourly_data['wind_speed_10m'],
            'wind_direction': hourly_data['wind_direction_10m']
        })
        
        logger.info(f"Успешно обработано {len(df)} записей")
        return df
    except Exception as e:
        logger.error(f"Ошибка при трансформации: {str(e)}")
        raise


@task
def transform_to_daily_with_logging(data: dict) -> pd.DataFrame:
    """
    Агрегирует почасовые данные в дневную статистику
    """
    logger = get_run_logger()
    logger.info(f"Создание дневной статистики для {data['city']}")
    
    try:
        hourly_data = data['hourly']
        city = data['city']
        
        temps = hourly_data['temperature_2m']
        precip = hourly_data['precipitation']
        date = pd.to_datetime(hourly_data['time'][0]).date()
        
        daily_data = {
            'city': [city],
            'date': [date],
            'temp_min': [min(temps)],
            'temp_max': [max(temps)],
            'temp_avg': [sum(temps) / len(temps)],
            'total_precipitation': [sum(precip)]
        }
        
        df = pd.DataFrame(daily_data)
        logger.info(f"Статистика: min={daily_data['temp_min'][0]:.1f}°C, max={daily_data['temp_max'][0]:.1f}°C")
        return df
    except Exception as e:
        logger.error(f"Ошибка при создании статистики: {str(e)}")
        raise
