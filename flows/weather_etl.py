from prefect import flow
from tasks.extract import fetch_weather_forecast_with_backoff, save_to_minio
from tasks.transform import transform_to_hourly_with_logging, transform_to_daily_with_logging
from tasks.load import load_to_clickhouse_hourly, load_to_clickhouse_daily
from tasks.notify import send_telegram_notification
from tasks.load import validate_data_quality

# Координаты городов
CITIES = {
    'Москва': {'lat': 55.7558, 'lon': 37.6173},
    'Самара': {'lat': 53.1959, 'lon': 50.1002}
}

@flow(name="Weather ETL Pipeline", log_prints=True)
def weather_etl(
    bot_token: str = "8346556112:AAGELOt7c2_NW44HjtKe2_l7h4Ufj2BMZWk",
    chat_id: str = "523719246"
):
    """
    Основной ETL поток для обработки погодных данных
    """
    print("Запуск ETL пайплайна для погодных данных...")
    
    for city_name, coords in CITIES.items():
        print(f"\nОбработка данных для города: {city_name}")
        
        # Extract
        weather_data = fetch_weather_forecast_with_backoff(
            city=city_name,
            lat=coords['lat'],
            lon=coords['lon']
        )
        
        # Сохранение сырых данных
        minio_path = save_to_minio(weather_data, city_name)
        print(f"Сырые данные сохранены: {minio_path}")
        
        # Transform
        hourly_df = transform_to_hourly_with_logging(weather_data)
        daily_df = transform_to_daily_with_logging(weather_data)

        # Validate
        hourly_df = validate_data_quality(hourly_df, 'hourly')
        daily_df = validate_data_quality(daily_df, 'daily')
        
        # Load
        hourly_count = load_to_clickhouse_hourly(hourly_df)
        print(f"Загружено {hourly_count} почасовых записей")
        
        daily_count = load_to_clickhouse_daily(daily_df)
        print(f"Загружено {daily_count} дневных записей")
        
        # Notify
        if bot_token and chat_id:
            daily_data_dict = daily_df.iloc[0].to_dict()
            send_telegram_notification(
                city=city_name,
                daily_data=daily_data_dict,
                bot_token=bot_token,
                chat_id=chat_id
            )
            print(f"Уведомление отправлено в Telegram")
    
    print("\nETL пайплайн успешно завершен!")

if __name__ == "__main__":
    # Для локального тестирования
    weather_etl(
        bot_token="8346556112:AAGELOt7c2_NW44HjtKe2_l7h4Ufj2BMZWk",
        chat_id="523719246"
    )