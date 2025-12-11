from flows.weather_etl import weather_etl

if __name__ == "__main__":
    weather_etl.serve(
        name="weather-etl-deployment",
        cron="0 9 * * *",  # Каждый день в 9:00
        parameters={
            "bot_token": "YOUR_BOT_TOKEN",
            "chat_id": "YOUR_CHAT_ID"
        }
    )