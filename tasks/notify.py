import requests
from prefect import task

@task(retries=3, retry_delay_seconds=5)
def send_telegram_notification(city: str, daily_data: dict, bot_token: str, chat_id: str):
    """
    Отправляет уведомление с прогнозом в Telegram
    """
    temp_min = daily_data['temp_min']
    temp_max = daily_data['temp_max']
    temp_avg = daily_data['temp_avg']
    precipitation = daily_data['total_precipitation']
    
    message = f"🌤 Прогноз погоды на завтра для {city}:\n\n"
    message += f"🌡 Температура:\n"
    message += f"  • Минимум: {temp_min:.1f}°C\n"
    message += f"  • Максимум: {temp_max:.1f}°C\n"
    message += f"  • Среднее: {temp_avg:.1f}°C\n"
    message += f"🌧 Осадки: {precipitation:.1f} мм\n"
    
    # Предупреждения
    if precipitation > 10:
        message += "\n⚠️ ВНИМАНИЕ: Ожидаются сильные осадки!"
    if temp_max > 35:
        message += "\n⚠️ ВНИМАНИЕ: Ожидается сильная жара!"
    if temp_min < -20:
        message += "\n⚠️ ВНИМАНИЕ: Ожидается сильный мороз!"
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'HTML'
    }
    
    response = requests.post(url, json=payload, timeout=10)
    response.raise_for_status()
    
    return response.json()