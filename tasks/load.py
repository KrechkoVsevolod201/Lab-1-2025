import clickhouse_connect
import pandas as pd
from prefect import task, get_run_logger

@task
def validate_data_quality(df: pd.DataFrame, table_type: str) -> pd.DataFrame:
    """
    Проверяет качество данных перед загрузкой
    """
    logger = get_run_logger()
    
    # Проверка на пустоту
    if df.empty:
        raise ValueError(f"DataFrame для {table_type} пуст!")
    
    # Проверка на пропущенные значения
    null_counts = df.isnull().sum()
    if null_counts.any():
        logger.warning(f"Обнаружены пропущенные значения: {null_counts[null_counts > 0].to_dict()}")
    
    # Проверка диапазонов для температуры
    if table_type == 'hourly' and 'temperature' in df.columns:
        invalid_temps = df[(df['temperature'] < -90) | (df['temperature'] > 60)]
        if not invalid_temps.empty:
            logger.error(f"Найдены некорректные температуры: {invalid_temps}")
            raise ValueError("Температура вне допустимого диапазона!")
    
    # Проверка на дубликаты
    duplicates = df.duplicated(subset=['city', 'timestamp' if table_type == 'hourly' else 'date'])
    if duplicates.any():
        logger.warning(f"Найдено {duplicates.sum()} дубликатов, удаляем...")
        df = df.drop_duplicates(subset=['city', 'timestamp' if table_type == 'hourly' else 'date'])
    
    logger.info(f"Валидация данных для {table_type} успешно пройдена")
    return df


@task(retries=2, retry_delay_seconds=5)
def load_to_clickhouse_hourly(df: pd.DataFrame):
    """
    Загружает почасовые данные в ClickHouse
    """
    client = clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username='default',
        password='',
        database='weather_db'
    )
    
    client.insert_df(
        'weather_hourly',
        df
    )
    
    return len(df)

@task(retries=2, retry_delay_seconds=5)
def load_to_clickhouse_daily(df: pd.DataFrame):
    """
    Загружает дневные данные в ClickHouse
    """
    client = clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username='default',
        password='',
        database='weather_db'
    )
    
    client.insert_df(
        'weather_daily',
        df
    )
    
    return len(df)