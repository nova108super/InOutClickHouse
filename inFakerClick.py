import clickhouse_connect
import datetime
import uuid
import pandas as pd
from faker import Faker

ch = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='click',
    password='click',
)

fake = Faker(locale='ru_RU')

list_of_dict = []
for _ in range(100):
    # Генерируем даты с правильными методами
    start_2024 = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    end_2025 = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
    
    start_1980 = datetime.datetime(1980, 1, 1)
    end_2005 = datetime.datetime(2005, 1, 1)
    
    dict_ = {
        'id': str(uuid.uuid4()),  # Конвертируем в строку!
        'created_at': fake.date_time_between(
            start_date=start_2024,
            end_date=end_2025
        ),
        'updated_at': fake.date_time_between(
            start_date=start_2024,
            end_date=end_2025
        ),
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'middle_name': fake.middle_name(),
        'birthday': fake.date_time_between(
            start_date=start_1980,
            end_date=end_2005
        ).date(),  # Берём только дату (без времени)
        'email': fake.email(),
        'city': fake.city(),
    }

    list_of_dict.append(dict_)

df = pd.DataFrame(list_of_dict)

# Вставка данных в ClickHouse
ch.insert_df(
    df=df,
    table='users',
    database='default',
)
