import os
import pandas as pd
import psycopg2

CSV_PATH = {
    2021: {'path': './data/Odata2021File.csv',
           'encoding': 'UTF-8-SIG'
           },
    2020: {'path': './data/Odata2020File.csv',
           'encoding': 'windows-1251'
           },
    2019: {'path': './data/Odata2019File.csv',
           'encoding': 'windows-1251'
           },
    2018: {'path': './data/OpenData2018.csv',
           'encoding': 'UTF-8-SIG'
           },
    2017: {'path': './data/OpenData2017.csv',
           'encoding': 'UTF-8-SIG'
           },
    2016: {'path': './data/OpenData2016.csv',
           'encoding': 'windows-1251'
           }
}

CSV_OPTIONS = {
    'error_bad_lines': False, 'warn_bad_lines': False,
    # 'skip_bad_lines': True,
    'delimiter': ';', 'na_values': ['null'], 'engine': 'python'}

# получаем значения для подключения к БД из переменных окружения Docker

db_port = os.environ.get('DB_PORT')
db_name = os.environ.get('POSTGRES_DB')
db_user = os.environ.get('POSTGRES_USER')
db_password = os.environ.get('POSTGRES_PASSWORD')


def push_to_sql(tabla_name, dataframe):
    # создаем подключение к БД
    conn = psycopg2.connect(
        host='db',
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )

    # загрузка DataFrame в базу данных PostgreSQL
    dataframe.to_sql(f'f{tabla_name}', conn, if_exists='replace', index=False)

    # подтверждаем транзакцию и закрываем соединение с БД
    conn.commit()
    conn.close()


# определяем функцию для обработки строк, которые не могут быть разобраны
def handle_bad_lines(line):
    print(f'Error line - {line}')
    pass


# Создаем пустой список для хранения данных из каждого файла
dataframes = []
shape_list = []

for year in CSV_PATH:
    print(year, 'already imported to dataframe')
    path = CSV_PATH[year]['path']
    encoding = CSV_PATH[year]['encoding']

    df = pd.read_csv(path,
                     # on_bad_lines=handle_bad_lines,
                     encoding=encoding, **CSV_OPTIONS)
    df['YEAR'] = year

    # Добавляем DataFrame в список dataframes
    dataframes.append(df)
    shape_list.append(df.shape)

merged_df = pd.concat(dataframes)
print(shape_list)
print(merged_df.shape)
print(merged_df.columns)

random_sample = merged_df.sample(n=30)

print(random_sample.to_string())
random_sample.to_csv('random-sample.csv')
