import os
import pandas as pd
import psycopg2
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

CSV_CONFIG = {
    'PATH_CONFIG': {
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
    },
    'OPEN_CONFIG': {
        'error_bad_lines': False,
        'warn_bad_lines': False,
        'delimiter': ';',
        'na_values': ['null'],
        'engine': 'python'
    }
}


def push_to_sql(tabla_name, dataframe):
    # создаем подключение к БД
    conn = psycopg2.connect(
        # получаем значения для подключения к БД из переменных окружения Docker
        host='db',
        port=os.environ.get('DB_PORT'),
        dbname=os.environ.get('POSTGRES_DB'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD')
    )

    # загрузка DataFrame в базу данных PostgreSQL
    dataframe.to_sql(f'f{tabla_name}', conn, if_exists='replace', index=False)

    # подтверждаем транзакцию и закрываем соединение с БД
    conn.commit()
    conn.close()


def merge_dataframe_from_csv_files(csv_path_config, csv_openfile_options):
    # Создаем пустой список для хранения данных из каждого файла
    dataframes = []
    shape_list = []

    for year in csv_path_config:
        path = csv_path_config[year]['path']
        encoding = csv_path_config[year]['encoding']
        df = pd.read_csv(path, encoding=encoding, **csv_openfile_options)
        df['YEAR'] = year

        # Добавляем DataFrame в список dataframes
        dataframes.append(df)
        shape_list.append(df.shape)

    return pd.concat(dataframes)


if __name__ == '__main__':
    merged_df = merge_dataframe_from_csv_files(CSV_CONFIG['PATH_CONFIG'], CSV_CONFIG['OPEN_CONFIG'])

    random_sample = merged_df.sample(n=30)
    random_sample.to_csv('random-sample.csv')
