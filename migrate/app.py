import csv
import logging
import os
import time

import psycopg2
from flask import Flask
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from psycopg2 import OperationalError
import subprocess

GENERAL_TABLE_NAME = os.getenv("RESULTS_TABLE_NAME")
OUT_CSV_FILE = os.getenv("OUTPUT_FILE_NAME")

DB_CONFIG = {'user': os.getenv("DB_USER"),
             'password': os.getenv("DB_PASSWORD"),
             'host': os.getenv("DB_HOST"),
             'port': os.getenv("DB_PORT"),
             'database': os.getenv("DB_NAME")}

SQLA_CONFIG_STR = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

TASK_QUERY = ["""
    SELECT 
    p."RegName",
    ROUND(AVG(t."Ball")::NUMERIC, 2) AS average_score,
    t."Year"
FROM 
    testings t
    INNER JOIN participants p ON t."Part_ID" = p."ID"
WHERE 
    t."Ball" IS NOT NULL
    AND
    t."Ball"::text !~ '[^0-9.-]'
    AND
    t."TestStatus" = 'Зараховано'
GROUP BY 
    p."RegName",
    t."Year";
    """, """
        SELECT 
        p."RegName",
        ROUND(AVG(CASE WHEN t."Year" = 2019 THEN t."Ball" END)::NUMERIC, 2) AS average_score_2019,
        ROUND(AVG(CASE WHEN t."Year" = 2020 THEN t."Ball" END)::NUMERIC, 2) AS average_score_2020
    FROM 
        testings t
        INNER JOIN participants p ON t."Part_ID" = p."ID"
    WHERE 
        t."TestStatus" = 'Зараховано'
        AND
        t."Ball"::text !~ '[^0-9.-]'
        AND
        t."Ball" IS NOT NULL
        AND
        (t."Year" = 2019 OR t."Year" = 2020)
    GROUP BY 
        p."RegName";
        """]

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = SQLA_CONFIG_STR
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy()

from models import *

db.init_app(app)

migrate = Migrate(app, db)


def init_logging():
    """
    Инициализирует журналирование (логирование) для текущего модуля с уровнем DEBUG.

    Возвращает:
    Экземпляр логгера для текущего модуля.
    """
    logging.basicConfig(level=logging.DEBUG)
    return logging.getLogger(__name__)


def open_conn(db_config):
    """
    Функция open_conn принимает на вход конфигурацию базы данных db_config и устанавливает соединение с базой данных PostgreSQL.
    В случае неудачного подключения, функция повторяет попытку 3 раза с интервалом в 5 секунд между попытками.
    Если после 3 попыток соединение не установлено, то генерируется исключение.

    Аргументы:

    db_config (dict): словарь, содержащий параметры подключения к базе данных
    Возвращаемое значение:

    conn (psycopg2.extensions.connection): объект-соединение с базой данных
    """
    check = 3
    while check != 0:
        try:
            conn = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                dbname=db_config['database'],
                user=db_config['user'],
                password=db_config['password']
            )
            break
        except OperationalError:
            print(f"Не удалось подключиться к базе данных {db_config['database']}. Попытка повторого подключения будет"
                  f" выполнена еще {check} раз(а)")
            time.sleep(5)
            check -= 1
            if check == 0:
                raise
    return conn


def execute_query(conn: psycopg2.extensions.connection, query: str, csv_name: str or None = None) -> None:
    """
    Виконує SQL-запит на заданому з'єднанні та, якщо вказано, зберігає результат у файл CSV.

        Аргументи:
        - conn (psycopg2.extensions.connection): Об'єкт з'єднання з базою даних PostgreSQL.
        - query (str): SQL-запит, який потрібно виконати.
        - csv_name (str або None, за замовчуванням None): Назва файлу CSV, у який потрібно зберегти результати запиту.
          Якщо не вказано, результати запиту не будуть збережені у файл.

        Повертає:
        None

    """
    condition = csv_name is not None
    with conn.cursor() as cursor:
        cursor.execute(query)
        if condition:
            columns = [desc[0] for desc in cursor.description]  # Отримання назв стовпців
            rows = cursor.fetchall()
    logger.info('Запит успішно виконано! ')
    if condition:
        with open(csv_name, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow(columns)  # Запис назв стовпців
            writer.writerows(rows)
        logger.info(f'Файл csv для запиту завдання – створено! Його можна перевірити за шляхом {csv_name}')


if __name__ == '__main__':
    start_time = time.time()
    logger = init_logging()

    def run_flask_db_upgrade():
        command = 'flask db upgrade'
        subprocess.call(command, shell=True)

    logger.info("Розпочато міграцію...")
    run_flask_db_upgrade()

    conn = open_conn(DB_CONFIG)
    logger.info("Виконується запит до завдання з варіантом 2")
    execute_query(conn, TASK_QUERY[0], 'results/result_variant_2.csv')

    logger.info("Виконується запит до завдання з варіантом 2 у іншому форматі")
    execute_query(conn, TASK_QUERY[1], 'results/result_variant_2_type2.csv')
    conn.close()

    fin_time = time.time() - start_time
    logger.info(f"Час виконання програми: {fin_time} секунд ({round(fin_time / 60, 2)} хвилин) ")
