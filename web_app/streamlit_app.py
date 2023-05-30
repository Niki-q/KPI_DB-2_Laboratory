from collections import namedtuple
import altair as alt
import math
import pandas as pd
import streamlit as st
import os
from sqlalchemy.sql import text
import psycopg2
from psycopg2 import OperationalError


DB_CONFIG = {'user': os.getenv("DB_USER"),
             'password': os.getenv("DB_PASSWORD"),
             'host': os.getenv("DB_HOST"),
             'port': os.getenv("DB_PORT"),
             'database': os.getenv("DB_NAME")}


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


def sql_to_dataframe(conn, query, table_name):
    """
    Import data from a PostgreSQL database using a SELECT query
    """
    cursor = conn.cursor()
    cursor.execute(query)
    # The execute returns a list of tuples:
    tuples_list = cursor.fetchall()

    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
    column_names = cursor.fetchall()

    cursor.close()
    # Now we need to transform the list into a pandas DataFrame:
    df = pd.DataFrame(tuples_list, columns=column_names).set_index(column_names[0])
    return df


conn = open_conn(DB_CONFIG)
query = st.text_input(label='sql', value="""SELECT * FROM public.testings ORDER BY "ID" ASC LIMIT 100;""")
table_name = st.text_input(label='table_name', value="testings")
df = sql_to_dataframe(conn, query, table_name)
edited_df = st.experimental_data_editor(df, num_rows="dynamic")
st.balloons()
