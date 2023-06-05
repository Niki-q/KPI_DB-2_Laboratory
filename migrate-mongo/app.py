import logging
import os
import time
import psycopg2
from pymongo import MongoClient

DB_CONFIG = {'user': os.getenv("POSTGRES_USER"),
             'password': os.getenv("POSTGRES_PASSWORD"),
             'host': os.getenv("POSTGRES_HOST"),
             'port': os.getenv("POSTGRES_PORT"),
             'database': os.getenv("POSTGRES_NAME")}





def init_logging():
    """
    Инициализирует журналирование (логирование) для текущего модуля с уровнем DEBUG.

    Возвращает:
    Экземпляр логгера для текущего модуля.
    """
    logging.basicConfig(level=logging.DEBUG)
    return logging.getLogger(__name__)


start_time = time.time()

logger = init_logging()




# Підключення до PostgreSQL
pg_conn = psycopg2.connect(database=DB_CONFIG['database'], user=DB_CONFIG['user'], password=DB_CONFIG['password'], host=DB_CONFIG['host'], port=DB_CONFIG['port'])
pg_cursor = pg_conn.cursor()

mongo_host = os.getenv('MONGO_HOST')
mongo_port = int(os.getenv('MONGO_PORT'))
mongo_database = os.getenv('MONGO_DB')


mongo_client = MongoClient(f'mongodb://{mongo_host}:{mongo_port}')
mongo_db = mongo_client[f'{mongo_database}']

mongo_client.drop_database(mongo_db)



mongo_collection = mongo_db['educational_institution_mongo']

# Отримання загальної кількості рядків в PostgreSQL таблиці
pg_cursor.execute("SELECT COUNT(*) FROM educational_institutions")
total_rows = pg_cursor.fetchone()[0]

# Підготовка списку документів для вставки в MongoDB
documents = []
pg_cursor.execute("SELECT * FROM educational_institutions")
# Ітерація через рядки з PostgreSQL та додавання їх до списку документів
for row in pg_cursor:
    # Перетворення рядка з PostgreSQL на документ MongoDB
    document = {
        '_id': row[0],
        'TypeName': row[1],
        'AreaName':row[2],
        'TerName': row[3],
        'Parent': row[4],
        'Name': row[5]
    }
    documents.append(document)

    # Якщо список документів досягне певного розміру (наприклад, 1000), виконується вставка в MongoDB
    if len(documents) == 10000:
        mongo_collection.insert_many(documents)
        documents = []  # Очистка списку документів

# Вставка залишку документів, якщо вони є
if documents:
    mongo_collection.insert_many(documents)

logger.info("educational_institution_mongo already migrated")


mongo_collection = mongo_db['territory_mongo']

# Отримання загальної кількості рядків в PostgreSQL таблиці
pg_cursor.execute("SELECT COUNT(*) FROM territories")
total_rows = pg_cursor.fetchone()[0]

# Підготовка списку документів для вставки в MongoDB
documents = []
pg_cursor.execute("SELECT * FROM territories")
# Ітерація через рядки з PostgreSQL та додавання їх до списку документів
for row in pg_cursor:
    # Перетворення рядка з PostgreSQL на документ MongoDB
    document = {
        '_id': row[0],
        'Name': row[1],
        'TypeName':row[2],
    }
    documents.append(document)

    # Якщо список документів досягне певного розміру (наприклад, 1000), виконується вставка в MongoDB
    if len(documents) == 10000:
        mongo_collection.insert_many(documents)
        documents = []  # Очистка списку документів

# Вставка залишку документів, якщо вони є
if documents:
    mongo_collection.insert_many(documents)

logger.info("territory_mongo already migrated")



mongo_collection = mongo_db['participant_mongo']

# Отримання загальної кількості рядків в PostgreSQL таблиці
pg_cursor.execute("SELECT COUNT(*) FROM participants")
total_rows = pg_cursor.fetchone()[0]

# Підготовка списку документів для вставки в MongoDB
documents = []
pg_cursor.execute("SELECT * FROM participants")
# Ітерація через рядки з PostgreSQL та додавання їх до списку документів
for row in pg_cursor:
    # Перетворення рядка з PostgreSQL на документ MongoDB
    document = {
        '_id': row[0],
        'Birth': row[1],
        'SexTypeName':row[2],
        'RegName': row[3],
        'AreaName': row[4],
        'Ter_id': row[5],
        'ClassProfileName': row[6],
        'ClassLangName': row[7],
        'EO_id': row[8],

    }
    documents.append(document)

    # Якщо список документів досягне певного розміру (наприклад, 1000), виконується вставка в MongoDB
    if len(documents) == 10000:
        mongo_collection.insert_many(documents)
        documents = []  # Очистка списку документів

# Вставка залишку документів, якщо вони є
if documents:
    mongo_collection.insert_many(documents)
logger.info("participants_mongo already migrated")




mongo_collection = mongo_db['testing_mongo']

# Отримання загальної кількості рядків в PostgreSQL таблиці
pg_cursor.execute("SELECT COUNT(*) FROM testings")
total_rows = pg_cursor.fetchone()[0]

# Підготовка списку документів для вставки в MongoDB
documents = []
pg_cursor.execute("SELECT * FROM testings")
# Ітерація через рядки з PostgreSQL та додавання їх до списку документів
for row in pg_cursor:
    # Перетворення рядка з PostgreSQL на документ MongoDB
    document = {
        '_id': row[0],
        'Part_ID': row[1],
        'Point_ID':row[2],
        'Year': row[3],
        'Test': row[4],
        'TestStatus': row[5],
        'Ball100': row[6],
        'Ball12': row[7],
        'Ball':row[8]
    }
    documents.append(document)

    # Якщо список документів досягне певного розміру (наприклад, 1000), виконується вставка в MongoDB
    if len(documents) == 10000:
        mongo_collection.insert_many(documents)
        documents = []  # Очистка списку документів

# Вставка залишку документів, якщо вони є
if documents:
    mongo_collection.insert_many(documents)

logger.info("testings_mongo already migrated")


mongo_collection = mongo_db['point_of_observation_mongo']

# Отримання загальної кількості рядків в PostgreSQL таблиці
pg_cursor.execute("SELECT COUNT(*) FROM points_of_observation")
total_rows = pg_cursor.fetchone()[0]

# Підготовка списку документів для вставки в MongoDB
documents = []
pg_cursor.execute("SELECT * FROM points_of_observation")
# Ітерація через рядки з PostgreSQL та додавання їх до списку документів
for row in pg_cursor:
    # Перетворення рядка з PostgreSQL на документ MongoDB
    document = {
        '_id': row[0],
        'Name': row[1],
        'RegName':row[2],
        'AreaName': row[3],
        'TerName': row[4],
    }
    documents.append(document)

    # Якщо список документів досягне певного розміру (наприклад, 1000), виконується вставка в MongoDB
    if len(documents) == 10000:
        mongo_collection.insert_many(documents)
        documents = []  # Очистка списку документів

# Вставка залишку документів, якщо вони є
if documents:
    mongo_collection.insert_many(documents)

logger.info("points_of_observation_mongo already migrated")


# Закриття підключень
pg_cursor.close()
pg_conn.close()
mongo_client.close()


fin_time = time.time() - start_time
logger.info(f"Час виконання програми: {fin_time} секунд ({round(fin_time / 60, 2)} хвилин) ")
