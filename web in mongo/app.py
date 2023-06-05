import logging
import os
import pickle
import time
import uuid
import psycopg2
from flask import Flask, redirect, request, render_template
from flask_sqlalchemy import SQLAlchemy
from psycopg2 import OperationalError
from pymongo import MongoClient
from sqlalchemy import func, distinct
import redis
from flask import jsonify
import struct

client = MongoClient('mongodb://localhost:27017')
db1 = client['result_zno']

GENERAL_TABLE_NAME = os.getenv("RESULTS_TABLE_NAME")
OUT_CSV_FILE = os.getenv("OUTPUT_FILE_NAME")

DB_CONFIG = {'user': os.getenv("DB_USER"),
             'password': os.getenv("DB_PASSWORD"),
             'host': os.getenv("DB_HOST"),
             'port': os.getenv("DB_PORT"),
             'database': os.getenv("DB_NAME")}

SQLA_CONFIG_STR = f"postgresql://postgres:111@localhost:5432/zno"

app = Flask(__name__, template_folder='templates')

redis_host = "localhost"
redis_port = 6379

redis_client = redis.Redis(host='localhost', port=6379)

app.config['CACHE_TYPE'] = 'redis'
app.config['CACHE_REDIS_HOST'] = redis_host
app.config['CACHE_REDIS_PORT'] = redis_port


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


def get_table_model(table_name):
    table_models = {
        'educational_institutions': EducationalInstitution,
        'territories': Territory,
        'participants': Participant,
        'points_of_observation': PointOfObservation,
        'testings': Testing
    }
    return table_models.get(table_name)


def get_table_data(selected_table, count_limit):
    models = [EducationalInstitution, Participant, Territory, PointOfObservation, Testing]
    if selected_table not in [model.__tablename__ for model in models]:
        raise ValueError('Недопустимый запрос. Неверное имя таблицы')
    for model in models:
        if selected_table == model.__tablename__:
            table_data = model.query.order_by(model.ID).limit(count_limit).all()
            column_names = [column.key for column in model.__table__.columns]

    return column_names, table_data


@app.route('/')
def index():
    return render_template('chose.html')


@app.route('/query', methods=['GET'])
def query():
    participant_collection = db1['participant_mongo']
    testing_collection = db1['testing_mongo']

    reg_names = participant_collection.distinct('RegName')
    tests = testing_collection.distinct('Test')
    average_scores = False

    return render_template('query.html', reg_names=reg_names, tests=tests, average_scores=average_scores)


def get_regname_list():
    collection = db1['participant_mongo']

    regname_set = set()
    cursor = collection.find({}, {'RegName': 1})
    for document in cursor:
        regname = document['RegName']
        regname_set.add(regname)

    return list(regname_set)


def get_test_list():
    collection = db1['testing_mongo']

    test_set = set()
    cursor = collection.find({}, {'Test': 1})
    for document in cursor:
        test = document['Test']
        test_set.add(test)

    return list(test_set)


@app.route('/query', methods=['POST'])
def execute_query():
    reg_name = request.form.get('reg_name')
    test_name = request.form.get('test_name')
    year = request.form.get('year')

    arguments = {'selected_reg_name': reg_name, 'selected_test_name': test_name, 'selected_year': year}

    # key = reg_name + test_name + year
    reg_names = get_regname_list()
    tests = get_test_list()
    year_options = [2019, 2020]
    # Отримання унікальних років з бази даних

    # if average_scores := redis_client.get(str(key)):
    #    average_scores = pickle.loads(average_scores)
    #    return render_template('query.html', reg_names=reg_names, tests=tests, year_options=year_options,
    #                           average_scores=average_scores, reg_name=reg_name, year=year, **arguments)
    # else:
    #    cache = False

    # Обработка отправки формы и выполнение необходимых действий
    print(test_name, reg_name, year)
    avg_dict = {}
    if reg_name == 'all':
        if year == 'all':
            result = db1['testing_mongo'].aggregate([
                {
                    '$match': {
                        'Test': test_name,
                        'TestStatus': 'Зараховано',
                    }
                }, {
                    '$lookup': {
                        'from': 'participant_mongo',
                        'localField': 'Part_ID',
                        'foreignField': '_id',
                        'as': 'participant'
                    }
                }, {
                    '$group': {
                        '_id': {
                            'Year': '$Year',
                            'RegName': '$participant.RegName'
                        },
                        'average': {
                            '$avg': '$Ball'
                        }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'Year': '$_id.Year',
                        'RegName': '$_id.RegName',
                        'average': 1
                    }
                }
            ])

        else:
            result = client['result_zno']['testing_mongo'].aggregate([
                {
                    '$match': {
                        'Year': int(year),
                        'Test': test_name,
                        'TestStatus': 'Зараховано',
                    }
                }, {
                    '$lookup': {
                        'from': 'participant_mongo',
                        'localField': 'Part_ID',
                        'foreignField': '_id',
                        'as': 'participant'
                    }
                }, {
                    '$group': {
                        '_id': {
                            'Year': '$Year',
                            'RegName': '$participant.RegName'
                        },
                        'average': {
                            '$avg': '$Ball'
                        }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'Year': '$_id.Year',
                        'RegName': '$_id.RegName',
                        'average': 1
                    }
                },

            ])
        print(result)

        for elem in result:
            # if ''.join(elem["RegName"] != ""
            RegName = ''.join(elem["RegName"])
            Year = elem["Year"]
            avg_dict[f'{RegName}-{Year}'] = elem['average']

        print(avg_dict)
        #    reg_name1 = i['RegName']
        #    year1 = i['Year']
        #    average_ball1 = i['average']
        #    print(reg_name1,average_ball1)

        # if not cache:
        #    redis_client.set(str(key), pickle.dumps(average_scores))
        # print(average_scores)
        return render_template('query.html', reg_names=reg_names, tests=tests, year_options=year_options,
                               average_scores=avg_dict, reg_name=reg_name, year=year, **arguments)
    else:
        if year == 'all':
            result = client['result_zno']['testing_mongo'].aggregate([
                {
                    '$match': {
                        'Test': test_name,
                        'TestStatus': 'Зараховано',
                    }
                }, {
                    '$lookup': {
                        'from': 'participant_mongo',
                        'localField': 'Part_ID',
                        'foreignField': '_id',
                        'as': 'participant'
                    }
                }, {
                    '$group': {
                        '_id': {
                            'Year': '$Year',
                            'RegName': '$participant.RegName'
                        },
                        'average': {
                            '$avg': '$Ball'
                        }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'Year': '$_id.Year',
                        'RegName': '$_id.RegName',
                        'average': 1
                    }
                },
                {
                    '$match': {
                        'RegName': reg_name,
                    }
                }
            ])

            avg_dict = {}
            for elem in result:
                RegName = ''.join(elem["RegName"])
                Year = elem["Year"]
                avg_dict[f'{RegName}-{Year}'] = elem['average']

            print(avg_dict)
            # if not cache:
            #    redis_client.set(str(key), pickle.dumps(average_scores))
            return render_template('query.html', reg_names=reg_names, tests=tests, year_options=year_options,
                                   average_scores=avg_dict, reg_name=reg_name, year=year, **arguments)
        else:
            result = client['result_zno']['testing_mongo'].aggregate([
                {
                    '$match': {
                        'Test': test_name,
                        'Year': int(year),
                        'TestStatus': 'Зараховано',
                    }
                }, {
                    '$lookup': {
                        'from': 'participant_mongo',
                        'localField': 'Part_ID',
                        'foreignField': '_id',
                        'as': 'participant'
                    }
                }, {
                    '$group': {
                        '_id': {
                            'Year': '$Year',
                            'RegName': '$participant.RegName'
                        },
                        'average': {
                            '$avg': '$Ball'
                        }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'Year': '$_id.Year',
                        'RegName': '$_id.RegName',
                        'average': 1
                    }
                },
                {
                    '$match': {
                        'RegName': reg_name,
                    }
                }
            ])

            print(result)
            avg_dict = {}
            for elem in result:
                print(elem["Year"])
                RegName = ''.join(elem["RegName"])
                Year = elem["Year"]
                avg_dict[f'{RegName}-{Year}'] = elem['average']

            print(avg_dict)

            # if not cache:
            #    redis_client.set(str(key), pickle.dumps(average_scores))
            return render_template('query.html', reg_names=reg_names, tests=tests, year_options=year_options,
                                   average_scores=avg_dict, average_score=avg_dict, reg_name=reg_name,
                                   year=year, **arguments)


def get_model_class(table_name):
    d = {
        'educational_institutions': EducationalInstitution,
        'territories': Territory,
        'participants': Participant,
        "points_of_observation": PointOfObservation,
        'testings': Testing
    }
    return d.get(table_name, None)


@app.route('/update_row', methods=['POST'])
def update_row():
    table_name = request.form.get('table_name')
    row_id = request.form.get('row_id')
    updated_data = {column: request.form.get(column) for column in request.form if
                    column != 'table_name' and column != 'row_id'}

    collection = db1[table_name]

    # Запит до MongoDB для оновлення документа
    if table_name != "participant_mongo":
        query = {'_id': int(row_id)}
    else:
        query = {'_id': row_id}

    update_data = {'$set': {k: v for k, v in updated_data.items() if k != '_id'}}
    collection.update_one(query, update_data, upsert=True)

    selected_table = table_name
    limit = 10
    column_names = list(collection.find_one().keys())
    all_pts = list(collection.find().limit(limit))
    return render_template('site.html', column_names=column_names, table_data=all_pts, table_name=selected_table)


@app.route('/add_row', methods=['GET'])
def add_row():
    return render_template('add_row.html')


def get_distinct_values(collection_name, column_name):
    collection = db1[collection_name]
    distinct_values = collection.distinct(column_name)
    return distinct_values


@app.route('/add_rows', methods=['POST'])
def add_rows():
    collection_name = request.form['table_name']
    updated_data = {column: request.form.get(column) for column in request.form if
                    column != 'table_name' and column != 'row_id'}
    print(updated_data)

    # Generate a random 32-bit integer ID using UUID
    random_id = struct.unpack('I', uuid.uuid4().bytes[:4])[0]

    # Add the ID to the updated_data dictionary
    updated_data['_id'] = random_id

    collection = db1[collection_name]  # MongoDB collection object
    collection.insert_one(updated_data)  # Insert the updated data as a new document

    return redirect('/main')


@app.route('/upgate_row', methods=['POST'])
def upgate_row():
    table_name = request.form['table-select']
    collection = db1[table_name]

    column_names = list(collection.find_one().keys())
    all_pts = list(collection.find().limit(1))
    excluded_column = ['_id']

    for column in excluded_column:
        if column in column_names:
            column_names.remove(column)
    result = {}
    special_colum = []
    for i in column_names:
        temp = get_distinct_values(table_name, i)
        if len(temp) <= 25:
            result[i] = [temp]
            special_colum.append(i)
    return render_template('add_row.html', selected_table=table_name, columns=column_names, special_colum=special_colum,
                           result=result)


@app.route('/upgrade', methods=['POST'])
def upgrade():
    row_id = request.form['row_id']
    table_name = request.form['table_name']

    table_model = get_table_model(table_name)
    row = table_model.query.get(row_id)

    for column in row.__table__.columns:
        column_name = column.name
        new_value = request.form.get(column_name)
        setattr(row, column_name, new_value)

    db.session.commit()

    return render_template('edit_row.html', row=row, table_name=table_name, row_id=row_id)


@app.route('/delete_row', methods=['POST'])
def delete_row():
    row_id = request.form['row_id']
    table_name = request.form['table_name']

    collection = db1[table_name]
    if table_name != "participant_mongo":
        query = {'_id': int(row_id)}
        collection.delete_one(query)
    else:
        query = {'_id': row_id}
        collection.delete_one(query)
    return redirect('/main')


@app.route('/search', methods=['POST'])
def search():
    table_search = request.form['table_search']
    column_search = request.form['column_search']
    value_search = request.form['value_search']

    collection = db1[table_search]

    query = {column_search: value_search}
    results = collection.find(query).limit(20)

    table_data = list(results)

    column_names = list(collection.find_one().keys())

    return render_template('site.html', table_data=table_data, table_name=table_search, column_names=column_names,
                           search_data=[table_search, column_search, value_search], limit=10)


@app.route('/get_columns', methods=['POST'])
def get_columns():
    table = request.json['table']
    collection = db1[table]
    first_document = collection.find_one()
    column_names = list(first_document.keys())
    return jsonify(column_names)


@app.route('/select_database', methods=['POST'])
def select_database():
    database = request.form.get('database')
    print(1, database)
    if database == "mongo":
        db1 = client['result_zno']

    return redirect('/main')


@app.route('/main', methods=['GET', 'POST'])
def table(limit=10):
    if request.method == 'POST':
        selected_table = request.form['table']
        limit = int(request.form['limit'])
        collection = db1[selected_table]

        column_names = list(collection.find_one().keys())
        all_pts = list(collection.find().limit(limit))
    else:
        all_pts = None
        column_names = None
        selected_table = None
    return render_template('site.html', column_names=column_names,
                           table_data=all_pts, table_name=selected_table, limit=limit)


if __name__ == '__main__':
    start_time = time.time()
    logger = init_logging()
    app.run(
        host='0.0.0.0',
        debug=True,
        port=5000
    )
    app.config['SQLALCHEMY_DATABASE_URI'] = SQLA_CONFIG_STR
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    db = SQLAlchemy()

    from models import *

    db.init_app(app)

    fin_time = time.time() - start_time
    logger.info(f"Час виконання програми: {fin_time} секунд ({round(fin_time / 60, 2)} хвилин) ")
