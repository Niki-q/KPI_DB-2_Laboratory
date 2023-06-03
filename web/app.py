import logging
import os
import pickle
import time

import psycopg2
from flask import Flask, redirect, request, render_template
from flask_sqlalchemy import SQLAlchemy
from psycopg2 import OperationalError
from sqlalchemy import func, distinct
import redis
from flask import jsonify

GENERAL_TABLE_NAME = os.getenv("RESULTS_TABLE_NAME")
OUT_CSV_FILE = os.getenv("OUTPUT_FILE_NAME")

DB_CONFIG = {'user': os.getenv("DB_USER"),
             'password': os.getenv("DB_PASSWORD"),
             'host': os.getenv("DB_HOST"),
             'port': os.getenv("DB_PORT"),
             'database': os.getenv("DB_NAME")}

SQLA_CONFIG_STR = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

app = Flask(__name__, template_folder='templates')

redis_host = os.getenv("REDIS_HOST")
redis_port = 6379

redis_client = redis.Redis(host=redis_host, port=redis_port, password=None)

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
    return redirect('/main')


@app.route('/query', methods=['GET'])
def query():
    reg_names = [reg_name[0] for reg_name in db.session.query(Participant.RegName).distinct().all()]
    tests = [test[0] for test in db.session.query(Testing.Test).distinct().all()]
    average_scores = False

    return render_template('query.html', reg_names=reg_names, tests=tests, average_scores=average_scores)


@app.route('/query', methods=['POST'])
def execute_query():
    reg_name = request.form.get('reg_name')
    test_name = request.form.get('test_name')
    year = request.form.getlist('year')

    arguments = {'selected_reg_name': reg_name, 'selected_test_name': test_name, 'selected_year': year}

    key = reg_name + test_name + year

    # Отримання унікальних років з бази даних
    year_options = [str(y[0]) for y in db.session.query(Testing.Year).distinct().all()]
    reg_names = [reg_name[0] for reg_name in db.session.query(Participant.RegName).distinct().all()]
    tests = [test[0] for test in db.session.query(Testing.Test).distinct().all()]

    if average_scores := redis_client.get(str(key)):
        average_scores = pickle.loads(average_scores)
        return render_template('query.html', reg_names=reg_names, tests=tests, year_options=year_options,
                               average_scores=average_scores, reg_name=reg_name, year=year, **arguments)
    else:
        cache = False

    # Обработка отправки формы и выполнение необходимых действий

    if reg_name == 'all':
        # Виконати запит до бази даних для всіх регіонів і років
        query = db.session.query(
            Participant.RegName,
            Testing.Year,
            func.avg(Testing.Ball)
        ).join(Testing, Participant.ID == Testing.Part_ID). \
            filter(Testing.TestStatus == 'Зараховано',
                   Testing.Test == test_name)

        if 'all' not in year:
            query = query.filter(Testing.Year.in_([int(y) for y in year]))

        query_result = query.group_by(Participant.RegName, Testing.Year).all()

        average_scores = {f"{reg}-{y}": avg_score for reg, y, avg_score in query_result}
        if not cache:
            redis_client.set(str(key), pickle.dumps(average_scores))

        return render_template('query.html', reg_names=reg_names, tests=tests, year_options=year_options,
                               average_scores=average_scores, reg_name=reg_name, year=year, **arguments)
    else:
        # Виконати запит до бази даних для конкретного регіону і року
        average_scores = {}

        if 'all' in year:
            query = db.session.query(
                Testing.Year,
                func.avg(Testing.Ball)
            ).join(Participant, Participant.ID == Testing.Part_ID). \
                filter(Participant.RegName == reg_name,
                       Testing.Test == test_name,
                       Testing.TestStatus == 'Зараховано'). \
                group_by(Testing.Year)

            query_result = query.all()
            average_scores = {f"{reg_name}-{y}": avg_score for y, avg_score in query_result}
            if not cache:
                redis_client.set(str(key), pickle.dumps(average_scores))
            return render_template('query.html', reg_names=reg_names, tests=tests, year_options=year_options,
                                   average_scores=average_scores, reg_name=reg_name, year=year, **arguments)
        else:
            for y in year:
                average_score = db.session.query(
                    func.avg(Testing.Ball)
                ).join(Participant, Participant.ID == Testing.Part_ID). \
                    filter(Participant.RegName == reg_name,
                           Testing.Test == test_name,
                           Testing.TestStatus == 'Зараховано',
                           Testing.Year == int(y))

                average_score = average_score.scalar()

                average_scores[f"{reg_name}-{y}"] = average_score
            if not cache:
                redis_client.set(str(key), pickle.dumps(average_scores))
            return render_template('query.html', reg_names=reg_names, tests=tests, year_options=year_options,
                                   average_scores=average_scores, average_score=average_score, reg_name=reg_name,
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

    model_class = get_model_class(table_name)

    if model_class is None:
        return 'Table not found'

    row = model_class.query.get(row_id)

    if row is None:
        return 'Row not found'

    # Оновити дані рядка
    for column, value in updated_data.items():
        setattr(row, column, value)

    # Зберегти зміни у базі даних
    db.session.commit()
    selected_table = table_name
    limit = 10
    column_names, table_data = get_table_data(selected_table, limit)
    return render_template('site.html', column_names=column_names, table_data=table_data, table_name=selected_table)


@app.route('/add_row', methods=['GET'])
def add_row():
    return render_template('add_row.html')


def get_distinct_values(table_name, column_name):
    YourTableName = get_table_model(table_name)
    distinct_values = db.session.query(distinct(getattr(YourTableName, column_name)).label('count')).all()

    return distinct_values

@app.route('/add_rows', methods=['POST'])
def add_rows():
    table_name = request.form['table_name']
    updated_data = {column: request.form.get(column) for column in request.form if
                    column != 'table_name' and column != 'row_id'}
    model_class = get_model_class(table_name)
    row = model_class(**updated_data)  # Створення нового об'єкта рядка
    db.session.add(row)  # Додавання рядка до сесії ORM
    db.session.commit()
    return "42"

@app.route('/upgate_row', methods=['POST'])
def upgate_row():
    table_name = request.form['table-select']
    column_names, table_data = get_table_data(table_name, 1)
    excluded_column = ['ID']

    for column in excluded_column:
        if column in column_names:
            column_names.remove(column)
    result = {}
    special_colum = []
    for i in column_names:
        temp = get_distinct_values(table_name,i)
        if len(temp)<=25:
            result[i]=[temp]
            special_colum.append(i)
    print(result)
    res=get_distinct_values(table_name,column_names[1])
    print(special_colum)
    return render_template('add_row.html', selected_table=table_name, columns=column_names, special_colum=special_colum, result=result)


@app.route('/upgrade', methods=['POST'])
def upgrade():
    row_id = request.form['row_id']
    table_name = request.form['table_name']

    table_model = get_table_model(table_name)
    row = table_model.query.get(row_id)

    # Отримання значень з форми редагування та оновлення даних рядка
    for column in row.__table__.columns:
        column_name = column.name
        new_value = request.form.get(column_name)
        setattr(row, column_name, new_value)

    # Збереження оновленого рядка в базі даних
    db.session.commit()

    return render_template('edit_row.html', row=row, table_name=table_name, row_id=row_id)


@app.route('/delete_row', methods=['POST'])
def delete_row():
    row_id = request.form['row_id']
    table_name = request.form['table_name']

    table_model = get_table_model(table_name)
    row = table_model.query.get(row_id)

    db.session.delete(row)
    db.session.commit()

    return redirect('/main')


@app.route('/search', methods=['POST'])
def search():
    table_search = request.form['table_search']
    column_search = request.form['column_search']
    value_search = request.form['value_search']

    model_class = get_model_class(table_search)
    table_data = db.session.query(model_class).filter(getattr(model_class, column_search) == value_search).limit(
        20).all()

    column_names = [column.name for column in model_class.__table__.columns]

    return render_template('site.html', table_data=table_data, table_name=table_search, column_names=column_names,
                           search_data=[table_search, column_search, value_search], limit=10)




@app.route('/get_columns', methods=['POST'])
def get_columns():
    table = request.json['table']
    columns, _ = get_table_data(table, 1)

    return jsonify(columns)


@app.route('/main', methods=['GET', 'POST'])
def table(limit=10):
    if request.method == 'POST':
        selected_table = request.form['table']
        limit = int(request.form['limit'])
        column_names, table_data = get_table_data(selected_table, limit)
    else:
        column_names = None
        table_data = None
        selected_table = None
    return render_template('site.html', column_names=column_names,
                           table_data=table_data, table_name=selected_table, limit=limit)


if __name__ == '__main__':
    app.config['SQLALCHEMY_DATABASE_URI'] = SQLA_CONFIG_STR
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    db = SQLAlchemy()

    from models import *

    db.init_app(app)

    start_time = time.time()
    logger = init_logging()
    app.run(
        host='0.0.0.0',
        debug=True,
        port=5000
    )
    fin_time = time.time() - start_time
    logger.info(f"Час виконання програми: {fin_time} секунд ({round(fin_time / 60, 2)} хвилин) ")
