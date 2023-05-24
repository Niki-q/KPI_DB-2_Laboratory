import csv
import logging
import os
import time
import uuid

import psycopg2
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from psycopg2 import OperationalError
from sqlalchemy import text

GENERAL_TABLE_NAME = os.getenv("RESULTS_TABLE_NAME")
OUT_CSV_FILE = os.getenv("OUTPUT_FILE_NAME")

DB_CONFIG = {'user': os.getenv("DB_USER"),
             'password': os.getenv("DB_PASSWORD"),
             'host': os.getenv("DB_HOST"),
             'port': os.getenv("DB_PORT"),
             'database': os.getenv("DB_NAME")}


SQLA_CONFIG_STR = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"


def init_logging():
    """
    Инициализирует журналирование (логирование) для текущего модуля с уровнем DEBUG.

    Возвращает:
    Экземпляр логгера для текущего модуля.
    """
    logging.basicConfig(level=logging.DEBUG)
    return logging.getLogger(__name__)


app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = SQLA_CONFIG_STR
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

migrate = Migrate(app, db)


class EducationalInstitution(db.Model):
    __tablename__ = 'educational_institutions'

    ID = db.Column(db.String, primary_key=True)
    TypeName = db.Column(db.String)
    AreaName = db.Column(db.String)
    TerName = db.Column(db.String)
    Parent = db.Column(db.String)
    Name = db.Column(db.String)

    def __init__(self, ID, TypeName, AreaName, TerName, Parent, Name):
        self.ID = ID
        self.TypeName = TypeName
        self.AreaName = AreaName
        self.TerName = TerName
        self.Parent = Parent
        self.Name = Name


class Participant(db.Model):
    __tablename__ = 'participants'

    ID = db.Column(db.String, primary_key=True)
    Birth = db.Column(db.Integer)
    SexTypeName = db.Column(db.String)
    RegName = db.Column(db.String)
    AreaName = db.Column(db.String)
    Ter_id = db.Column(db.String, db.ForeignKey('territories.ID'))
    ClassProfileName = db.Column(db.String)
    ClassLangName = db.Column(db.String)
    EO_id = db.Column(db.String, db.ForeignKey('educational_institutions.ID'))

    def __init__(self, ID, Birth, SexTypeName, RegName, AreaName, Ter_id, ClassProfileName, ClassLangName, EO_id):
        self.ID = ID
        self.Birth = Birth
        self.SexTypeName = SexTypeName
        self.RegName = RegName
        self.AreaName = AreaName
        self.Ter_id = Ter_id
        self.ClassProfileName = ClassProfileName
        self.ClassLangName = ClassLangName
        self.EO_id = EO_id


class Territory(db.Model):
    __tablename__ = 'territories'

    ID = db.Column(db.String, primary_key=True)
    Name = db.Column(db.String)
    TypeName = db.Column(db.String)

    def __init__(self, ID, Name, TypeName):
        self.ID = ID
        self.Name = Name
        self.TypeName = TypeName


class PointOfObservation(db.Model):
    __tablename__ = 'points_of_observation'
    ID = db.Column(db.String, primary_key=True)
    Name = db.Column(db.String)
    RegName = db.Column(db.String)
    AreaName = db.Column(db.String)
    TerName = db.Column(db.String)

    def __init__(self, ID, Name, RegName, AreaName, TerName):
        self.ID = ID
        self.Name = Name
        self.RegName = RegName
        self.AreaName = AreaName
        self.TerName = TerName


class Testing(db.Model):
    __tablename__ = 'testings'
    ID = db.Column(db.String, primary_key=True)
    Part_ID = db.Column(db.String, db.ForeignKey('participants.ID'))
    Point_ID = db.Column(db.String, db.ForeignKey('points_of_observation.ID'))
    Year = db.Column(db.Integer)
    Test = db.Column(db.String)
    Lang = db.Column(db.String)
    TestStatus = db.Column(db.String)
    Ball100 = db.Column(db.Float)
    Ball12 = db.Column(db.Float)
    Ball = db.Column(db.Float)
    AdaptScale = db.Column(db.Float)

    def __init__(self, ID, Part_ID, Point_ID, Year, Test, Lang, TestStatus, Ball100, Ball12, Ball, AdaptScale):
        self.ID = ID
        self.Part_ID = Part_ID
        self.Point_ID = Point_ID
        self.Year = Year
        self.Test = Test
        self.Lang = Lang
        self.TestStatus = TestStatus
        self.Ball100 = Ball100
        self.Ball12 = Ball12
        self.Ball = Ball
        self.AdaptScale = AdaptScale


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


def migrate_data(count_rows:int):
    res_zno_data = db.session.execute(
        text(f'SELECT * FROM {GENERAL_TABLE_NAME} ORDER BY outid ASC LIMIT {count_rows}'))
    columns2 = ["OutID", "Birth", "SEXTYPENAME", "RegName", "AreaName", "TerName", "REGTYPENAME", "TerTypeName",
                "ClassProfileName", "ClassLangName", "EOName", "EOTypeName", "EORegName", "EOAreaName", "EOTerName",
                "EOParent", "UkrTest", "UkrTestStatus", "UkrBall100", "UkrBall12", "UkrBall", "UkrAdaptScale",
                "UkrPTName", "UkrPTRegName",
                "UkrPTAreaName", "UkrPTTerName", "histTest", "histLang", "histTestStatus", "histBall100", "histBall12",
                "histBall", "histPTName",
                "histPTRegName", "histPTAreaName", "histPTTerName", "mathTest", "mathLang", "mathTestStatus",
                "mathBall100", "mathBall12",
                "mathBall", "mathPTName", "mathPTRegName", "mathPTAreaName", "mathPTTerName", "physTest", "physLang",
                "physTestStatus",
                "physBall100", "physBall12", "physBall", "physPTName", "physPTRegName", "physPTAreaName",
                "physPTTerName", "chemTest",
                "chemLang", "chemTestStatus", "chemBall100", "chemBall12", "chemBall", "chemPTName", "chemPTRegName",
                "chemPTAreaName",
                "chemPTTerName", "bioTest", "bioLang", "bioTestStatus", "bioBall100", "bioBall12", "bioBall",
                "bioPTName", "bioPTRegName",
                "bioPTAreaName", "bioPTTerName", "geoTest", "geoLang", "geoTestStatus", "geoBall100", "geoBall12",
                "geoBall", "geoPTName",
                "geoPTRegName", "geoPTAreaName", "geoPTTerName", "engTest", "engTestStatus", "engBall100", "engBall12",
                "engDPALevel",
                "engBall", "engPTName", "engPTRegName", "engPTAreaName", "engPTTerName", "fraTest", "fraTestStatus",
                "fraBall100", "fraBall12",
                "fraDPALevel", "fraBall", "fraPTName", "fraPTRegName", "fraPTAreaName", "fraPTTerName", "deuTest",
                "deuTestStatus", "deuBall100",
                "deuBall12", "deuDPALevel", "deuBall", "deuPTName", "deuPTRegName", "deuPTAreaName", "deuPTTerName",
                "spaTest",
                "spaTestStatus", "spaBall100", "spaBall12", "spaDPALevel", "spaBall", "spaPTName", "spaPTRegName",
                "spaPTAreaName",
                "spaPTTerName", "YEAR"]
    results = [dict(zip(columns2, row)) for row in res_zno_data.fetchall()]

    def generate_unique_id():
        return str(uuid.uuid4())

    def get_model_id(model: db.Model, **kwargs):
        obj = model.query.filter_by(**kwargs).first()
        return obj.ID

    count = 0
    for row in results:
        count += 1
        # Territories

        territory = Territory.query.filter_by(Name=row['TerName'], TypeName=row['TerTypeName']).first()
        if not territory:
            ter_id = generate_unique_id()
            territory = Territory(ID=ter_id, Name=row['TerName'], TypeName=row['TerTypeName'])
            db.session.add(territory)

        # Educational Institutions

        educational_org = EducationalInstitution.query.filter_by(
            TypeName=row['EOTypeName'],
            AreaName=row['EOAreaName'],
            TerName=row['EOTerName'],
            Parent=row['EOParent'],
            Name=row['EOName']
        ).first()
        if not educational_org:
            educational_org = EducationalInstitution(
                ID=generate_unique_id(),
                TypeName=row['EOTypeName'],
                AreaName=row['EOAreaName'],
                TerName=row['EOTerName'],
                Parent=row['EOParent'],
                Name=row['EOName'],
            )
            db.session.add(educational_org)

        # Participants
        participant = Participant(
            ID=generate_unique_id(),
            Birth=row['Birth'],
            SexTypeName=row['SEXTYPENAME'],
            RegName=row['RegName'],
            AreaName=row['AreaName'],
            Ter_id=get_model_id(Territory, Name=row['TerName'], TypeName=row['TerTypeName']),
            ClassProfileName=row['ClassProfileName'],
            ClassLangName=row['ClassLangName'],
            EO_id=get_model_id(EducationalInstitution, TypeName=row['EOTypeName'],
                               AreaName=row['EOAreaName'],
                               TerName=row['EOTerName'],
                               Parent=row['EOParent'],
                               Name=row['EOName'])
        )
        db.session.add(participant)

        # Testings

        def check_subject(subject_name):
            return row[f'{subject_name}Test'] != None

        subjects = ['Ukr', 'hist', 'math', 'phys', 'chem', 'bio', 'geo', 'eng', 'fra', 'deu', 'spa']
        for subject in subjects:
            if check_subject(subject):

                #  Points of observation

                point = PointOfObservation.query.filter_by(Name=row[f"{subject}PTName"],
                                                           RegName=row[f'{subject}PTRegName'],
                                                           AreaName=row[f'{subject}PTAreaName'],
                                                           TerName=row[f"{subject}PTTerName"]).first()
                if not point:
                    point = PointOfObservation(ID=generate_unique_id(),
                                               Name=row[f"{subject}PTName"],
                                               RegName=row[f'{subject}PTRegName'],
                                               AreaName=row[f'{subject}PTAreaName'],
                                               TerName=row[f"{subject}PTTerName"])
                db.session.add(point)

                # перевірка якщо предмет не Українська мова і література - встановити адаптивний поріг у значення NULL
                if subject not in ['Ukr']:
                    adaptscale = None
                else:
                    adaptscale = row[f'{subject}AdaptScale']

                # перевірка якщо предмет є мовним - встановити мову складання у значення NULL
                if subject in ['Ukr', 'fra', 'deu', 'spa', 'eng']:
                    lang = None
                else:
                    lang = row[f'{subject}Lang']

                result = Testing(
                    ID=generate_unique_id(),
                    Part_ID=participant.ID,
                    Point_ID=point.ID,
                    Year=row['YEAR'],
                    Test=row[f'{subject}Test'],
                    Lang=lang,
                    TestStatus=row[f'{subject}TestStatus'],
                    Ball100=row[f'{subject}Ball100'],
                    Ball12=row[f'{subject}Ball12'],
                    Ball=row[f'{subject}Ball'],
                    AdaptScale=adaptscale)

                db.session.add(result)
        db.session.commit()
        # Логування кожних 1000 рядків старих даних
        if count % 1000 == 0:
            logger.info(f'Переміщено {count} рядків')


def execute_query(conn: psycopg2.extensions.connection, query: str, csv_name: str or None = None) -> None:
    """
    Выполняет SQL-запрос в базе данных PostgreSQL, используя соединение conn.
    Результат запроса сохраняет в CSV-файл с именем filename и разделителем ','.

    :param csv_name: путь и имя файла, в который нужно сохранить результат запроса в формате CSV
    :type csv_name: str

    :param conn: соединение с базой данных PostgreSQL
    :type conn: psycopg2.extensions.connection

    :param query: SQL-запрос, который нужно выполнить
    :type query: str

    :return: None
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

    conn = open_conn(DB_CONFIG)
    CLEAN_QUERY = f"""
    ALTER TABLE {GENERAL_TABLE_NAME}
    DROP COLUMN IF EXISTS id;
    """
    TASK_QUERY1 = """
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
    """
    TASK_QUERY2 = """
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
        """

    logger.info("Виконується підготовка до міграції")
    execute_query(conn, CLEAN_QUERY)
    conn.close()

    logger.info("Розпочато міграцію...")
    with app.app_context():
        db.create_all()
        rows = 10000
        migrate_data(count_rows=rows)

        # залишаємо запуск веб додатка для наступної лабораторної
        # app.run(host='0.0.0.0')

    logger.info(f"Міграцію {rows} рядків - Виконано!")

    conn = open_conn(DB_CONFIG)
    logger.info("Виконується запит до завдання з варіантом 2")
    execute_query(conn, TASK_QUERY1, 'results/result_variant_2.csv')

    logger.info("Виконується запит до завдання з варіантом 2 у іншому форматі")
    execute_query(conn, TASK_QUERY2, 'results/result_variant_2_type2.csv')
    conn.close()

    fin_time = time.time() - start_time
    logger.info(f"Час виконання програми: {fin_time} секунд ({round(fin_time/60,2)} хвилин) ")
