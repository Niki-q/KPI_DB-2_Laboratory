"""fill

Revision ID: 73b4be4a60b9
Revises: 8bcefa649a8b
Create Date: 2023-05-29 01:26:51.362837

"""
import logging
import os
import time
import uuid

import sqlalchemy
from alembic import op
import sqlalchemy as sa
from sqlalchemy import Column, Integer, String, and_, union_all, union, or_
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import insert

# revision identifiers, used by Alembic.
revision = '73b4be4a60b9'
down_revision = '8bcefa649a8b'
branch_labels = None
depends_on = None


def init_logging():
    """
    Инициализирует журналирование (логирование) для текущего модуля с уровнем DEBUG.

    Возвращает:
    Экземпляр логгера для текущего модуля.
    """
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[
                            logging.StreamHandler(),  # Обработчик для вывода в консоль
                            logging.FileHandler('migration.log')  # Обработчик для вывода в файл
                        ])

    return logging.getLogger(__name__)


def upgrade():
    GENERAL_TABLE_NAME = os.getenv("RESULTS_TABLE_NAME")
    start_time = time.time()
    # logger = init_logging()
    logger = logging.getLogger('alembic.env')

    db = SQLAlchemy()
    ### Data migration
    metadata = sa.MetaData()
    metadata.reflect(bind=op.get_bind())

    def generate_unique_id():
        return str(uuid.uuid4())

    class ZNOResult(db.Model):
        __table__ = metadata.tables[GENERAL_TABLE_NAME]
        # outid = Column(Integer, primary_key=True)  # Определение первичного ключа

    class Meducational_institutions(db.Model):
        __table__ = metadata.tables["educational_institutions"]
        # id = Column(Integer, primary_key=True)  # Определение первичного ключа

    class Mteritory(db.Model):
        __table__ = metadata.tables["territories"]
        # id = Column(Integer, primary_key=True)  # Определение первичного ключа

    class Mparticipants(db.Model):
        __table__ = metadata.tables["participants"]
        # id = Column(Integer, primary_key=True)  # Определение первичного ключа

    class Mpoints_of_observation(db.Model):
        __table__ = metadata.tables["points_of_observation"]
        # id = Column(Integer, primary_key=True)  # Определение первичного ключа

    class Mtestings(db.Model):
        __table__ = metadata.tables["testings"]
        # id = Column(Integer, primary_key=True)  # Определение первичного ключа

    def get_select(param, arg_return=False):
        args = []
        for old_name, new_name in param:
            args.append(ZNOResult.__table__.c[f'{old_name}'].label(f'{new_name}'))
        if arg_return:
            return args
        return sa.select(*args).distinct()

    def fill_table(Model, column_params, query=None):
        if query is None:
            query_dict = op.get_bind().execute(get_select(column_params))
        else:
            query_dict = op.get_bind().execute(query)
        for i, p in enumerate(query_dict.mappings().partitions(100000)):
            logger.info(f"Migrating {Model.__table__} [~ {i * 100000}]")
            op.bulk_insert(Model.__table__, list(map(dict, p)))

    # teritory
    logger.info("Migrating Territory")
    column_params = [('tername', 'Name'), ('tertypename', 'TypeName')]
    fill_table(Mteritory, column_params)


    # educational_institutions
    logger.info("Migrating educational_institutions")
    column_params = [('eotypename', 'TypeName'), ('eoregname', 'RegName'), ('eoareaname', 'AreaName'),
                     ('eotername', 'TerName'), ('eoparent', 'Parent'), ('eoname', 'Name')]
    fill_table(Meducational_institutions, column_params)


    # participants
    logger.info("Migrating participants")
    column_params = [('outid', 'ID'), ('birth', 'Birth'), ('sextypename', 'SexTypeName'), ('areaname', 'AreaName'),
                     ('regname', 'RegName'), ('classprofilename', 'ClassProfileName'),
                     ('classlangname', 'ClassLangName')]
    q_select = sa.select(*get_select(column_params, arg_return=True),
                         Mteritory.ID.label("Ter_id"),
                         Meducational_institutions.ID.label("EO_id")) \
        .join(Mteritory, Mteritory.Name == ZNOResult.tername) \
        .join(Meducational_institutions, and_(Meducational_institutions.Name == ZNOResult.eoname,
                                              Meducational_institutions.Parent == ZNOResult.eoparent)).distinct()
    fill_table(Mparticipants, column_params, q_select)


    # testings and points_of_observation
    logger.info("Migrating Points of observation")

    test_column_params = [('test', 'Test'), ('teststatus', 'TestStatus'),
                          ('ball100', 'Ball100'), ('ball12', 'Ball12'), ('ball', 'Ball')]
    subjects = ['ukr', 'fra', 'deu', 'spa', 'eng', "math", "hist", "phys", "chem", "bio", "geo"]

    boole = {'ukr': Mpoints_of_observation.Name == ZNOResult.ukrptname,
             'math': Mpoints_of_observation.Name == ZNOResult.mathptname,
             'fra': Mpoints_of_observation.Name == ZNOResult.fraptname,
             'deu': Mpoints_of_observation.Name == ZNOResult.deuptname,
             'spa': Mpoints_of_observation.Name == ZNOResult.spaptname,
             'hist': Mpoints_of_observation.Name == ZNOResult.histptname,
             'phys': Mpoints_of_observation.Name == ZNOResult.physptname,
             'chem': Mpoints_of_observation.Name == ZNOResult.chemptname,
             'bio': Mpoints_of_observation.Name == ZNOResult.bioptname,
             'geo': Mpoints_of_observation.Name == ZNOResult.geoptname,
             'eng': Mpoints_of_observation.Name == ZNOResult.engptname
             }

    def get_subject_column_params(subject, columns):
        column_params = []
        lang_subjects = ['ukr', 'fra', 'deu', 'spa', 'eng']
        if subject in lang_subjects and 'Lang' in columns:
            columns.remove('Lang')
        if subject != 'ukr' and 'AdaptScale' in columns:
            columns.remove('AdaptScale')
        if 'Test' in columns:
            for column in columns:
                column_params.append((f"{subject}{column.lower()}", column))
            column_params.append(("year", "Year"))
        else:
            for column in columns:
                column_params.append((f"{subject}{column.lower()}", column[2:]))

        return column_params

    def get_test_select():
        lang_subjects = ['fra', 'deu', 'spa', 'eng']
        other_subjects = ["math", "hist", "phys", "chem", "bio", "geo"]

        other_test = []
        lang_test = []

        pt_list = []

        columns = ['Test', 'Lang', 'TestStatus', 'Ball', 'Ball12', 'Ball100', 'AdaptScale']
        pt_columns = ['PTName', 'PTRegName', 'PTAreaName', 'PTTerName']

        for subject in other_subjects:
            other_test.append(get_select(get_subject_column_params(subject, columns), arg_return=True))
            pt_list.append(get_select(get_subject_column_params(subject, pt_columns)))

        for subject in lang_subjects:
            lang_test.append(get_select(get_subject_column_params(subject, columns), arg_return=True))
            pt_list.append(get_select(get_subject_column_params(subject, pt_columns)))

        pt_list.append(get_select(get_subject_column_params('ukr', pt_columns)))

        return {'test': {
            'other': (other_test),
            'lang': (lang_test),
            'ukr': get_select(get_subject_column_params('ukr', columns), arg_return=True)},
            'pt': union(*pt_list)
        }

    test_query = get_test_select()

    fill_table(Mpoints_of_observation, [], query=test_query['pt'])


    logger.info("Migrating Testings")

    for subject in subjects:
        new_test_column_params = []
        for old_name, new_name in test_column_params:
            new_test_column_params.append((f"{subject}{old_name}", new_name))
        new_test_column_params.append(('year', 'Year'))

        logger.info(f'Filling subject - {subject}')

        q_select = sa.select(*get_select(new_test_column_params, arg_return=True),
                             Mpoints_of_observation.ID.label("Point_ID"),
                             Mparticipants.ID.label("Part_ID")) \
            .join(Mpoints_of_observation, boole[subject]) \
            .join(Mparticipants, Mparticipants.ID == ZNOResult.outid).distinct()
        fill_table(Mtestings, new_test_column_params, q_select)


    fin_time = time.time() - start_time
    logger.info(f"Migrated: {fin_time} seconds ({round(fin_time / 60, 2)} minutes) ")

def downgrade():
    op.drop_table('testings')
    op.drop_table('participants')
    op.drop_table('territories')
    op.drop_table('points_of_observation')
    op.drop_table('educational_institutions')
