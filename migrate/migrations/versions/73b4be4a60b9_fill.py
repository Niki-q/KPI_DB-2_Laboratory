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
from sqlalchemy import Column, Integer, String, and_
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
    logging.basicConfig(level=logging.DEBUG)
    return logging.getLogger(__name__)


def upgrade():
    GENERAL_TABLE_NAME = os.getenv("RESULTS_TABLE_NAME")
    start_time = time.time()
    logger = init_logging()
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

    def get_select(param, arg_return = False):
        args = []
        for old_name, new_name in param:
            args.append(ZNOResult.__table__.c[f'{old_name}'].label(f'{new_name}'))
        if arg_return:
            return args
        return sa.select(*args).distinct()

    def fiil_table(Model, column_params, query = None):
        if query is None:
            query_dict = op.get_bind().execute(get_select(column_params))
        else:
            query_dict = op.get_bind().execute(query)

        for i, p in enumerate(query_dict.mappings().partitions(100000)):
            logger.info(f"Migrating {Model.__table__} [~ {i * 100000}]")
            op.bulk_insert(Model.__table__, list(map(dict, p)))


    # teritory
    print("Migrating Territory")
    column_params = [('tername', 'Name'), ('tertypename', 'TypeName')]
    fiil_table(Mteritory, column_params)

    fin_time = time.time() - start_time
    print(f"Migrated: {fin_time} seconds ({round(fin_time / 60, 2)} minutes) ")

    #educational_institutions
    print("Migrating educational_institutions")
    column_params = [('eotypename', 'TypeName'), ('eoregname', 'RegName'), ('eoareaname', 'AreaName'), ('eotername', 'TerName'), ('eoparent', 'Parent'), ('eoname', 'Name')]
    fiil_table(Meducational_institutions, column_params)

    fin_time = time.time() - start_time
    print(f"Migrated: {fin_time} seconds ({round(fin_time / 60, 2)} minutes) ")

    # participants
    print("Migrating participants")
    column_params = [('outid', 'ID'), ('birth', 'Birth'), ('sextypename', 'SexTypeName'), ('areaname', 'AreaName'),('regname', 'RegName'), ('classprofilename', 'ClassProfileName'), ('classlangname', 'ClassLangName')]
    q_select = sa.select(*get_select(column_params, arg_return=True),
        Mteritory.ID.label("Ter_id"),
        Meducational_institutions.ID.label("EO_id"))\
        .join(Mteritory, Mteritory.Name == ZNOResult.tername)\
        .join(Meducational_institutions, and_(Meducational_institutions.Name == ZNOResult.eoname, Meducational_institutions.Parent == ZNOResult.eoparent)).distinct()
    fiil_table(Mparticipants,column_params, q_select)

    fin_time = time.time() - start_time
    print(f"Migrated: {fin_time} seconds ({round(fin_time / 60, 2)} minutes) ")





def downgrade():
    op.drop_table('testings')
    op.drop_table('participants')
    op.drop_table('territories')
    op.drop_table('points_of_observation')
    op.drop_table('educational_institutions')
