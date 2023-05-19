import logging
import os

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

DB_CONFIG = {'user': os.getenv("DB_USER"),
             'password': os.getenv("DB_PASSWORD"),
             'host': os.getenv("DB_HOST"),
             'port': os.getenv("DB_PORT"),
             'database': os.getenv("DB_NAME")}


def init_logging():
    """
    Инициализирует журналирование (логирование) для текущего модуля с уровнем DEBUG.

    Возвращает:
    Экземпляр логгера для текущего модуля.
    """
    logging.basicConfig(level=logging.DEBUG)
    return logging.getLogger(__name__)


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = f"postgresql://" \
                                        f"{DB_CONFIG['user']}" \
                                        f":{DB_CONFIG['password']}" \
                                        f"@{DB_CONFIG['host']}" \
                                        f":{DB_CONFIG['port']}" \
                                        f"/{DB_CONFIG['database']}"

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
migrate = Migrate(app, db)

from models import Testing, Participant, PointOfObservation, Territory, EducationalInstitution


if __name__ == '__main__':
    logger = init_logging()
    logger.info('Test')

    app.run()
