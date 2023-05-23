import logging
import os
import time
import uuid

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from sqlalchemy import text

DB_CONFIG = {'user': os.getenv("DB_USER"),
             'password': os.getenv("DB_PASSWORD"),
             'host': os.getenv("DB_HOST"),
             'port': os.getenv("DB_PORT"),
             'database': os.getenv("DB_NAME")}

SQLA_CONFIG_STR = f"postgresql://"f"" \
                  f"{DB_CONFIG['user']}" \
                  f":{DB_CONFIG['password']}" \
                  f"@{DB_CONFIG['host']}" \
                  f":{DB_CONFIG['port']}" \
                  f"/{DB_CONFIG['database']}"

new_SQLA_CONFIG_STR = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"


def init_logging():
    """
    Инициализирует журналирование (логирование) для текущего модуля с уровнем DEBUG.

    Возвращает:
    Экземпляр логгера для текущего модуля.
    """
    logging.basicConfig(level=logging.DEBUG)
    return logging.getLogger(__name__)


app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = new_SQLA_CONFIG_STR
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

migrate = Migrate(app, db)


# Модель таблиці "testings"

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
    RegName = db.Column(db.String)
    AreaName = db.Column(db.String)
    Ter_id = db.Column(db.String, db.ForeignKey('territories.ID'))
    ClassProfileName = db.Column(db.String)
    ClassLangName = db.Column(db.String)
    EO_id = db.Column(db.String, db.ForeignKey('educational_institutions.ID'))

    def __init__(self, ID, Birth, RegName, AreaName, Ter_id, ClassProfileName, ClassLangName, EO_id):
        self.ID = ID
        self.Birth = Birth
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


# class OldData(db.Model):
#     __tablename__ = 'zno_result'
#
#     id = db.Column(db.Integer, primary_key=True)
#     outid = db.Column(db.String(255))
#     birth = db.Column(db.Integer)
#     sextypename = db.Column(db.String(255))
#     regname = db.Column(db.String(255))
#     areaname = db.Column(db.String(255))
#     tername = db.Column(db.String(255))
#     regtypename = db.Column(db.String(255))
#     tertypename = db.Column(db.String(255))
#     classprofilename = db.Column(db.String(255))
#     classlangname = db.Column(db.String(255))
#     eoname = db.Column(db.String(255))
#     eotypename = db.Column(db.String(255))
#     eoregname = db.Column(db.String(255))
#     eoareaname = db.Column(db.String(255))
#     eotername = db.Column(db.String(255))
#     eoparent = db.Column(db.String(255))
# ukrtest = db.Column(db.String(255))
# ukrteststatus = db.Column(db.String(255))
# ukrball100 = db.Column(db.Float)
# ukrball12 = db.Column(db.Float)
# ukrball = db.Column(db.Float)
# ukradaptscale = db.Column(db.String(255))
# ukrptname = db.Column(db.String(255))
# ukrptregname = db.Column(db.String(255))
# ukrptareaname = db.Column(db.String(255))
# ukrpttername = db.Column(db.String(255))
# histtest = db.Column(db.String(255))
# histlang = db.Column(db.String(255))
# histteststatus = db.Column(db.String(255))
# histball100 = db.Column(db.Float)
# histball12 = db.Column(db.Float)
# histball = db.Column(db.Float)
# histptname = db.Column(db.String(255))
# histptregname = db.Column(db.String(255))
# histptareaname = db.Column(db.String(255))
# histpttername = db.Column(db.String(255))
# mathtest = db.Column(db.String(255))
# mathlang = db.Column(db.String(255))
# mathteststatus = db.Column(db.String(255))
# mathball100 = db.Column(db.Float)
# mathball12 = db.Column(db.Float)
# mathball = db.Column(db.Float)
# mathptname = db.Column(db.String(255))
# mathptregname = db.Column(db.String(255))
# mathptareaname = db.Column(db.String(255))
# mathpttername = db.Column(db.String(255))
# phystest = db.Column(db.String(255))
# physlang = db.Column(db.String(255))
# physteststatus = db.Column(db.String(255))
# physball100 = db.Column(db.Float)
# physball12 = db.Column(db.Float)
# physball = db.Column(db.Float)
# physptname = db.Column(db.String(255))
# physptregname = db.Column(db.String(255))
# physptareaname = db.Column(db.String(255))
# physpttername = db.Column(db.String(255))
# chemtest = db.Column(db.String(255))
# chemlang = db.Column(db.String(255))
# chemteststatus = db.Column(db.String(255))
# chemball100 = db.Column(db.Float)
# chemball12 = db.Column(db.Float)
# chemball = db.Column(db.Float)
# chemptname = db.Column(db.String(255))
# chemptregname = db.Column(db.String(255))
# chemptareaname = db.Column(db.String(255))
# chempttername = db.Column(db.String(255))
# biotest = db.Column(db.String(255))
# biolang = db.Column(db.String(255))
# bioteststatus = db.Column(db.String(255))
# bioball100 = db.Column(db.Float)
# bioball12 = db.Column(db.Float)
# bioball = db.Column(db.Float)
# bioptname = db.Column(db.String(255))
# bioptregname = db.Column(db.String(255))
# bioptareaname = db.Column(db.String(255))
# biopttername = db.Column(db.String(255))
# geotest = db.Column(db.String(255))
# geolang = db.Column(db.String(255))
# geoteststatus = db.Column(db.String(255))
# geoball100 = db.Column(db.Float)
# geoball12 = db.Column(db.Float)
# geoball = db.Column(db.Float)
# geoptname = db.Column(db.String(255))
# geoptregname = db.Column(db.String(255))
# geoptareaname = db.Column(db.String(255))
# geopttername = db.Column(db.String(255))
# engtest = db.Column(db.String(255))
# engteststatus = db.Column(db.String(255))
# engball100 = db.Column(db.Float)
# engball12 = db.Column(db.Float)
# engdpalevel = db.Column(db.String(255))
# engball = db.Column(db.Float)
# engptname = db.Column(db.String(255))
# engptregname = db.Column(db.String(255))
# engptareaname = db.Column(db.String(255))
# engpttername = db.Column(db.String(255))
# fratest = db.Column(db.String(255))
# frateststatus = db.Column(db.String(255))
# fraball100 = db.Column(db.Float)
# fraball12 = db.Column(db.Float)
# fradpalevel = db.Column(db.String(255))
# fraball = db.Column(db.Float)
# fraptname = db.Column(db.String(255))
# fraptregname = db.Column(db.String(255))
# fraptareaname = db.Column(db.String(255))
# frapttername = db.Column(db.String(255))
# deutest = db.Column(db.String(255))
# deuteststatus = db.Column(db.String(255))
# deuball100 = db.Column(db.Float)
# deuball12 = db.Column(db.Float)
# deudpalevel = db.Column(db.String(255))
# deuball = db.Column(db.Float)
# deuptname = db.Column(db.String(255))
# deuptregname = db.Column(db.String(255))
# deuptareaname = db.Column(db.String(255))
# deupttername = db.Column(db.String(255))
# spatest = db.Column(db.String(255))
# spateststatus = db.Column(db.String(255))
# spaball100 = db.Column(db.Float)
# spaball12 = db.Column(db.Float)
# spadpalevel = db.Column(db.String(255))
# spaball = db.Column(db.Float)
# spaptname = db.Column(db.String(255))
# spaptregname = db.Column(db.String(255))
# spaptareaname = db.Column(db.String(255))
# spapttername = db.Column(db.String(255))
# year = db.Column(db.Integer)

def migrate_data():
    res_zno_data = db.session.execute(
        text(f'SELECT * FROM public.{os.getenv("RESULTS_TABLE_NAME")} ORDER BY outid ASC LIMIT 10000'))
    columns = res_zno_data.keys()
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

                if subject not in ['Ukr']:
                    adaptscale = None
                else:
                    adaptscale = row[f'{subject}AdaptScale']

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
        if count % 1000 == 0:
            logger.info(f'Migrated {count} lines')


if __name__ == '__main__':
    start_time = time.time()
    logger = init_logging()
    with app.app_context():
        db.create_all()
        migrate_data()
        # app.run(host='0.0.0.0')

    end_time = time.time()
    logger.info("Program running time: %s seconds" % (end_time - start_time))
