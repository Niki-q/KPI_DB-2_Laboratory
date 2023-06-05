from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from mongoengine import connect, Document, StringField, IntField, FloatField
import uuid


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:111@localhost:5432/zno'
db = SQLAlchemy(app)
connect('result_zno')

class EducationalInstitution(db.Model):
    __tablename__ = 'educational_institutions'

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    TypeName = db.Column(db.String)
    AreaName = db.Column(db.String)
    TerName = db.Column(db.String)
    Parent = db.Column(db.String)
    Name = db.Column(db.String)


class EducationalInstitutionMongo(Document):
    _id = IntField()
    TypeName = StringField()
    AreaName = StringField()
    TerName = StringField()
    Parent = StringField()
    Name = StringField()


class Participant(db.Model):
    __tablename__ = 'participants'

    ID = db.Column(db.String(36), primary_key=True, default=str(uuid.uuid4()))
    Birth = db.Column(db.Integer)
    SexTypeName = db.Column(db.String)
    RegName = db.Column(db.String)
    AreaName = db.Column(db.String)
    Ter_id = db.Column(db.Integer, db.ForeignKey('territories.ID'))
    ClassProfileName = db.Column(db.String)
    ClassLangName = db.Column(db.String)
    EO_id = db.Column(db.Integer, db.ForeignKey('educational_institutions.ID'))
    testings = db.relationship("Testing", cascade="delete")


class Territory(db.Model):
    __tablename__ = 'territories'

    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    Name = db.Column(db.String)
    TypeName = db.Column(db.String)
    participants = db.relationship("Participant", cascade="delete")


class PointOfObservation(db.Model):
    __tablename__ = 'points_of_observation'
    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    Name = db.Column(db.String)
    RegName = db.Column(db.String)
    AreaName = db.Column(db.String)
    TerName = db.Column(db.String)
    testings = db.relationship("Testing", cascade="delete")

mongo_client.drop_database(mongo_db)

class Testing(db.Model):
    __tablename__ = 'testings'
    ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    Part_ID = db.Column(db.String(36), db.ForeignKey('participants.ID'))
    Point_ID = db.Column(db.Integer, db.ForeignKey('points_of_observation.ID'))
    Year = db.Column(db.Integer)
    Test = db.Column(db.String)
    TestStatus = db.Column(db.String)
    Ball100 = db.Column(db.Float)
    Ball12 = db.Column(db.Float)
    Ball = db.Column(db.Float)
    participants = db.relationship("Participant", cascade="delete")


class ParticipantMongo(Document):
    _id = StringField()
    Birth = IntField()
    SexTypeName = StringField()
    RegName = StringField()
    AreaName = StringField()
    Ter_id = IntField()
    ClassProfileName = StringField()
    ClassLangName = StringField()
    EO_id = IntField()


class TerritoryMongo(Document):
    _id = IntField()
    Name = StringField()
    TypeName = StringField()


class PointOfObservationMongo(Document):
    _id = IntField()
    Name = StringField()
    RegName = StringField()
    AreaName = StringField()
    TerName = StringField()


class TestingMongo(Document):
    _id = IntField()
    Part_ID = StringField()
    Point_ID = IntField()
    Year = IntField()
    Test = StringField()
    TestStatus = StringField()
    Ball100 = FloatField()
    Ball12 = FloatField()
    Ball = FloatField()



with app.app_context():
    educational_institutions = db.session.query(EducationalInstitution).all()

    for institution in educational_institutions:
        ei = EducationalInstitutionMongo(
            _id=institution.ID,
            TypeName=institution.TypeName,
            AreaName=institution.AreaName,
            TerName=institution.TerName,
            Parent=institution.Parent,
            Name=institution.Name
        )
        ei.save()

    participants = db.session.query(Participant).all()

    for participant in participants:
        p = ParticipantMongo(
            _id=participant.ID,
            Birth=participant.Birth,
            SexTypeName=participant.SexTypeName,
            RegName=participant.RegName,
            AreaName=participant.AreaName,
            Ter_id=participant.Ter_id,
            ClassProfileName=participant.ClassProfileName,
            ClassLangName=participant.ClassLangName,
            EO_id=participant.EO_id
        )
        p.save()

    territories = db.session.query(Territory).all()

    for territory in territories:
        t = TerritoryMongo(
            _id = territory.ID,
            Name=territory.Name,
            TypeName=territory.TypeName
        )
        t.save()

    points = db.session.query(PointOfObservation).all()

    for point in points:
        p = PointOfObservationMongo(
            _id=point.ID,
            Name=point.Name,
            RegName=point.RegName,
            AreaName=point.AreaName,
            TerName=point.TerName
        )
        p.save()

    testings = db.session.query(Testing).all()

    for testing in testings:
        t = TestingMongo(
            _id=testing.ID,
            Part_ID=testing.Part_ID,
            Point_ID=testing.Point_ID,
            Year=testing.Year,
            Test=testing.Test,
            TestStatus=testing.TestStatus,
            Ball100=testing.Ball100,
            Ball12=testing.Ball12,
            Ball=testing.Ball
        )
        t.save()
