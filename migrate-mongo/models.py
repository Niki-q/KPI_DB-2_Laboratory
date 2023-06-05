from mongoengine import Document, StringField, IntField, FloatField
from flask_sqlalchemy import SQLAlchemy
import uuid
db = SQLAlchemy()
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
