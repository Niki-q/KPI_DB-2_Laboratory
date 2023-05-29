import uuid

from flask_sqlalchemy import SQLAlchemy

# from app import app

db = SQLAlchemy()


class EducationalInstitution(db.Model):
    __tablename__ = 'educational_institutions'

    ID = db.Column(db.String, primary_key=True)
    TypeName = db.Column(db.String)
    AreaName = db.Column(db.String)
    TerName = db.Column(db.String)
    Parent = db.Column(db.String)
    Name = db.Column(db.String)


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


class Territory(db.Model):
    __tablename__ = 'territories'

    ID = db.Column(db.String, primary_key=True)
    Name = db.Column(db.String)
    TypeName = db.Column(db.String)


class PointOfObservation(db.Model):
    __tablename__ = 'points_of_observation'
    ID = db.Column(db.String, primary_key=True)
    Name = db.Column(db.String)
    RegName = db.Column(db.String)
    AreaName = db.Column(db.String)
    TerName = db.Column(db.String)


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
