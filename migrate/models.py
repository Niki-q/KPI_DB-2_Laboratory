from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


# Модель таблиці "testings"
class Testing(db.Model):
    __tablename__ = 'testings'

    ID = db.Column(db.Integer, primary_key=True)
    Part_ID = db.Column(db.Integer, db.ForeignKey('participants.ID'))
    Point_ID = db.Column(db.Integer, db.ForeignKey('points_of_observation.ID'))
    Year = db.Column(db.Integer)
    Test = db.Column(db.String)
    Lang = db.Column(db.String)
    TestStatus = db.Column(db.String)
    Ball100 = db.Column(db.Integer)
    Ball12 = db.Column(db.Integer)
    Ball = db.Column(db.Integer)
    AdaptScale = db.Column(db.Integer)

    def __init__(self, Part_ID, Point_ID, Year, Test, Lang, TestStatus, Ball100, Ball12, Ball, AdaptScale):
        self.Part_ID = Part_ID
        self.point_ID = Point_ID
        self.year = Year
        self.Test = Test
        self.Lang = Lang
        self.TestStatus = TestStatus
        self.Ball100 = Ball100
        self.Ball12 = Ball12
        self.Ball = Ball
        self.AdaptScale = AdaptScale


# Модель таблиці "participants"
class Participant(db.Model):
    __tablename__ = 'participants'

    ID = db.Column(db.Integer, primary_key=True)
    Birth = db.Column(db.Integer)
    RegName = db.Column(db.String)
    AreaName = db.Column(db.String)
    Ter_id = db.Column(db.Integer, db.ForeignKey('territories.ID'))
    ClassProfileName = db.Column(db.String)
    ClassLangName = db.Column(db.String)
    EO_id = db.Column(db.Integer, db.ForeignKey('educational_institutions.ID'))

    def __init__(self, Birth, RegName, AreaName, Ter_id, ClassProfileName, ClassLangName, EO_id):
        self.Birth = Birth
        self.RegName = RegName
        self.AreaName = AreaName
        self.Ter_id = Ter_id
        self.ClassProfileName = ClassProfileName
        self.ClassLangName = ClassLangName
        self.EO_id = EO_id


# Модель таблиці "points_of_observation"
class PointOfObservation(db.Model):
    __tablename__ = 'points_of_observation'
    ID = db.Column(db.Integer, primary_key=True)
    Name = db.Column(db.String)
    RegName = db.Column(db.String)
    AreaName = db.Column(db.String)
    TerName = db.Column(db.String)

    def __init__(self, Name, RegName, AreaName, TerName):
        self.Name = Name
        self.RegName = RegName
        self.AreaName = AreaName
        self.TerName = TerName


# Модель таблиці "territories"
class Territory(db.Model):
    __tablename__ = 'territories'

    ID = db.Column(db.Integer, primary_key=True)
    Name = db.Column(db.String)
    TypeName = db.Column(db.String)

    def __init__(self, Name, TypeName):
        self.Name = Name
        self.TypeName = TypeName


# Модель таблиці "educational_institutions"
class EducationalInstitution(db.Model):
    __tablename__ = 'educational_institutions'

    ID = db.Column(db.Integer, primary_key=True)
    TypeName = db.Column(db.String)
    AreaName = db.Column(db.String)
    TerName = db.Column(db.String)
    Parent = db.Column(db.String)
    Name = db.Column(db.String)

    def __init__(self, TypeName, AreaName, TerName, Parent, Name):
        self.TypeName = TypeName
        self.AreaName = AreaName
        self.TerName = TerName
        self.Parent = Parent
        self.Name = Name
