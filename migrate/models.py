import os

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


class OldData(db.Model):
    __tablename__ = os.getenv("RESULTS_TABLE_NAME")

    id = db.Column(db.Integer, primary_key=True)
    OUTID = db.Column(db.String(100))
    Birth = db.Column(db.Integer)
    SEXTYPENAME = db.Column(db.String(100))
    REGNAME = db.Column(db.String(100))
    AREANAME = db.Column(db.String(100))
    TERNAME = db.Column(db.String(100))
    REGTYPENAME = db.Column(db.String(100))
    TerTypeName = db.Column(db.String(100))
    year = db.Column(db.Integer)
    ClassProfileNAME = db.Column(db.String(100))
    ClassLangName = db.Column(db.String(100))
    EONAME = db.Column(db.String(100))
    EOTYPENAME = db.Column(db.String(100))
    EOREGNAME = db.Column(db.String(100))
    EOAreaName = db.Column(db.String(100))
    EOTerName = db.Column(db.String(100))
    EOParent = db.Column(db.String(100))
    UkrTest = db.Column(db.String(100))
    UkrTestStatus = db.Column(db.String(100))
    UkrBall100 = db.Column(db.Integer)
    UkrBall12 = db.Column(db.Integer)
    UkrBall = db.Column(db.Integer)
    UkrAdaptScale = db.Column(db.Integer)
    UkrPTName = db.Column(db.String(100))
    UkrPTRegName = db.Column(db.String(100))
    UkrPTAreaName = db.Column(db.String(100))
    UkrPTTerName = db.Column(db.String(100))
    histTest = db.Column(db.String(100))
    HistLang = db.Column(db.String(100))
    histTestStatus = db.Column(db.String(100))
    histBall100 = db.Column(db.Integer)
    histBall12 = db.Column(db.Integer)
    histBall = db.Column(db.Integer)
    histPTName = db.Column(db.String(100))
    histPTRegName = db.Column(db.String(100))
    histPTAreaName = db.Column(db.String(100))
    histPTTerName = db.Column(db.String(100))
    mathTest = db.Column(db.String(100))
    mathLang = db.Column(db.String(100))
    mathTestStatus = db.Column(db.String(100))
    mathBall100 = db.Column(db.Integer)
    mathBall12 = db.Column(db.Integer)
    mathBall = db.Column(db.Integer)
    mathPTName = db.Column(db.String(100))
    mathPTRegName = db.Column(db.String(100))
    mathPTAreaName = db.Column(db.String(100))
    mathPTTerName = db.Column(db.String(100))
    physTest = db.Column(db.String(100))
    physLang = db.Column(db.String(100))
    physTestStatus = db.Column(db.String(100))
    physBall100 = db.Column(db.Integer)
    physBall12 = db.Column(db.Integer)
    physBall = db.Column(db.Integer)
    physPTName = db.Column(db.String(100))
    physPTRegName = db.Column(db.String(100))
    physPTAreaName = db.Column(db.String(100))
    physPTTerName = db.Column(db.String(100))
    chemTest = db.Column(db.String(100))
    chemLang = db.Column(db.String(100))
    chemTestStatus = db.Column(db.String(100))
    chemBall100 = db.Column(db.Integer)
    chemBall12 = db.Column(db.Integer)
    chemBall = db.Column(db.Integer)
    chemPTName = db.Column(db.String(100))
    chemPTRegName = db.Column(db.String(100))
    chemPTAreaName = db.Column(db.String(100))
    chemPTTerName = db.Column(db.String(100))
    bioTest = db.Column(db.String(100))
    bioLang = db.Column(db.String(100))
    bioTestStatus = db.Column(db.String(100))
    bioBall100 = db.Column(db.Integer)
    bioBall12 = db.Column(db.Integer)
    bioBall = db.Column(db.Integer)
    bioPTName = db.Column(db.String(100))
    bioPTRegName = db.Column(db.String(100))
    bioPTAreaName = db.Column(db.String(100))
    bioPTTerName = db.Column(db.String(100))
    geoTest = db.Column(db.String(100))
    geoLang = db.Column(db.String(100))
    geoTestStatus = db.Column(db.String(100))
    geoBall100 = db.Column(db.Integer)
    geoBall12 = db.Column(db.Integer)
    geoBall = db.Column(db.Integer)
    geoPTName = db.Column(db.String(100))
    geoPTRegName = db.Column(db.String(100))
    geoPTAreaName = db.Column(db.String(100))
    geoPTTerName = db.Column(db.String(100))
    engTest = db.Column(db.String(100))
    engTestStatus = db.Column(db.String(100))
    engBall100 = db.Column(db.Integer)
    engBall12 = db.Column(db.Integer)
    engDPALevel = db.Column(db.String(100))
    engBall = db.Column(db.Integer)
    engPTName = db.Column(db.String(100))
    engPTRegName = db.Column(db.String(100))
    engPTAreaName = db.Column(db.String(100))
    engPTTerName = db.Column(db.String(100))
    fraTest = db.Column(db.String(100))
    fraTestStatus = db.Column(db.String(100))
    fraBall100 = db.Column(db.Integer)
    fraBall12 = db.Column(db.Integer)
    fraDPALevel = db.Column(db.String(100))
    fraBall = db.Column(db.Integer)
    fraPTName = db.Column(db.String(100))
    fraPTRegName = db.Column(db.String(100))
    fraPTAreaName = db.Column(db.String(100))
    fraPTTerName = db.Column(db.String(100))
    deuTest = db.Column(db.String(100))
    deuTestStatus = db.Column(db.String(100))
    deuBall100 = db.Column(db.Integer)
    deuBall12 = db.Column(db.Integer)
    deuDPALevel = db.Column(db.String(100))
    deuBall = db.Column(db.Integer)
    deuPTName = db.Column(db.String(100))
    deuPTRegName = db.Column(db.String(100))
    deuPTAreaName = db.Column(db.String(100))
    deuPTTerName = db.Column(db.String(100))
    spaTest = db.Column(db.String(100))
    spaTestStatus = db.Column(db.String(100))
    spaBall100 = db.Column(db.Integer)
    spaBall12 = db.Column(db.Integer)
    spaDPALevel = db.Column(db.String(100))
    spaBall = db.Column(db.Integer)
    spaPTName = db.Column(db.String(100))
    spaPTRegName = db.Column(db.String(100))
    spaPTAreaName = db.Column(db.String(100))
    spaPTTerName = db.Column(db.String(100))

    def __init__(self, OUTID, Birth, SEXTYPENAME, REGNAME, AREANAME, TERNAME, REGTYPENAME, TerTypeName, year,
                 ClassProfileNAME, ClassLangName, EONAME, EOTYPENAME, EOREGNAME, EOAreaName, EOTerName, EOParent,
                 UkrTest, UkrTestStatus, UkrBall100, UkrBall12, UkrBall, UkrAdaptScale, UkrPTName, UkrPTRegName,
                 UkrPTAreaName, UkrPTTerName, histTest, HistLang, histTestStatus, histBall100, histBall12, histBall,
                 histPTName, histPTRegName, histPTAreaName, histPTTerName, mathTest, mathLang, mathTestStatus,
                 mathBall100, mathBall12, mathBall, mathPTName, mathPTRegName, mathPTAreaName, mathPTTerName,
                 physTest, physLang, physTestStatus, physBall100, physBall12, physBall, physPTName, physPTRegName,
                 physPTAreaName, physPTTerName, chemTest, chemLang, chemTestStatus, chemBall100, chemBall12, chemBall,
                 chemPTName, chemPTRegName, chemPTAreaName, chemPTTerName, bioTest, bioLang, bioTestStatus,
                 bioBall100, bioBall12, bioBall, bioPTName, bioPTRegName, bioPTAreaName, bioPTTerName, geoTest,
                 geoLang, geoTestStatus, geoBall100, geoBall12, geoBall, geoPTName, geoPTRegName, geoPTAreaName,
                 geoPTTerName, engTest, engTestStatus, engBall100, engBall12, engDPALevel, engBall, engPTName,
                 engPTRegName, engPTAreaName, engPTTerName, fraTest, fraTestStatus, fraBall100, fraBall12,
                 fraDPALevel, fraBall, fraPTName, fraPTRegName, fraPTAreaName, fraPTTerName, deuTest, deuTestStatus,
                 deuBall100, deuBall12, deuDPALevel, deuBall, deuPTName, deuPTRegName, deuPTAreaName, deuPTTerName,
                 spaTest, spaTestStatus, spaBall100, spaBall12, spaDPALevel, spaBall, spaPTName, spaPTRegName,
                 spaPTAreaName, spaPTTerName):
        self.OUTID = OUTID
        self.Birth = Birth
        self.SEXTYPENAME = SEXTYPENAME
        self.REGNAME = REGNAME
        self.AREANAME = AREANAME
        self.TERNAME = TERNAME
        self.REGTYPENAME = REGTYPENAME
        self.TerTypeName = TerTypeName
        self.year = year
        self.ClassProfileNAME = ClassProfileNAME
        self.ClassLangName = ClassLangName
        self.EONAME = EONAME
        self.EOTYPENAME = EOTYPENAME
        self.EOREGNAME = EOREGNAME
        self.EOAreaName = EOAreaName
        self.EOTerName = EOTerName
        self.EOParent = EOParent
        self.UkrTest = UkrTest
        self.UkrTestStatus = UkrTestStatus
        self.UkrBall100 = UkrBall100
        self.UkrBall12 = UkrBall12
        self.UkrBall = UkrBall
        self.UkrAdaptScale = UkrAdaptScale
        self.UkrPTName = UkrPTName
        self.UkrPTRegName = UkrPTRegName
        self.UkrPTAreaName = UkrPTAreaName
        self.UkrPTTerName = UkrPTTerName
        self.histTest = histTest
        self.HistLang = HistLang
        self.histTestStatus = histTestStatus
        self.histBall100 = histBall100
        self.histBall12 = histBall12
        self.histBall = histBall
        self.histPTName = histPTName
        self.histPTRegName = histPTRegName
        self.histPTAreaName = histPTAreaName
        self.histPTTerName = histPTTerName
        self.mathTest = mathTest
        self.mathLang = mathLang
        self.mathTestStatus = mathTestStatus
        self.mathBall100 = mathBall100
        self.mathBall12 = mathBall12
        self.mathBall = mathBall
        self.mathPTName = mathPTName
        self.mathPTRegName = mathPTRegName
        self.mathPTAreaName = mathPTAreaName
        self.mathPTTerName = mathPTTerName
        self.physTest = physTest
        self.physLang = physLang
        self.physTestStatus = physTestStatus
        self.physBall100 = physBall100
        self.physBall12 = physBall12
        self.physBall = physBall
        self.physPTName = physPTName
        self.physPTRegName = physPTRegName
        self.physPTAreaName = physPTAreaName
        self.physPTTerName = physPTTerName
        self.chemTest = chemTest
        self.chemLang = chemLang
        self.chemTestStatus = chemTestStatus
        self.chemBall100 = chemBall100
        self.chemBall12 = chemBall12
        self.chemBall = chemBall
        self.chemPTName = chemPTName
        self.chemPTRegName = chemPTRegName
        self.chemPTAreaName = chemPTAreaName
        self.chemPTTerName = chemPTTerName
        self.bioTest = bioTest
        self.bioLang = bioLang
        self.bioTestStatus = bioTestStatus
        self.bioBall100 = bioBall100
        self.bioBall12 = bioBall12
        self.bioBall = bioBall
        self.bioPTName = bioPTName
        self.bioPTRegName = bioPTRegName
        self.bioPTAreaName = bioPTAreaName
        self.bioPTTerName = bioPTTerName
        self.geoTest = geoTest
        self.geoLang = geoLang
        self.geoTestStatus = geoTestStatus
        self.geoBall100 = geoBall100
        self.geoBall12 = geoBall12
        self.geoBall = geoBall
        self.geoPTName = geoPTName
        self.geoPTRegName = geoPTRegName
        self.geoPTAreaName = geoPTAreaName
        self.geoPTTerName = geoPTTerName
        self.engTest = engTest
        self.engTestStatus = engTestStatus
        self.engBall100 = engBall100
        self.engBall12 = engBall12
        self.engDPALevel = engDPALevel
        self.engBall = engBall
        self.engPTName = engPTName
        self.engPTRegName = engPTRegName
        self.engPTAreaName = engPTAreaName
        self.engPTTerName = engPTTerName
        self.fraTest = fraTest
        self.fraTestStatus = fraTestStatus
        self.fraBall100 = fraBall100
        self.fraBall12 = fraBall12
        self.fraDPALevel = fraDPALevel
        self.fraBall = fraBall
        self.fraPTName = fraPTName
        self.fraPTRegName = fraPTRegName
        self.fraPTAreaName = fraPTAreaName
        self.fraPTTerName = fraPTTerName
        self.deuTest = deuTest
        self.deuTestStatus = deuTestStatus
        self.deuBall100 = deuBall100
        self.deuBall12 = deuBall12
        self.deuDPALevel = deuDPALevel
        self.deuBall = deuBall
        self.deuPTName = deuPTName
        self.deuPTRegName = deuPTRegName
        self.deuPTAreaName = deuPTAreaName
        self.deuPTTerName = deuPTTerName
        self.spaTest = spaTest
        self.spaTestStatus = spaTestStatus
        self.spaBall100 = spaBall100
        self.spaBall12 = spaBall12
        self.spaDPALevel = spaDPALevel
        self.spaBall = spaBall
        self.spaPTName = spaPTName
        self.spaPTRegName = spaPTRegName
        self.spaPTAreaName = spaPTAreaName
        self.spaPTTerName = spaPTTerName