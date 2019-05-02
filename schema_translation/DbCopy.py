#! /usr/bin/env python
#------------------------------------------------------------------------------
#$Id: DbCopy.py 10826 2015-09-24 13:36:19Z Eckhard Sutorius $
"""
   Create MySql schema and prepare data for ingest.

   @author: E. Sutorius
   @org:    WFAU, IfA, University of Edinburgh
"""
#------------------------------------------------------------------------------
from __future__    import print_function, division

from   collections import defaultdict, namedtuple
import cPickle
import dircache
import getopt
import inspect
from   keyword     import iskeyword
import MySQLdb     as mysql
import mx.DateTime
import os
import string
import sys

from   wsatools.CLI                    import CLI
#import wsatools.CSV                        as csv
from   wsatools.DbConnect.DbSession    import DbSession, Outgester, SelectSQL
#from   wsatools.File                   import File, PickleFile
#from   wsatools.Logger                 import Logger, ForLoopMonitor
from   wsatools.DbConnect.IngCuSession import IngCuSession
#from   wsatools.SystemConstants        import SystemConstants
#import wsatools.Utilities                  as utils

#------------------------------------------------------------------------------

class DbCopy(IngCuSession):
    """Create MySql schema and prepare data for ingest.
    """
    #--------------------------------------------------------------------------
    # Define class constants (access as DbCopy.varName)
    xmlPath = "hbm/"
    viewPath = "views/"
    excludedPath = "excluded/"
    sqlPath = "sql/"
    classPath = "hbm/classes/schema/"
    javaPath = "hbm/src/schema/"
    csvOutPath = "/disk59/mysql/inout/"
    #lsstBasePath = "/data/ukidss-data/"
    lsstBasePath = "/datapool/data/"
    # Views in UKIDSS DB
    ukidssViews = (
        "allFrameSets", "dxsFrameSets", "dxsJKmergeLog", "dxsJKsource",
        "gcsFrameSets", "gcsPointSource", "gcsZYJHKmergeLog", "gcsZYJHKsource",
        "lasExtendedSource", "lasFrameSets", "lasPointSource",
        "lasYJHKmergeLog", "lasYJHKsource", "reliableDxsSource",
        "reliableGcsPointSource", "reliableGpsPointSource",
        "reliableLasPointSource", "reliableUdsSource", "udsFrameSets",
        "UKIDSSDetection", "gpsFrameSets", "gpsJHKmergeLog", "gpsJHKsource",
        "gpsPointSource")
    # Tables to be excluded from UKIDSS DB
    ukidssExcluded = ()
    # Views in SDSS DB
    sdssViews = (
        "Columns", "CoordType", "FieldMask", "FieldQuality", "FramesStatus",
        "Galaxy", "GalaxyTag", "HoleType", "ImageMask", "InsideMask",
        "MaskType", "ObjType", "PhotoAux", "PhotoAuxAll", "PhotoFamily",
        "PhotoFlags", "PhotoMode", "PhotoObj", "PhotoPrimary", "PhotoSecondary",
        "PhotoStatus", "PhotoType", "PrimTarget", "ProgramType", "PspStatus",
        "QsoCatalog", "QsoConcordance", "RegionConvex", "Run", "SecTarget",
        "Sky", "SpecClass", "SpecLine", "SpecLineNames", "SpecObj", "SpecPhoto",
        "SpecZStatus", "SpecZWarning", "Star", "StarTag", "TargPhotoObj",
        "TargPhotoPrimary", "TargPhotoSecondary", "TiMask", "Tile",
        "TiledTarget", "TilingBoundary", "TilingMask", "UberCalibStatus",
        "Unknown", "spbsParams")
    # Tables to be excluded from SDSS DB
    sdssExcluded = (
        "AlexLimits", "ES_FlagsGalaxyRedshiftSampleDR7", "QsoNeighbors",
        "QueryResults", "dtproperties", "input_jim", "nli_delete.",
        "output_jim_-20_20", "output_jim_-90_-20", "output_jim_20_50",
        "output_jim_50_90", "pairs", "pairs10000", "pairs1000000", "pairs_alt",
        "sysdiagrams", "zone1", "zone10000", "zone1000000", "SiteDBs",
        "TargPhotoTag", "top100000", "TargPhotoObjAll", "nli_delete")
    # Reserved Java keywords (2018-07-26)
    reservedJavaKeys = {
        "abstract":'', "assert":'',
        "boolean":'', "break":'', "byte":'',
        "case":'', "catch":'', "char":'', "class":"classif",
        "const":'', "continue":'',
        "default":'', "double":'', "do":'', "else":'',
        "enum":'enumed', "extends":'',
        "false":'', "final":'', "finally":'', "float":'', "for":'',
        "goto":'',
        "if":'', "implements":'', "import":'', "instanceof":'', "int":'',
        "interface":'', "key":"theKey", "long":'',
        "native":'nativeData', "new":'', "null":'', "package":'', "private":'',
        "protected":'', "public":'',
        "return":'',
        "short":'', "static":'', "strictfp":'', "super":'',
        "switch":'', "synchronized":'',
        "this":'', "throw":'', "throws":'', "transient":'',
        "true":'', "try":'',
        "void":'', "volatile":'', "while":''}
    # Reserved MySql keywords (v8.0)
    reservedMySqlKeys = {
        "ACCESSIBLE":'', "ADD":'', "ALL":'', "ALTER":'', "ANALYZE":'',
        "AND":'', "AS":'', "ASC":'', "ASENSITIVE":'',
        "BEFORE":'', "BETWEEN":'', "BIGINT":'', " BINARY":'',
        "BLOB":'', "BOTH":'', "BY":'',
        "CALL":'', "CASCADE":'', "CASE":'', "CHANGE":'',
        "CHAR":'', "CHARACTER":'', "CHECK":'',
        "COLLATE":'', "COLUMN":'', "CONDITION":'',
        "CONSTRAINT":'', "CONTINUE":'', "CONVERT":'',
        "CREATE":'', "CROSS":'', "CUBE":'',
        "CUME_DIST":'', "CURRENT_DATE":'', "CURRENT_TIME":'',
        "CURRENT_TIMESTAMP":'', "CURRENT_USER":'', "CURSOR":'',
        "DATABASE":'', "DATABASES":'', "DAY_HOUR":'',
        "DAY_MICROSECOND":'', "DAY_MINUTE":'',
        "DAY_SECOND":'', "DEC":"DECL",
        "DECIMAL":'', "DECLARE":'', "DEFAULT":'',
        "DELAYED":'', "DELETE":'', "DENSE_RANK":'',
        "DESC":'', "DESCRIBE":'', "DETERMINISTIC":'',
        "DISTINCT":'', "DISTINCTROW":'', "DIV":'',
        "DOUBLE":'', "DROP":'', "DUAL":'',
        "EACH":'', "ELSE":'', "ELSEIF":'',
        "EMPTY":'', "ENCLOSED":'', "ESCAPED":'',
        "EXCEPT":'', "EXISTS":'', "EXIT":'',
        "EXPLAIN":'',
        "FALSE":'', "FETCH":'', "FIRST_VALUE":'',
        "FLOAT":'', "FLOAT4":'', "FLOAT8":'',
        "FOR":'', "FORCE":'', "FOREIGN":'',
        "FROM":'', "FULLTEXT":'', "FUNCTION":'',
        "GENERATED":'', "GET":'', "GRANT":'',
        "GROUP":'', "GROUPING":'', "GROUPS":'',
        "HAVING":'', "HIGH_PRIORITY":'', "HOUR_MICROSECOND":'',
        "HOUR_MINUTE":'', "HOUR_SECOND":'',
        "IF":'', "IGNORE":'IGNOR', "IN":'',
        "INDEX":'', "INFILE":'', "INNER":'',
        "INOUT":'', "INSENSITIVE":'', "INSERT":'',
        "INT":'', "INT1":'', "INT2":'',
        "INT3":'', "INT4":'', "INT8":'',
        "INTEGER":'', "INTERVAL":'', "INTO":'',
        "IO_AFTER_GTIDS":'', "IO_BEFORE_GTIDS":'', "IS":'',
        "ITERATE":'',
        "JOIN":'', "JSON_TABLE":'',
        "KEY":'theKey', "KEYS":'', "KILL":'',
        "LAG":'', "LAST_VALUE":'', "LEAD":'',
        "LEADING":'', "LEAVE":'', "LEFT":'',
        "LIKE":'', "LIMIT":'', "LINEAR":'',
        "LINES":'', "LOAD":'', "LOCALTIME":'',
        "LOCALTIMESTAMP":'', "LOCK":'', "LONG":'',
        "LONGBLOB":'', "LONGTEXT":'', "LOOP":'',
        "LOW_PRIORITY":'',
        "MASTER_BIND":'', "MASTER_SSL_VERIFY_SERVER_CERT":'',
        "MATCH":'matchobj', "MAXVALUE":'', "MEDIUMBLOB":'',
        "MEDIUMINT":'', "MEDIUMTEXT":'', "MIDDLEINT":'',
        "MINUTE_MICROSECOND":'', "MINUTE_SECOND":'',
        "MOD":'', "MODIFIES":'',
        "NATURAL":'', "NOT":'', "NO_WRITE_TO_BINLOG":'',
        "NTH_VALUE":'', "NTILE":'', "NULL":'', "NUMERIC":'',
        "OF":'', "ON":'', "OPTIMIZE":'',
        "OPTIMIZER_COSTS":'', "OPTION":'', "OPTIONALLY":'',
        "OR":'', "ORDER":'', "OUT":'',
        "OUTER":'', "OUTFILE":'', "OVER":'',
        "PARTITION":'', "PERCENT_RANK":'', "PERSIST":'',
        "PERSIST_ONLY":'', "PRECISION":'', "PRIMARY":'',
        "PROCEDURE":'', "PURGE":'',
        "RANGE":'', "RANK":'rankpos', "READ":'',
        "READS":'', "READ_WRITE":'', "REAL":'',
        "RECURSIVE":'', "REFERENCES":'', "REGEXP":'',
        "RELEASE":'ReleaseTable', "RENAME":'', "REPEAT":'',
        "REPLACE":'', "REQUIRE":'', "RESIGNAL":'',
        "RESTRICT":'', "RETURN":'', "REVOKE":'',
        "RIGHT":'', "RLIKE":'', "ROW":'rowno',
        "ROWS":'', "ROW_NUMBER":'',
        "SCHEMA":'', "SCHEMAS":'', "SECOND_MICROSECOND":'',
        "SELECT":'', "SENSITIVE":'', "SEPARATOR":'',
        "SET":'', "SHOW":'', "SIGNAL":'',
        "SMALLINT":'', "SPATIAL":'', "SPECIFIC":'',
        "SQL":'', "SQLEXCEPTION":'', "SQLSTATE":'',
        "SQLWARNING":'', "SQL_BIG_RESULT":'',
        "SQL_CALC_FOUND_ROWS":'', "SQL_SMALL_RESULT":'',
        "SSL":'', "STARTING":'', "STORED":'',
        "STRAIGHT_JOIN":'', "SYSTEM":'',
        "TABLE":'', "TERMINATED":'', "THEN":'',
        "TINYBLOB":'', "TINYINT":'', "TINYTEXT":'',
        "TO":'', "TRAILING":'', "TRIGGER":'', "TRUE":'',
        "UNDO":'', "UNION":'', "UNIQUE":'',
        "UNLOCK":'', "UNSIGNED":'', "UPDATE":'',
        "USAGE":'', "USE":'', "USING":'',
        "UTC_DATE":'', "UTC_TIME":'', "UTC_TIMESTAMP":'',
        "VALUES":'', "VARBINARY":'', "VARCHAR":'',
        "VARCHARACTER":'', "VARYING":'', "VIRTUAL":'',
        "WHEN":'whenevt', "WHERE":'', "WHILE":'',
        "WINDOW":'', "WITH":'', "WRITE":'',
        "XOR":'', "YEAR_MONTH":'', "ZEROFILL":''}

    uniqueStatements = {
        "Multiframe": "alter table Multiframe add unique (fileTimeStamp,fileName);"}
    #database = "ramses1.TestWSAlsst"
    Stats = namedtuple(
        "Stats",
        "tabName, colName, datType, numPrec, numPrecRadix, minim, maxim, averg, quTime")
    # for restart with some results already done
    #tabPattern = ("uds", "ukirt")
    #tabPattern = ("las")
    #tabPattern = ("gcs")
    #tabPattern = ("ExternalSurveyTable")
    #tabPattern = ("AstrCalVers")
    #tabPattern = ("Multiframe")
    tabPattern = tuple(x for x in string.ascii_letters)
    #tabPattern = ("gps")
    #tabPattern = ("Photoz2")
    #tabPattern = ("dxsSource")
    #tabPattern = ()

    #exclPattern = ("gps")
    #exclPattern = ("X", "Neighbours", "Remeasurement")
    #exclPattern = ("sppParams", "MatchTable", "PhotoObjAll",
    #               "QsoConcordanceAll", "SpecPhotoAll", "sppLines")
    exclPattern = ()

    #--------------------------------------------------------------------------

    def __init__(self,
                 curator=CLI.getOptDef("curator"),
                 cleanUp=CLI.getOptDef("cleanup"),
                 fixCode=CLI.getOptDef("fixcode"),
                 hbmToJava=CLI.getOptDef("hbm2java"),
                 compileJava=CLI.getOptDef("compilejava"),
                 doSchema=CLI.getOptDef("schemacreate"),
                 dbOutgest=CLI.getOptDef("outgest"),
                 transferData=CLI.getOptDef("transfer"),
                 calcAvg = CLI.getOptDef("average"),
                 ReDo=CLI.getOptDef("redo"),
                 modus=CLI.getOptDef("mode"),
                 dataType=CLI.getOptDef("type"),
                 usePattern=CLI.getOptDef("pattern"),
                 isTrialRun=DbSession.isTrialRun,
                 xferlog=CLI.getOptDef("xferlog"),
                 database=DbSession.database,
                 comment=CLI.getArgDef("comment")):

        # Initialize parent class
        super(DbCopy, self).__init__(cuNum=0,
                                     curator=curator,
                                     comment=comment,
                                     reqWorkDir=False,
                                     keepWorkDir=False,
                                     database=database,
                                     autoCommit=False,
                                     isTrialRun=isTrialRun)
        
        typeTranslation = {"curator":str,
                           "cleanUp":str,
                           "fixCode":str,
                           "hbmToJava":bool,
                           "compileJava":bool,
                           "doSchema":str,
                           "dbOutgest":bool,
                           "transferData":bool,
                           "calcAvg":str,
                           "ReDo":bool,
                           "modus":str,
                           "usePattern":bool,
                           "isTrialRun":bool,
                           "xferlog":str,
                           "database":str,
                           "comment":str}

        super(DbCopy, self).attributesFromArguments(
            inspect.getargspec(DbCopy.__init__)[0], locals(),
            types=typeTranslation)

        self.createSchema = False
        self.splitSchema = False
        if self.doSchema:
            if self.doSchema == 'c':
                self.createSchema = True
            elif self.doSchema == 's':
                self.splitSchema = True
            else:
                Logger.addMessage("Wrong schema manipulation!")
                raise SystemExit()

        self.allFixes = defaultdict(list)
        self.cleanUp = self.cleanUp[:1]
        
        if self.fixCode.startswith('x'):
            self.fixCode, self.fixDbName = self.fixCode.split(':')
        self.fixCode = self.fixCode[:1]

        self.server, self.dbName = self.database.split('.')

        self.server = self.server.upper()
        self.dbName = (None if self.dbName == "WSA" else self.dbName)
        if "QSERV" in self.server:
            self.isQserv = True
        else:
            self.isQserv = False

    #--------------------------------------------------------------------------

    def _onRun(self):
        """ Run each CU requested.
        """
        # Clean Up
        if any(x in "hsljcov" for x in self.cleanUp):
             # hbm/*
            if 'h' in self.cleanUp:
                cmd = "rm -rf hbm/*"
                outText = "Deleted hbm dir"
            # sql/*
            if 's' in self.cleanUp:
                cmd = "rm -rf sql/*"
                outText = "Deleted sql dir"
           # lib/schema.jar
            if 'l' in self.cleanUp:
                cmd = "rm -f lib/schema.jar"
                outText = "Deleted schema.jar"
            # hbm/classes/schema/*
            if 'c' in self.cleanUp:
                cmd = "rm -f %s/*" % DbCopy.classPath
                outText = "Deleted all .class files."
            # hbm/src/schema/*
            if 'j' in self.cleanUp:
                cmd = "rm -f %s/*" % DbCopy.javaPath
                outText = "Deleted all .java files."
            if 'v' in self.cleanUp:
                cmd = "rm -f %s/*; rm -f %s/*" % (
                    DbCopy.viewPath, DbCopy.excludedPath)
                outText = "Deleted all views/excluded files."
            # csv dir
            if 'o' in self.cleanUp:
                cmd = "rm -f %s/*" % DbCopy.csvOutPath
                outText = "Deleted all .csv outgest files."
            self.runCommand(cmd)
            Logger.addMessage(outText)

        # Fix code
        if any(x in "xjl" for x in self.fixCode):
            if 'x' in self.fixCode:
                self.fixXml()
            if 'j' in self.fixCode:
                self.fixJava()
            if 'l' in self.fixCode:
                self.fixLineBreak()

        # hbm2java
        if self.hbmToJava:
            self.runCommand("ant hbm2java")

        # compilejava
        if self.compileJava:
            self.runCommand("ant compilejava")

        # schema-create
        if self.createSchema:
            self.runCommand("mv -f hbm2java_code_generation.log hbm2java_code_generation.log.part1")
            self.runCommand("touch hbm2java_code_generation.log")
            self.runCommand("ant schema-create")
            if self.modus == "sqlserver":
                self.runCommand("mv -f sql/MySQL_schema_create.sql SQLServer_schema_create.sql")

        # split schema tablewise
        if self.splitSchema:
            self.splitFullSchema()

        # DB outgest
        if self.dbOutgest:
            theMySql = self.readSql(mode="outgest")
            outgestTables = theMySql.keys()
            isCSV = (self.dataType == "csv")
            theOutDir = self.theOutgest(outgestTables, theMySql, isCSV)

        # Copy data to lsstuk4
        if self.transferData:
            theDB = self.database.split('.')[-1]
            theOutDir = os.path.join(DbCopy.csvOutPath, "%s.csv" % theDB)
            lsstDir = os.path.join(DbCopy.lsstBasePath, theDB)
            #allFiles = [("data", os.path.join(theOutDir, x))
            #            for x in dircache.listdir(theOutDir)]
            allFiles = []
            for sqlFile in os.listdir(DbCopy.sqlPath):
                if sqlFile.endswith(".sql"):
                    allFiles.append(("sql", os.path.join(DbCopy.sqlPath,
                                                         sqlFile)))
            if self.isTrialRun:
                print(allFiles)
                print(theOutDir, "==>", lsstDir)
            else:
                for fileDir, fileName in allFiles:
                    outDir = os.path.join(lsstDir, fileDir)
                    Logger.addMessage("Copying %s..." % fileName)
                    cmd = "scp %s root@lsstuk4.roe.ac.uk:%s" % (
                        fileName, outDir)
                    self.runCommand(cmd)
                    Logger.addMessage("Copied to %s." % outDir)
            Logger.addMessage("finished.")

        if self.calcAvg:
            if self.server in ["MYSQL", "QSERV"]:
                self.checkMySql()
            else:
                if self.database:
                    self.checkDB()

    #--------------------------------------------------------------------------

    def getColumns(self, theSql, tableName):
        if self.modus == "mysql":
            columns = "[%s]" % '],['.join(theSql[tableName])
        if self.modus == "sqlserver":
            #outgestTables = ["Filter", "gcsDetection"]
            #outgestTables = ["RequiredNeighbours"]
            columns = '*'
        return columns
    
    #--------------------------------------------------------------------------

    def theOutgest(self, outgestTables, theSql, isCSV=True):
        """ Outgest from DB.
        """
        if self.database:
            # connect to DB
            self._connectToDb()
            self.archive.enableDirtyRead()

            # outdir
            suffix = ("csv" if isCSV else "bin")
            binOutDir = os.path.join(DbCopy.csvOutPath,
                                     "%s.%s" % (self.archive.database, suffix))
            ensureDirExist(binOutDir)

            # outgest tables
            CuID = ("000998" if self.modus == "mysql" else "000444_MS")
            if not self.ReDo:
                existTables = [x.partition("%sCuID%s" % (self.archive.database,
                                                         CuID))[0]
                               for x in dircache.listdir(binOutDir)]
                outgestTables = list(
                    set(outgestTables).difference(existTables))

            if self.usePattern:
                tmpTables = []
                for tabName in outgestTables:
                    if tabName.startswith(DbCopy.tabPattern):
                        tmpTables.append(tabName)
                outgestTables = sorted(tmpTables)

                # exclude pattern
                tmpTables = []
                for tabName in outgestTables:
                    if not any(x in tabName for x in DbCopy.exclPattern):
                        tmpTables.append(tabName)
                outgestTables = sorted(tmpTables)

            #print(">>>",existTables,outgestTables)

            shareFileID = "%sCuID%s%s" % (
                self.archive.database, CuID, ".csv" if isCSV else '')
            self._outgester = Outgester(self.archive, tag=shareFileID)

            progress = ForLoopMonitor(outgestTables)
            for tableName in sorted(outgestTables):
                columns = self.getColumns(theSql, tableName)
                Logger.addMessage("Outgesting %s..." % tableName)
                query = SelectSQL(columns, "%s..%s" % (
                    self.archive.database, tableName), '')
                if self.isTrialRun:
                    print(">>>",query)
                else:
                    print(">>> %s [%d]" % (query._sql, len(query._sql)))
                    outgestFileName = self._outgester.outgestQuery(
                        query, tableName, binOutDir, isCsv=isCSV)
                Logger.addMessage("Outgested to %s." % outgestFileName)

                progress.testForOutput()

            # disconnect from DB
            self._disconnectFromDb()

        Logger.addMessage("finished.")
        return binOutDir

    #--------------------------------------------------------------------------

    def binOutgest(self, outgestTables):
        """ Outgest from DB into binary file.
        """
        if self.database:
            # connect to DB
            self._connectToDb()
            self.archive.enableDirtyRead()

            # outdir
            binOutDir = os.path.join(DbCopy.csvOutPath,
                                     "%s.bin" % self.archive.database)
            ensureDirExist(binOutDir)
            
            # outgest tables
            if not self.ReDo:
                existTables = [x.split("UKIDSSDR8PLUSCuID000444")[0]
                               for x in dircache.listdir(binOutDir)]
                outgestTables = list(
                    set(outgestTables).difference(existTables))

            shareFileID = '%sCuID%06d' % (self.archive.database, 444)
            self._outgester = Outgester(self.archive, tag=shareFileID)

            progress = ForLoopMonitor(outgestTables)
            for tableName in sorted(outgestTables):
                Logger.addMessage("Outgesting %s..." % tableName)
                query = SelectSQL("*", "%s..%s" % (
                    self.archive.database, tableName), '')
                if self.isTrialRun:
                    print(">>>",query)
                else:
                    outgestFileName = self._outgester.outgestQuery(
                        query, tableName, binOutDir, isCsv=False)
                Logger.addMessage("Outgested to %s." % outgestFileName)

                progress.testForOutput()

            # disconnect from DB
            self._disconnectFromDb()

        Logger.addMessage("finished.")
        return binOutDir

    #--------------------------------------------------------------------------

    def csvOutgest(self, outgestTables, columns):
        """ Outgest from DB into csv file in primary-keys-first order.
        """
        if self.database:
            # connect to DB
            self._connectToDb()
            self.archive.enableDirtyRead()

            # outdir
            csvOutDir = os.path.join(DbCopy.csvOutPath,
                                     "%s.csv" % self.archive.database)
            ensureDirExist(csvOutDir)

            # outgest tables
            if not self.ReDo:
                existTables = [x.split("UKIDSSDR8PLUSCuID000998")[0]
                               for x in dircache.listdir(csvOutDir)]
                outgestTables = list(
                    set(outgestTables).difference(existTables))
            #print(sorted(outgestTables))
            #sys.exit()
            shareFileID = '%sCuID%06d' % (self.archive.database, 998)
            self._outgester = Outgester(self.archive, tag=shareFileID)

            progress = ForLoopMonitor(outgestTables)
            for tableName in sorted(outgestTables):
                Logger.addMessage("Outgesting %s..." % tableName)
                #attrs = theMySql[tableName]
                #result = self.archive.query(
                #    "%s" % ','.join(attrs),
                #    tableName)
                #csvFileName = os.path.join(csvOutDir,
                #                           "%s.csv" % tableName)
                #csv.File(csvFileName, 'w').writelines(result)
                #Logger.addMessage("%s outgested..." % tableName)
                csvFileName = os.path.join(csvOutDir,
                                           "%s.csv" % tableName)
                #columns = "%s" % ','.join(theMySql[tableName])
                query = SelectSQL(columns, "%s..%s" % (
                    self.archive.database, tableName), '')
                if self.isTrialRun:
                    print(">>>",query)
                else:
                    outgestFileName = self._outgester.outgestQuery(
                        query, tableName, csvOutDir, isCsv=True)
                Logger.addMessage("Outgested to %s." % outgestFileName)

                progress.testForOutput()

            # disconnect from DB
            self._disconnectFromDb()

        Logger.addMessage("finished.")
        return csvOutDir

    #--------------------------------------------------------------------------

    def connectMySql(self):
        try:
            if self.isQserv:
                # Qserv MySQL test DB [lsstuk1]
                self.db = mysql.connect(host="129.215.175.50", port=13306,
                                        db=self.dbName, user="qsmaster",
                                        passwd='')
                self.dbName = "lsstuk1" 
            else:    
                # MySQL test DB
                #self.db = mysql.connect(host="129.215.193.22", port=3306,
                #                        db=self.dbName, user="lsst",
                #                        passwd="lssttest")
                #self.dbName = "mysqltest" 

                # MySQL plain test DB [lsstuk2]
                #self.db = mysql.connect(host="129.215.175.51", port=3306,
                #                        db=self.dbName, user="qserv",
                #                        passwd="lssttest")
                #self.dbName = "lsstuk2" 
                
                # MySQL plain test DB [lsstuk4]
                self.db = mysql.connect(host="129.215.175.53", port=3360,
                                        db=self.dbName, user="qserv_eval",
                                        passwd="Qserv@R0E")
                self.dbName = "TESTlsst" 
                
                
            self.cursor = self.db.cursor()
        except:
            Logger.addMessage("Can't connect to lsst (MySQL) DB.")

    #--------------------------------------------------------------------------

    def disconnectMySql(self):
        self.cursor.close()
        self.db.close()

    #--------------------------------------------------------------------------

    def queryMySql(self, selectStr, fromStr, whereStr='', groupBy='',
                   orderBy='', firstOnly=False, default=None,
                   ResultsTuple=None):
        """
        @param selectStr: SQL SELECT string; comma-separated column names.
        @type  selectStr: str
        @param fromStr:   SQL FROM string; single table name or L{Join} object.
        @type  fromStr:   str or L{Join}
        @param whereStr:  Optional SQL WHERE clause.
        @type  whereStr:  str
        @param groupBy:   Optional SQL GROUP BY string.
        @type  groupBy:   str
        @param orderBy:   Optional SQL ORDER BY string.
        @type  orderBy:   str
        @param firstOnly: If True, just return the first entry in result set.
        @type  firstOnly: bool
        @param default:   Value to return if querying first entry and no
                          entries found.
        @type  default:   object
        @param ResultsTuple: Convert results to this specific namedtuple type.
        @type  ResultsTuple: type
          
        """

        theQuery = "SELECT %s FROM %s" % (selectStr, fromStr)

        if whereStr:
            theQuery += " WHERE " + whereStr

        if groupBy:
            theQuery += " GROUP BY " + groupBy

        if orderBy:
            theQuery += " ORDER BY " + orderBy

        if firstOnly:
            theQuery = "%s LIMIT 1" % theQuery

        resultNum = self.cursor.execute(theQuery)
        result = self.cursor.fetchall()

        if firstOnly:
            result = result[0]

        if firstOnly and (result is None or result[0] is None):
            # NB: if firstOnly then result is always a tuple or None
            if default is None and ',' in selectStr:
                result = tuple([None] * (
                    len(selectStr.split(',')) - \
                    selectStr.upper().count("DECIMAL")))
            else:
                return default

        if selectStr.endswith('*') or ',' in selectStr:
            # Case of a list of tuples - make a namedtuple if possible
            if not ResultsTuple:
                metadata = self.cursor.description or []
                identity = string.maketrans('', '')
                columns = [entry[0].translate(
                    identity, string.punctuation).translate(identity, ' ')
                           for entry in metadata]

                # i.e. there are no unnamed columns, no duplicates and no
                # keywords - this last check can go when we get Python 2.7
                if columns and not any(not column or iskeyword(column)
                                       for column in columns) \
                  and len(columns) == len(set(columns)):
                    ResultsTuple = namedtuple("Query", ' '.join(columns))

            if ResultsTuple:
                if firstOnly:
                    return ResultsTuple(*result)
                else:
                    return [ResultsTuple(*row) for row in result]

            return result

        elif firstOnly:
            # Strip single tuple values down to scalars
            return result[0]

        else:
            # Multiple rows but single column
            return [eachTuple[0] for eachTuple in result]

    #--------------------------------------------------------------------------

    def queryMySqlNumRows(self, tableName, whereStr='', groupBy='',
                           distinctAttr=''):
        """
        @param tableName:    Table to query.
        @type  tableName:    str
        @param whereStr:     WHERE string, optional where clause.
        @type  whereStr:     str
        @param groupBy:      Specify a comma-separated list of columns in which
                             to group counts by.
        @type  groupBy:      str
        @param distinctAttr: With distinct values of this attribute.
        @type  distinctAttr: str

        """
        attr = ('DISTINCT ' + distinctAttr) if distinctAttr else '*'
        cmd = "COUNT(%s) AS count" % attr
        for _attempt in range(2):
            try:
                if groupBy:
                    return self.queryMySql(
                        groupBy + ", " + cmd, tableName, whereStr,
                        groupBy=groupBy, orderBy=groupBy)
                else:
                    results = self.queryMySql(cmd, tableName, whereStr)

                    if len(results) == 1:
                        return results[0]
                    else: # "Group by"-type query: length is the desired value
                        return len(results)

            except mysql.ProgrammingError as error:
                if "Arithmetic overflow" in str(error):
                    cmd = "COUNT_BIG(%s) AS count" % attr
                else:
                    raise

    #--------------------------------------------------------------------------

    def checkMySql(self):
        theMySql = self.readSql()
        dbMeta = defaultdict(lambda : defaultdict(list))

        qsTag = ("_qserv" if self.isQserv else '')

        # connect to DB
        self.connectMySql()

        # open stats file
        statsOutFile = File("LSST_mysql%s_%s_Stats.txt" % (
            qsTag, self.calcAvg.upper()))
        statsOutFile.wopen(buffering=0)

        # availalbe tables
        availTables = theMySql.keys()

        checkAttr = ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
                     "NUMERIC_PRECISION"]
        result = self.queryMySql(','.join(checkAttr),
                                 "INFORMATION_SCHEMA.COLUMNS",
                                 "TABLE_SCHEMA='%s'" % self.dbName)
        
        for tabName, colName, datType, numPrec in result:
            dbMeta[tabName][colName] = DbCopy.Stats(
                tabName, colName, datType, numPrec, 0, 0, 0 , 0, '-')

        # for restart with some results already done
        # include pattern
        tmpTables = []
        for tabName in availTables:
            if tabName.startswith(DbCopy.tabPattern):
                tmpTables.append(tabName)
        availTables = sorted(tmpTables)

        # exclude pattern
        tmpTables = []
        for tabName in availTables:
            if not tabName.startswith(DbCopy.exclPattern):
                tmpTables.append(tabName)
        availTables = sorted(tmpTables)

        progress = ForLoopMonitor(availTables)
        for tableName in sorted(availTables):
            self.cursor.execute("FLUSH TABLES")
            theTab = ("`%s`" % tableName
                      if tableName in ["Release"] else tableName)
            Logger.addMessage("Checking %s..." % tableName)
            
            #if tableName == "CurationTask":
            #    break
            # num rows
            self.cleanCache()
            queryBeg = makeDateTime()
            numRows = self.queryMySqlNumRows(theTab)
            queryEnd = makeDateTime()
            queryTime = "{%s}" % str(queryEnd - queryBeg)
            print("[%s rows] %s" % (numRows, queryTime))
            statsOutFile.writetheline("%s: [%d rows] %s" % (
                tableName, numRows, queryTime))

            for colName in sorted(dbMeta[tableName]):
                print("%s.%s: %s  " % (tableName, colName,
                                       dbMeta[tableName][colName].datType),
                      end="")
                theCol = ("`%s`" % colName
                          if colName in ["dec", "ignore"] else colName)
                # get stats by type
                if dbMeta[tableName][colName].datType == "varchar" or \
                       dbMeta[tableName][colName].datType == "text":
                    theQuery = []
                    if self.calcAvg in ['n', 'm', 'a']:
                        theQuery.append("MIN(LENGTH(%s))" % theCol)
                    if self.calcAvg in ['x', 'm', 'a']:
                        theQuery.append("MAX(LENGTH(%s))" % theCol)
                    if self.calcAvg in ['g', 'a']:
                        theQuery.append(
                            "CAST(AVG(LENGTH(%s)) AS DECIMAL(65,7))" % theCol)

                    self.cleanCache()
                    queryBeg = makeDateTime()
                    result = self.queryMySql(
                        ','.join(theQuery), theTab, firstOnly=True)
                    
                    #result = self.queryMySql(
                    #    "MIN(LENGTH(%s)), MAX(LENGTH(%s)), "
                    #    "CAST(AVG(LENGTH(%s)) AS DECIMAL(65,7))" % (
                    #    theCol, theCol, theCol),
                    #    theTab, firstOnly=True)
                    queryEnd = makeDateTime()
                    queryTime = "{%s}" % str(queryEnd - queryBeg)
                    print(result, queryTime)
                    if self.calcAvg in ['n']:
                        theMin = result[0]
                        theMax = None
                        theAverg = None
                    elif self.calcAvg in ['x']:
                        theMin = None
                        theMax = result[0]
                        theAverg = None
                    elif self.calcAvg in ['g']:
                        theMin = None
                        theMax = None
                        theAverg = (float(result[0]) if result[0] else None)
                    elif self.calcAvg in ['m']:
                        theMin = result[0]
                        theMax = result[1]
                        theAverg = None
                    elif self.calcAvg in ['a']:
                        theMin = result[0]
                        theMax = result[1]
                        theAverg = (float(result[2]) if result[2] else None)
                    dbMeta[tableName][colName] = \
                        dbMeta[tableName][colName]._replace(
                           minim = theMin,
                           maxim = theMax,
                           averg = theAverg,
                           quTime = queryTime)
                    
                    #dbMeta[tableName][colName] = \
                    #    dbMeta[tableName][colName]._replace(
                    #       minim = result[0],
                    #       maxim = result[1],
                    #       averg = result[2],
                    #       quTime = queryTime)
                elif dbMeta[tableName][colName].datType == "datetime":
                    theQuery = []
                    if self.calcAvg in ['n', 'm', 'a']:
                        theQuery.append("MIN(LENGTH(%s))" % theCol)
                    if self.calcAvg in ['x', 'm', 'a']:
                        theQuery.append("MAX(LENGTH(%s))" % theCol)

                    self.cleanCache()
                    queryBeg = makeDateTime()
                    result = self.queryMySql(
                        ','.join(theQuery), theTab, firstOnly=True)
                    #result = self.queryMySql(
                    #    "MIN(%s), MAX(%s)" % (theCol, theCol),
                    #    theTab, firstOnly=True)
                    queryEnd = makeDateTime()
                    queryTime = "{%s}" % str(queryEnd - queryBeg)
                    print(result, queryTime)

                    if self.calcAvg in ['n']:
                        theMin = str(result[0])
                        theMax = None
                        theAverg = None
                    elif self.calcAvg in ['x']:
                        theMin = None
                        theMax = str(result[0])
                        theAverg = None
                    elif self.calcAvg in ['g']:
                        theMin = None
                        theMax = None
                        theAverg = '----'
                    elif self.calcAvg in ['m']:
                        theMin = str(result[0])
                        theMax = str(result[1])
                        theAverg = None
                    elif self.calcAvg in ['a']:
                        theMin = str(result[0])
                        theMax = str(result[1])
                        theAverg = '----'
                    dbMeta[tableName][colName] = \
                        dbMeta[tableName][colName]._replace(
                           minim = theMin,
                           maxim = theMax,
                           averg = theAverg,
                           quTime = queryTime)

                    #dbMeta[tableName][colName] = \
                    #    dbMeta[tableName][colName]._replace(
                    #       minim = str(result[0]),
                    #       maxim = str(result[1]),
                    #       averg = '----',
                    #       quTime = queryTime)

                elif "int" in dbMeta[tableName][colName].datType:
                    resultErr = None
                    theQuery = []
                    if self.calcAvg in ['n', 'm', 'a']:
                        theQuery.append("MIN(%s)" % theCol)
                    if self.calcAvg in ['x', 'm', 'a']:
                        theQuery.append("MAX(%s)" % theCol)
                    if self.calcAvg in ['g', 'a']:
                        theQuery.append(
                            "AVG(1.0*CAST(%s AS SIGNED))" % theCol)
                    
                    try:
                        self.cleanCache()
                        queryBeg = makeDateTime()
                        result = self.queryMySql(
                            ','.join(theQuery), theTab, firstOnly=True)
                        #result = self.queryMySql(
                        #    "MIN(%s), MAX(%s), AVG(1.0*CAST(%s AS SIGNED))" % (
                        #    theCol, theCol, theCol),
                        #    theTab, firstOnly=True)
                    except Exception as error:
                        if "Arithmetic overflow" in str(error):
                            self.cleanCache()
                            queryBeg = makeDateTime()
                            result = self.queryMySql(
                                ','.join(theQuery[:2]), theTab, firstOnly=True)
                            #result = self.queryMySql(
                            #    "MIN(%s), MAX(%s)" % (
                            #    theCol, theCol),
                            #    theTab, firstOnly=True)
                            resultErr = '---'
                        else:
                            print("***",error,"***")
                            raise SystemExit()
                    queryEnd = makeDateTime()
                    queryTime = "{%s}" % str(queryEnd - queryBeg)
                    print(result, queryTime)
                    if self.calcAvg in ['n']:
                        theMin = str(result[0])
                        theMax = None
                        theAverg = None
                    elif self.calcAvg in ['x']:
                        theMin = None
                        theMax = str(result[0])
                        theAverg = None
                    elif self.calcAvg in ['g']:
                        theMin = None
                        theMax = None
                        theAverg = (resultErr if resultErr
                                    else (float(result[0])
                                          if result[0] else None))
                    elif self.calcAvg in ['m']:
                        theMin = str(result[0])
                        theMax = str(result[1])
                        theAverg = None
                    elif self.calcAvg in ['a']:
                        theMin = str(result[0])
                        theMax = str(result[1])
                        theAverg = (resultErr if resultErr
                                    else (float(result[2])
                                          if result[2] else None))
                    dbMeta[tableName][colName] = \
                        dbMeta[tableName][colName]._replace(
                           minim = theMin,
                           maxim = theMax,
                           averg = theAverg,
                           quTime = queryTime)
                    #average = (resultErr if resultErr
                    #           else (float(result[2]) if result[2] else None))
                    #dbMeta[tableName][colName] = \
                    #    dbMeta[tableName][colName]._replace(
                    #       minim = result[0],
                    #       maxim = result[1],
                    #       averg = average,
                    #       quTime = queryTime)
                else:
                    theQuery = []
                    if self.calcAvg in ['n', 'm', 'a']:
                        theQuery.append("MIN(%s)" % theCol)
                    if self.calcAvg in ['x', 'm', 'a']:
                        theQuery.append("MAX(%s)" % theCol)
                    if self.calcAvg in ['g', 'a']:
                        theQuery.append("AVG(%s)" % theCol)

                    self.cleanCache()
                    queryBeg = makeDateTime()
                    result = self.queryMySql(
                        ','.join(theQuery), theTab, firstOnly=True)
                    #result = self.queryMySql(
                    #    "MIN(%s), MAX(%s), AVG(%s)" % (
                    #    theCol, theCol, theCol),
                    #    theTab, firstOnly=True)
                    queryEnd = makeDateTime()
                    queryTime = "{%s}" % str(queryEnd - queryBeg)
                    print(result, queryTime)
                    if self.calcAvg in ['n']:
                        theMin = result[0]
                        theMax = None
                        theAverg = None
                    elif self.calcAvg in ['x']:
                        theMin = None
                        theMax = result[0]
                        theAverg = None
                    elif self.calcAvg in ['g']:
                        theMin = None
                        theMax = None
                        theAverg = (float(result[0]) if result[0] else None)
                    elif self.calcAvg in ['m']:
                        theMin = result[0]
                        theMax = result[1]
                        theAverg = None
                    elif self.calcAvg in ['a']:
                        theMin = result[0]
                        theMax = result[1]
                        theAverg = (float(result[2]) if result[2] else None)
                    dbMeta[tableName][colName] = \
                        dbMeta[tableName][colName]._replace(
                           minim = theMin,
                           maxim = theMax,
                           averg = theAverg,
                           quTime = queryTime)

                    #dbMeta[tableName][colName] = \
                    #    dbMeta[tableName][colName]._replace(
                    #       minim = result[0],
                    #       maxim = result[1],
                    #       averg = (float(result[2]) if result[2] else None),
                    #       quTime = queryTime)

                # write stats into file
                if self.calcAvg in ['n', 'x', 'g']:
                    outFormat = "%s: %r %s"
                    if self.calcAvg in ['n']:
                        outText = [dbMeta[tableName][colName].minim]
                    elif  self.calcAvg in ['x']:
                        outText = [dbMeta[tableName][colName].maxim]
                    elif  self.calcAvg in ['g']:
                        outText = [dbMeta[tableName][colName].averg]
                elif self.calcAvg in ['m']:
                    outFormat = "%s: %r %r %s"
                    outText = [dbMeta[tableName][colName].minim,
                               dbMeta[tableName][colName].maxim]
                elif self.calcAvg in ['a']:
                    outFormat = "%s: %r %r %r %s"
                    outText = [dbMeta[tableName][colName].minim,
                               dbMeta[tableName][colName].maxim,
                               dbMeta[tableName][colName].averg]

                statsOutFile.writetheline(outFormat % tuple(
                    [colName] + outText + [dbMeta[tableName][colName].quTime]))
                #outFormat = "%s: %r %r %r %s"
                #statsOutFile.writetheline(outFormat % (colName,
                #    dbMeta[tableName][colName].minim,
                #    dbMeta[tableName][colName].maxim,
                #    dbMeta[tableName][colName].averg,
                #    dbMeta[tableName][colName].quTime))
            statsOutFile.writetheline('')
            progress.testForOutput()

        # close the connection
        self.disconnectMySql()

        # close stats file
        statsOutFile.close()

        Logger.addMessage("finished.")
        
    #--------------------------------------------------------------------------

    def cleanCache(self):
        if self.server in ["MYSQL", "QSERV"]:
            self.cursor.execute("RESET QUERY CACHE")
            self.cursor.execute("FLUSH QUERY CACHE")
        else:
            self.archive._executeScript("DBCC FREEPROCCACHE")
            self.archive._executeScript("DBCC DROPCLEANBUFFERS")

    #--------------------------------------------------------------------------

    def checkDB(self):
        theMySql = self.readSql()
        # connect to DB
        self._connectToDb()
        self.archive.enableDirtyRead()
        dbMeta = defaultdict(lambda : defaultdict(list))

        # open stats file
        statsOutFile = File("%s_sqlserv_%s_Stats.txt" % (
            self.archive.database.upper(), self.calcAvg.upper()))
        statsOutFile.wopen(buffering=0)

        # availalbe tables
        availTables = theMySql.keys()
        checkAttr = ["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
                     "NUMERIC_PRECISION","NUMERIC_PRECISION_RADIX"]
        self.cleanCache()
        result = self.archive.query(
            "%s" % ','.join(checkAttr),
            "INFORMATION_SCHEMA.COLUMNS")
        for tabName, colName, datType, numPrec, numPrecRadix in result:
            dbMeta[tabName][colName] = DbCopy.Stats(
                tabName, colName, datType, numPrec, numPrecRadix, 0, 0 ,0,
                '-')

        # for restart with some results already done
        # include pattern
        tmpTables = []
        for tabName in availTables:
            if tabName.startswith(DbCopy.tabPattern):
                tmpTables.append(tabName)
        availTables = sorted(tmpTables)

        # exclude pattern
        tmpTables = []
        for tabName in availTables:
            if not tabName.startswith(DbCopy.exclPattern):
                tmpTables.append(tabName)
        availTables = sorted(tmpTables)

        progress = ForLoopMonitor(availTables)
        for tableName in sorted(availTables):
            Logger.addMessage("Checking %s..." % tableName)
            # num rows
            self.cleanCache()
            queryBeg = makeDateTime()
            numRows = self.archive.queryNumRows(tableName)
            queryEnd = makeDateTime()
            queryTime = "{%s}" % str(queryEnd - queryBeg)
            print("[%s rows] %s" % (numRows, queryTime))
            statsOutFile.writetheline("%s: [%d rows] %s" % (
                tableName, numRows, queryTime))

            for colName in sorted(dbMeta[tableName]):
                print("%s.%s: %s  " % (tableName, colName,
                                       dbMeta[tableName][colName].datType),
                      end="")
                # get stats by type
                if dbMeta[tableName][colName].datType == "varchar":
                    theQuery = []
                    if self.calcAvg in ['n', 'm', 'a']:
                        theQuery.append("MIN(LEN(%s))" % colName)
                    if self.calcAvg in ['x', 'm', 'a']:
                        theQuery.append("MAX(LEN(%s))" % colName)
                    if self.calcAvg in ['g', 'a']:
                        theQuery.append(
                            "CAST(AVG(1.0*LEN(%s)) AS DECIMAL(38,6))" % colName)

                    self.cleanCache()
                    queryBeg = makeDateTime()
                    result = self.archive.query(
                        ','.join(theQuery), tableName, firstOnly=True)

                    #result = self.archive.query(
                    #    "min(len(%s)), max(len(%s)), "
                    #    "CAST(avg(1.0*len(%s)) AS DECIMAL(38,6))" % (
                    #    colName, colName, colName),
                    #    tableName, firstOnly=True)
                    queryEnd = makeDateTime()
                    queryTime = "{%s}" % str(queryEnd - queryBeg)
                    print(result, queryTime)
                    if self.calcAvg in ['n']:
                        theMin = result[0]
                        theMax = None
                        theAverg = None
                    elif self.calcAvg in ['x']:
                        theMin = None
                        theMax = result[0]
                        theAverg = None
                    elif self.calcAvg in ['g']:
                        theMin = None
                        theMax = None
                        theAverg = (float(result[0]) if result[0] else None)
                    elif self.calcAvg in ['m']:
                        theMin = result[0]
                        theMax = result[1]
                        theAverg = None
                    elif self.calcAvg in ['a']:
                        theMin = result[0]
                        theMax = result[1]
                        theAverg = (float(result[2]) if result[2] else None)
                    dbMeta[tableName][colName] = \
                        dbMeta[tableName][colName]._replace(
                           minim = theMin,
                           maxim = theMax,
                           averg = theAverg,
                           quTime = queryTime)

                    #dbMeta[tableName][colName] = \
                    #    dbMeta[tableName][colName]._replace(
                    #       minim = result[0],
                    #       maxim = result[1],
                    #       averg = result[2],
                    #       quTime = queryTime)
                elif dbMeta[tableName][colName].datType == "datetime":
                    theQuery = []
                    if self.calcAvg in ['n', 'm', 'a']:
                        theQuery.append("MIN(%s)" % colName)
                    if self.calcAvg in ['x', 'm', 'a']:
                        theQuery.append("MAX(%s)" % colName)

                    self.cleanCache()
                    queryBeg = makeDateTime()
                    result = self.archive.query(
                        ','.join(theQuery), tableName, firstOnly=True)
                    #result = self.archive.query(
                    #    "min(%s), max(%s)" % (
                    #    colName, colName),
                    #    tableName, firstOnly=True)
                    queryEnd = makeDateTime()
                    queryTime = "{%s}" % str(queryEnd - queryBeg)
                    print(result, queryTime)

                    if self.calcAvg in ['n']:
                        theMin = str(result[0])
                        theMax = None
                        theAverg = None
                    elif self.calcAvg in ['x']:
                        theMin = None
                        theMax = str(result[0])
                        theAverg = None
                    elif self.calcAvg in ['g']:
                        theMin = None
                        theMax = None
                        theAverg = '----'
                    elif self.calcAvg in ['m']:
                        theMin = str(result[0])
                        theMax = str(result[1])
                        theAverg = None
                    elif self.calcAvg in ['a']:
                        theMin = str(result[0])
                        theMax = str(result[1])
                        theAverg = '----'
                    dbMeta[tableName][colName] = \
                        dbMeta[tableName][colName]._replace(
                           minim = theMin,
                           maxim = theMax,
                           averg = theAverg,
                           quTime = queryTime)

                    #dbMeta[tableName][colName] = \
                    #    dbMeta[tableName][colName]._replace(
                    #       minim = str(result[0]),
                    #       maxim = str(result[1]),
                    #       averg = '----',
                    #       quTime = queryTime)
                elif "int" in dbMeta[tableName][colName].datType:
                    resultErr = None
                    theQuery = []
                    if self.calcAvg in ['n', 'm', 'a']:
                        theQuery.append("MIN(%s)" % colName)
                    if self.calcAvg in ['x', 'm', 'a']:
                        theQuery.append("MAX(%s)" % colName)
                    if self.calcAvg in ['g', 'a']:
                        theQuery.append(
                            "AVG(1.0*CAST(%s AS bigint))" % colName)

                    try:
                        self.cleanCache()
                        queryBeg = makeDateTime()
                        result = self.archive.query(
                            ','.join(theQuery), tableName, firstOnly=True)
                        #result = self.archive.query(
                        #    "min(%s), max(%s), "
                        #    "avg(1.0*cast(%s as bigint))" % (
                        #    colName, colName, colName),
                        #    tableName, firstOnly=True)
                    except Exception as error:
                        if "Arithmetic overflow" in str(error):
                            self.cleanCache()
                            queryBeg = makeDateTime()
                            result = self.archive.query(
                                ','.join(theQuery[:2]), tableName,
                                firstOnly=True)
                            #result = self.archive.query(
                            #    "min(%s), max(%s)" % (
                            #    colName, colName),
                            #    tableName, firstOnly=True)
                            resultErr = '---'
                        else:
                            print("***",error,"***")
                            raise SystemExit()
                    queryEnd = makeDateTime()
                    queryTime = "{%s}" % str(queryEnd - queryBeg)
                    print(result, queryTime)
                    if self.calcAvg in ['n']:
                        theMin = str(result[0])
                        theMax = None
                        theAverg = None
                    elif self.calcAvg in ['x']:
                        theMin = None
                        theMax = str(result[0])
                        theAverg = None
                    elif self.calcAvg in ['g']:
                        theMin = None
                        theMax = None
                        theAverg = (resultErr if resultErr else result[0])
                    elif self.calcAvg in ['m']:
                        theMin = str(result[0])
                        theMax = str(result[1])
                        theAverg = None
                    elif self.calcAvg in ['a']:
                        theMin = str(result[0])
                        theMax = str(result[1])
                        theAverg = (resultErr if resultErr else result[2])

                    dbMeta[tableName][colName] = \
                        dbMeta[tableName][colName]._replace(
                           minim = theMin,
                           maxim = theMax,
                           averg = theAverg,
                           quTime = queryTime)
                    #average = (resultErr if resultErr else result[2])
                    #dbMeta[tableName][colName] = \
                    #    dbMeta[tableName][colName]._replace(
                    #       minim = result[0],
                    #       maxim = result[1],
                    #       averg = average,
                    #       quTime = queryTime)
                else:
                    theQuery = []
                    if self.calcAvg in ['n', 'm', 'a']:
                        theQuery.append("MIN(%s)" % colName)
                    if self.calcAvg in ['x', 'm', 'a']:
                        theQuery.append("MAX(%s)" % colName)
                    if self.calcAvg in ['g', 'a']:
                        theQuery.append("AVG(%s)" % colName)

                    self.cleanCache()
                    queryBeg = makeDateTime()
                    result = self.archive.query(
                        ','.join(theQuery), tableName, firstOnly=True)
                    #result = self.archive.query(
                    #    "min(%s), max(%s), avg(%s)" % (
                    #    colName, colName, colName),
                    #    tableName, firstOnly=True)
                    queryEnd = makeDateTime()
                    queryTime = "{%s}" % str(queryEnd - queryBeg)
                    print(result, queryTime)
                    if self.calcAvg in ['n']:
                        theMin = result[0]
                        theMax = None
                        theAverg = None
                    elif self.calcAvg in ['x']:
                        theMin = None
                        theMax = result[0]
                        theAverg = None
                    elif self.calcAvg in ['g']:
                        theMin = None
                        theMax = None
                        theAverg = (float(result[0]) if result[0] else None)
                    elif self.calcAvg in ['m']:
                        theMin = result[0]
                        theMax = result[1]
                        theAverg = None
                    elif self.calcAvg in ['a']:
                        theMin = result[0]
                        theMax = result[1]
                        theAverg = (float(result[2]) if result[2] else None)
                    dbMeta[tableName][colName] = \
                        dbMeta[tableName][colName]._replace(
                           minim = theMin,
                           maxim = theMax,
                           averg = theAverg,
                           quTime = queryTime)

                    #dbMeta[tableName][colName] = \
                    #    dbMeta[tableName][colName]._replace(
                    #       minim = result[0],
                    #       maxim = result[1],
                    #       averg = result[2],
                    #       quTime = queryTime)

                # write stats into file
                if self.calcAvg in ['n', 'x', 'g']:
                    outFormat = "%s: %r %s"
                    if self.calcAvg in ['n']:
                        outText = [dbMeta[tableName][colName].minim]
                    elif  self.calcAvg in ['x']:
                        outText = [dbMeta[tableName][colName].maxim]
                    elif  self.calcAvg in ['g']:
                        outText = [dbMeta[tableName][colName].averg]
                elif self.calcAvg in ['m']:
                    outFormat = "%s: %r %r %s"
                    outText = [dbMeta[tableName][colName].minim,
                               dbMeta[tableName][colName].maxim]
                elif self.calcAvg in ['a']:
                    outFormat = "%s: %r %r %r %s"
                    outText = [dbMeta[tableName][colName].minim,
                               dbMeta[tableName][colName].maxim,
                               dbMeta[tableName][colName].averg]

                statsOutFile.writetheline(outFormat % tuple(
                    [colName] + outText + [dbMeta[tableName][colName].quTime]))
                #statsOutFile.writetheline("%s: %r, %r, %r %s" % (colName,
                #    dbMeta[tableName][colName].minim,
                #    dbMeta[tableName][colName].maxim,
                #    dbMeta[tableName][colName].averg,
                #    dbMeta[tableName][colName].quTime))
            statsOutFile.writetheline('')
            progress.testForOutput()

        # disconnect from DB
        self._disconnectFromDb()

        # close stats file
        statsOutFile.close()

    Logger.addMessage("finished.")

    #--------------------------------------------------------------------------

    def readSql(self, mode=''):
        theSql = defaultdict(list)
        print("M", self.modus, "m",mode)
        if mode == "outgest":
            pklInFile = PickleFile("allFixes.pkl")
            allFixes = list(pklInFile.pickleRead())[0]

        theFixes = []
        for line in file("sql/MySQL_schema_create.sql"):
            if line.startswith('#'):
                pass
            elif line.lstrip().startswith("drop table"):
                pass
            elif line.lstrip().startswith("create index"):
                pass
            elif line.lstrip().startswith("primary key"):
                pass
            elif line.lstrip().startswith("create table"):
                tableName = line.split()[2].replace('`', '')
                theFixes = (allFixes[tableName] if allFixes else [])
                if mode == "outgest":
                    for newEntry, oldEntry in theFixes:
                        line = line.replace(newEntry, oldEntry)
                tableName = line.split()[2].replace('`', '')
                theSql[tableName] = []
            elif line.lstrip().startswith(");"):
                tableName = ''
            else:
                if mode == "outgest":
                    for newEntry, oldEntry in theFixes:
                        line = line.replace(newEntry, oldEntry)
                theSql[tableName].append(line.split()[0].replace('`', ''))

        if mode == "outgest":
            outFile = File("sql/SQLServer_schema_create.sql")
            outFile.wopen()
            for tableName in theSql:
                outFile.writelines(theSql[tableName])
            outFile.close()

        return theSql

    #--------------------------------------------------------------------------

    def runCommand(self, cmd):
        try:
            outMess = False
            for line in self.runSysCmd(cmd)[0]:
                if line:
                    print(line.strip())
                    outMess = True
            if outMess:
                print()
        except Exception as error:
            print(error)

    #--------------------------------------------------------------------------

    def fixXml(self):
        xmlList = []
        if self.xferlog:
            for line in file(self.xferlog):
                if line.startswith('#'):
                    pass
                else:
                    entry = line.rstrip()
                    xmlList.append(os.path.join(DbCopy.xmlPath, entry))
        else:
            xmlList.extend([os.path.join(DbCopy.xmlPath, item) 
                           for item in dircache.listdir(DbCopy.xmlPath)
                           if item.endswith(".xml")])

        # move views/excluded aside
        allViews = ''
        allExcluded = ''
        if "UKIDSS" in self.fixDbName:
            allViews = DbCopy.ukidssViews
            allExcluded = DbCopy.ukidssExcluded
            matchFunc = partMatch
        elif "BestDR"  in self.fixDbName:
            allViews = DbCopy.sdssViews
            allExcluded = DbCopy.sdssExcluded
            matchFunc = trueMatch
        else:
            Logger.addMessage("Wrong database specified, no views removed!")

        # views
        tmpList = []
        ensureDirExist(DbCopy.viewPath)
        for fileName in xmlList:
            if matchFunc(fileName.replace(DbCopy.xmlPath, ''),
                         allViews, exclude=".hbm.xml"):
                os.rename(
                    fileName,
                    fileName.replace(DbCopy.xmlPath,
                                     DbCopy.viewPath).replace(".xml", ".xmx"))
            else:
                tmpList.append(fileName)
        xmlList = tmpList

        # exluded
        tmpList = []
        ensureDirExist(DbCopy.excludedPath)
        for fileName in xmlList:
            if matchFunc(fileName.replace(DbCopy.xmlPath, ''),
                         allExcluded, exclude=".hbm.xml"):
                os.rename(
                    fileName,
                    fileName.replace(DbCopy.xmlPath,
                                     DbCopy.excludedPath).replace(".xml",
                                                                  ".xmx"))
            else:
                tmpList.append(fileName)
        xmlList = tmpList

        # fix reserved keywords
        numFixed = 0
        for fileName in xmlList:
            tableName = os.path.basename(fileName).partition('.')[0]
            self.isFixed = True
            xmlFile = File(fileName)
            xmlFile.ropen()
            allText = xmlFile.readlines()
            xmlFile.close()
            fixedJavText = self.fixKeys(allText,
                                        DbCopy.reservedJavaKeys,
                                        "Java", tableName)
            fixedMySqlText = self.fixKeys(fixedJavText,
                                          DbCopy.reservedMySqlKeys,
                                          "MySql", tableName)
            newText = fixedMySqlText
            outFileName = (fileName.replace(".xml", ".new.xml")
                           if self.isTrialRun else fileName)
            xmlOutFile = File(outFileName)
            xmlOutFile.wopen()
            xmlOutFile.writelines(newText)
            xmlOutFile.close()
            if self.isFixed:
                Logger.addMessage("fixed %s" % xmlOutFile.name)
                numFixed += 1
            else:
                Logger.addMessage("todo: %s" % xmlOutFile.name)
        pklOutFile = PickleFile("allFixes.pkl")
        pklOutFile.pickleWrite(self.allFixes)

        Logger.addMessage("Fixed %d(%d) files." % (numFixed, len(xmlList)))

    #--------------------------------------------------------------------------

    def fixKeys(self, theText, keyDict, langStr, tableName):
        newText = []
        for line in theText:
            for key in keyDict:
                # property
                if '<property name=\"%s\"' % key.lower() in line.lower():
                    line = self.fixTheKey(line, key, keyDict, langStr,
                                             "property", tableName)
                    

                # class name
                if '<class name=\"schema.%s\" table=\"%s\">' % (
                      key.lower(), key.lower()) in line.lower():
                    line = self.fixTheKey(line, key, keyDict, langStr,
                                             "class", tableName)

                # id name
                if '<id name=\"%s\"' % key.lower() in line.lower():
                    line = self.fixTheKey(line, key, keyDict, langStr,
                                             "id", tableName)

                # column
                if line.lower().startswith('<id') \
                       and 'column=\"%s\"' % key.lower() in line.lower():
                    line = self.fixTheKey(line, key, keyDict, langStr,
                                             "column", tableName)

                # table name
                if 'table=\"%s\">' % key.lower() in line.lower():
                    line = self.fixTheKey(line, key, keyDict, langStr,
                                             "table", tableName)

                # column name
                if line.lstrip().lower().startswith(
                      'name=\"%s\"' % key.lower()):
                    line = self.fixTheKey(line, key, keyDict, langStr,
                                             "column name", tableName)

            # name starting with a number
            if 'name="' in line:
                iS = line.index('name="') + 6
                iE = line[iS:].index('"')
                numKey = line[iS:iS + iE]
                if numKey[0].isdigit():
                    alnumKey = 'X' + numKey
                    self.allFixes[tableName].append(
                        [alnumKey, numKey])
                    line = line[:iS] + alnumKey + line[iS + iE:]
                    Logger.addMessage("Translated number: %s => %s" % (
                        numKey, alnumKey))

            # byte[]
            if 'type=\"byte[]\"' in line:
                line = line.replace('type=\"byte[]\"', 'type=\"blob\"')
                Logger.addMessage("Translated binary: byte[] => blob")
                self.allFixes[tableName].append(
                    ['type=\"blob\"', 'type=\"byte[]\"'])

            newText.append(line)
        return newText

    #--------------------------------------------------------------------------

    def fixTheKey(self, line, key, keyDict, langStr, typeStr, tableName):
        if keyDict[key]:
            if typeStr == "table":
                lB, lT, lR = line.lower().partition('table=\"')
                lsB, lW, lE = lR.lower().partition(key.lower())
                lW = line[len(lB + lT + lsB):-len(lE)]
                newKey = changeCase(key, lW) + "Table"
                lS = lB + lT + lsB
                self.allFixes[newKey].append([newKey, key])
            elif typeStr == "column":
                lB, lT, lR = line.lower().partition('column=\"')
                lsB, lW, lE = lR.lower().partition(key.lower())
                lW = line[len(lB + lT + lsB):-len(lE)]
                newKey = changeCase(keyDict[key], lW)
                lS = lB + lT + lsB
            else:
                lS, lW, lE = line.lower().partition(key.lower())
                lW = line[len(lS):-len(lE)]
                newKey = changeCase(keyDict[key], lW)
            if typeStr not in ("class", "table"):
                self.allFixes[tableName].append([newKey, key])
            line = line[:len(lS)] \
                   + newKey \
                   + line[-len(lE):]
            Logger.addMessage("Translated %s: %s => %s" % (
                typeStr, key, newKey))
            self.isFixed = True
        else:
            self.isFixed = False
            Logger.addMessage(
                "No translation for %sKey: %s" % (langStr, key))
        return line
    
    #--------------------------------------------------------------------------

    def fixJava(self):
        javaList = []
        if self.xferlog:
            for line in file(self.xferlog):
                if line.startswith('#'):
                    pass
                else:
                    entry = line.rstrip()
                    javaList.append(os.path.join(DbCopy.javaPath, entry))
        else:
            javaList.extend([os.path.join(DbCopy.javaPath, item) 
                           for item in dircache.listdir(DbCopy.javaPath)
                           if item.endswith(".java")])

        # remove full constructor
        counter = 0
        counterPK = 0
        for fileName in javaList:
            if fileName.endswith("PK.java"):
                counterPK += 1
            else:
                javaFile = File(fileName)
                javaFile.ropen()
                allText = javaFile.readlines()
                javaFile.close()
                newText = []
                outPut = "on"
                for line in allText:
                    if "/** full constructor */" in line:
                        outPut = "off"
                    if "/** default constructor */" in line:
                        outPut = "on"
                    if outPut == "on":
                        newText.append(line)
                outFileName = (fileName.replace(".java", ".new.java")
                               if self.isTrialRun else fileName)
                javaOutFile = File(outFileName)
                javaOutFile.wopen()
                javaOutFile.writelines(newText)
                javaOutFile.close()
                Logger.addMessage("fixed %s" % javaOutFile.name)
                counter += 1
                
        Logger.addMessage("fixed %d files. (%d PK files)" % (
            counter, counterPK))

    #--------------------------------------------------------------------------

    def fixLineBreak(self):
        theInDir = os.path.join(DbCopy.csvOutPath, "%s.csv" % self.dbName)
        print(theInDir)
        allFiles = sorted([os.path.join(theInDir, item) 
                           for item in dircache.listdir(theInDir)
                           if item.endswith("000998.csv.dat") \
                           and item.startswith("Al")])
        theOutDir = theInDir.replace(".csv", "_fixedcr.csv").replace("disk57", "disk59")
        print(theOutDir)
        #allFiles = allFiles[:10]
        counter = 0
        if self.isTrialRun:
            print(allFiles)
            print(theInDir, "==>", theOutDir)
            counter = len(allFiles)
        else:
            for fileName in allFiles:
                Logger.addMessage("Checking %s..." % fileName)
                theFile = File(os.path.join(theInDir, fileName))
                #theFile.fobj = open(theFile.name, 'rb')
                theFile.ropen()
                theData = theFile.readlines()
                theFile.close()
                for line in theData:
                    if line.endswith('\r\n'):
                        print(counter,':',line)
                        Logger.addMessage("fixed %s" % fileName)
                        counter += 1
        Logger.addMessage("fixed %d files." % counter)
        
    #--------------------------------------------------------------------------

    def splitFullSchema(self):
        prefix = ("SQLServer" if self.modus == "sqlserver" else "MySQL")
        fullSchema = File(os.path.join(DbCopy.sqlPath,
                                       "%s_schema_create.sql" % prefix))
        fullSchema.ropen()
        allSchemaLines = fullSchema.readlines()
        fullSchema.close()
        allTables = defaultdict()

        # get all tables
        for line in allSchemaLines:
            if line.startswith("create table"):
                theTable = line.replace("create table", '').replace(
                    '(', '').strip()
                allTables[theTable] = defaultdict(list)
                
        for line in allSchemaLines:
            if line.startswith("drop table"):
                theTable = line.strip()[:-1].partition(' exists ')[2]
                allTables[theTable.strip()]["drop"] = [line]
            elif line.startswith("create index"):
                theTable = line.strip()[:-1].partition(
                    ' on ')[2].partition(' (')[0]
                allTables[theTable.strip()]["indx"].append(line)
            elif line.startswith("create table"):
                theTable = line.replace("create table", '').replace(
                    '(', '').strip()
                uniqued = False
                allTables[theTable.strip()]["create"].append(line)
            #elif line.strip() != ");":
            #    allTables[theTable.strip()]["create"].append(line)
            else:
                if "unique" in line:
                    line = line.replace(" unique", '')
                    uniqued = True
                    allTables[theTable.strip()]["uniq"].append(
                        DbCopy.uniqueStatements[theTable.strip()])
                if theTable.strip() in ["Multiframe"]:
                    words = line.strip().split()
                    if "TEXT" in words:
                        line = line.replace("TEXT", "VARCHAR(255)")
                allTables[theTable.strip()]["create"].append(line)

        if self.modus == "sqlserver":
            #checkTables = ["`Release`"]
            checkTables = allTables.keys()
            for theTable in allTables:
                if theTable in checkTables:
                    Logger.addMessage("Rewriting %s..." % theTable)
                    theAttrList = self.getAttributes(theTable.replace('`',''))
                    transDict = defaultdict()
                    for line in allTables[theTable]["create"]:
                        attr = line.split()[0].replace('`','')
                        attr2 = ' '.join(line.split()[:2]).replace('`','')
                        transDict[attr] = (line)
                        transDict[attr2] = (line)
                    newLines = [transDict["create"]]
                    for theAttr in theAttrList:
                        newLines.append(transDict[theAttr])
                    if "primary key" in transDict:
                        newLines.append(transDict["primary key"])
                    newLines.append(transDict[");"])
                    allTables[theTable]["create"] = newLines

        # write to files
        for theTable in allTables:
            prefix = ("SQLServer" if self.modus == "sqlserver" else "MySQL")
            theFile = File(os.path.join(
                DbCopy.sqlPath,
                "%s_%sSchema.sql" % (prefix, theTable.replace('`',''))))
            theFile.wopen()
            theFile.writelines(allTables[theTable]["drop"])
            theFile.writelines(allTables[theTable]["create"])
            theFile.writelines(allTables[theTable]["indx"])
            theFile.writelines(allTables[theTable]["uniq"])
            theFile.close()

        Logger.addMessage("Created schemas for %d tables." % len(allTables))

    #--------------------------------------------------------------------------

    def getAttributes(self, tableName):
        if self.database:
            # connect to DB
            self._connectToDb()
            self.archive.enableDirtyRead()
            attrList = self.archive.queryColumnNames(tableName)
            # disconnect from DB
            self._disconnectFromDb()
            return attrList

#------------------------------------------------------------------------------

def trueMatch(targetStr, keyList , exclude=''):
    return any(x == targetStr.replace(exclude, '') for x in keyList)

#------------------------------------------------------------------------------

def partMatch(targetStr, keyList, exclude=''):
    return any(x in targetStr.replace(exclude, '') for x in keyList)

#------------------------------------------------------------------------------

def changeCase(word, refWord):
    outText = ''
    if "match" in word.lower():
        print(">>>>>",word,"::REF:",refWord)
    for i, letter in enumerate(word):
        try:
            if refWord[i].isupper():
                outText += letter.upper()
            else:
                outText += letter.lower()
        except IndexError:
            if refWord[-1].isupper():
                outText += letter.upper()
            else:
                outText += letter.lower()
    return outText


#------------------------------------------------------------------------------
#$Id: File.py 11624 2017-03-06 16:05:51Z EckhardSutorius $
"""
   General class for file handling.

   @author: E. Sutorius
   @org:    WFAU, IfA, University of Edinburgh
"""
#------------------------------------------------------------------------------
class File(object):
    """general File class"""
    def __init__(self, fname):
        """Split name into parts, eg.:
        /diskNN/wsa/ingest/fits/20050101_v0/w20050101_01234_sf_st_cat.fits
        {           path                   }{         base                }
        {        topdir        }{  subdir  }{         root           }{ext}
        { diskdir  }{ commondir}{          fileID (w/o cat)     }
                                            { sdate }{runno}{ ftype  }
        """
        self.name = fname
        self.path, self.base = os.path.split(self.name)
        self.root, self.ext =  os.path.splitext(self.base)
        splitPath = self.name.split('/')
        addL = (1 if "test" in splitPath[:4] else 0)
        if len(splitPath) > 5 + addL:
            self.datedir = splitPath[5 + addL]
        if len(splitPath) > 5 + addL:
            if self.ext.endswith((".0", ".1", ".2", ".3",".4")) or \
                   ('.' not in self.base and self.base.upper() != self.base):
                self.allsubs = '/'.join(splitPath[5 + addL:])
            else:
                self.allsubs = '/'.join(splitPath[5 + addL:-1])
        else:
            self.allsubs = ''
        self.topdir = '/'.join(splitPath[:5 + addL])
        self.subdir = (splitPath[5 + addL] if len(splitPath) > 5 + addL
                       else os.path.split(self.path)[-1])
            
        self.diskdir = '/'.join(splitPath[:3 + addL])
        self.commondir = '/'.join(splitPath[3 + addL:5 + addL])
        #self.topdir, self.subdir = os.path.split(self.path)
        #self.diskdir = '/'.join(self.topdir.split('/', 3)[:-1])
        #self.commondir = self.topdir.split('/', 3)[-1]
        self.sdate, self.runno, self.ftype = (self.root.split('_', 2) + \
                                              ['', '', ''])[:3]
        self.fileID = os.path.join(
            self.subdir, self.root.replace("_cat", '').replace("_fix", ''))

        self.fobj = None

    #--------------------------------------------------------------------------

    def ropen(self):
        """open for reading"""
        self.fobj = open(self.name,'r')

    #--------------------------------------------------------------------------

    def wopen(self,buffering=-1):
        """open for writing"""
        self.fobj = open(self.name,'w',buffering)

    #--------------------------------------------------------------------------

    def close(self):
        """close the file object"""
        self.fobj.close()

    #--------------------------------------------------------------------------

    def readlines(self, strip=True, commentChar=None, omitEmptyLines=False,
                  findValues=[], omitValues=[]):
        """read all lines and strip the linebreaks"""
        lines = self.fobj.readlines()
        if strip:
            lines = [l.rstrip() for l in lines]
        if commentChar:
            lines = [l for l in lines if not l.startswith(commentChar)][:]
        if omitEmptyLines:
            lines = [l for l in lines if len(l)>0][:]
        if findValues:
            lines = [l for l in lines if any(v in l for v in findValues)]
        if omitValues:
            lines = [l for l in lines if all(v not in l for v in omitValues)]
        return lines

    #--------------------------------------------------------------------------

    def readline(self, strip=False):
        """read one line (including linebreak)"""
        line = self.fobj.readline()
        return line.rstrip() if strip else line

    #--------------------------------------------------------------------------

    def writelines(self, lines):
        """write all lines in given list, appending linebreaks"""
        for l in lines:
            self.fobj.write(l + '\n')

    #--------------------------------------------------------------------------

    def writeline(self, the_line):
        """write given line, no linebreak appended"""
        self.fobj.write(the_line)

    #--------------------------------------------------------------------------

    def writetheline(self, the_line):
        """write given line with linebreak"""
        self.fobj.write(the_line + '\n')

#------------------------------------------------------------------------------

class PickleFile(File):
    """Logfile holding the information to ingest csv/binary data and to
    update the curation histories accordingly.
    """
    def __init__(self, fileName):
        """
        @param fileName: Name of the pickle data file.
        @type  fileName: str
        """
        super(PickleFile, self).__init__(fileName)

    #--------------------------------------------------------------------------

    def pickleRead(self):
        """Read all objects from the file.
        """
        self.lines = []
        ifh = open(self.name, 'r')
        while True:
            try:
                yield cPickle.load(ifh)
            except EOFError:
                break
        ifh.close()

    #--------------------------------------------------------------------------

    def pickleWrite(self, *objects):
        """
        @param objects: List of objects to be written to the file.
        @type  objects: Python objects
        """
        ofh = open(self.name, 'w')
        for obj in objects:
            cPickle.dump(obj, ofh)
        ofh.close()

#------------------------------------------------------------------------------
# Logger
"""
   Logging of run-time messages. Messages are logged through calling static
   methods of the singleton class L{Logger}. Message logs are kept in memory
   until an instance of a Logger class object is destroyed, at which point the
   log is written to a file - over-writing any previous log file with the same
   name. There is also an option that writes the log to file continuously. We
   do not append to pre-existing log files with the same name, as this would be
   confusing, especially when test databases are reset.

   Also, a L{ForLoopMonitor} class is provided to display percentage completion
   progress of for-loops. Use to monitor lengthy For-Loops, providing the
   minimal logged output of percentage progess to completion.
"""
#------------------------------------------------------------------------------

class Logger(object):
    """
    A monostate class for run-time logging of messages. Just call
    L{Logger.addMessage()} to log a message. To output messages to a file you
    just need to create an instance of a Logger object, and when that object
    falls out-of-scope the log file will be written. Alternatively, if the
    isVerbose option is set to False, then the log file will be written too
    continuously whilst an initialised Logger object is in scope.
    """
    # Define public class variable default values (access as Logger.varName)

    #: Archive all existing logs for the current database upon initialisation?
    archive = False
    #: Echo log to terminal?
    echoOn = True
    #: Is this a verbose log? If not, continually write the log to file.
    isVerbose = True
    #: Full path to the log file.
    pathName = ''
    #: Requires a line clear before logging?
    reqClearLine = False

    # Define private member variable default values (access prohibited)
    _logText = []        #: List of every log entry.
    _logTime = []        #: List of the time of every log entry.

    #--------------------------------------------------------------------------

    def __init__(self, fileName, path=None):
        """
        Initialise a Logger object by setting the full path to the log file.
        The path is existed to make sure it exists, and if isVerbose is set to
        False then the log file is initialised with messages logged to date.

        @param fileName: Name of the log file.
        @type  fileName: str
        @param path:     Log file directory path. Defaults to system log path.
        @type  path:     str

        """
        defaultLogFilePath = "./logs"
        ensureDirExist(defaultLogFilePath)
        
        # @@TODO: This is a temporary solution to put test db logs in separate
        # directories. Will redesign constructor to Logger.
        if fileName.lower().startswith('test'):
            fileName = fileName[0].upper() \
                     + fileName[1:].replace('_', os.sep, 1)

        Logger.pathName = os.path.join(path or defaultLogFilePath, fileName)
        path = os.path.dirname(Logger.pathName)

        # Ensure directory exists - if not swap to home directory
        try:
            if not os.path.exists(path):
                os.mkdir(path)
                os.chmod(path, 0777)
        except Exception as error:
            self._revertToHomePath(error)
        else:
            if Logger.archive and path != os.path.dirname(defaultLogFilePath):
                files = os.listdir(path)
                if any(name.endswith('log') for name in files):
                    exts = ('tar', 'tar.gz')
                    version = sum(1 for name in files if name.endswith(exts))
                    os.system("cd %s; tar cf logs_v%s.tar *.log" %
                              (path, version))
                    os.system("rm %s/*.log" % path)

        # Initialise log file with logged text to date if log isn't verbose
        if not Logger.isVerbose:
            try:
                Logger.dump(file(Logger.pathName, 'w'))
            except IOError:
                # If there's an error do nothing now, the log is still written
                # upon destruction
                pass

    #--------------------------------------------------------------------------

    def __del__(self):
        """ Writes all messages that have been currently logged.
        """
        try:
            # Even if log already exists (due to appending), the whole file is
            # re-written just in case it's incomplete following a write error
            if Logger.pathName:
                Logger.dump(file(Logger.pathName, 'w'))
        except IOError as error:
            # Write to home-dir if log directory is unavailable
            self._revertToHomePath(error)
            Logger.dump(file(Logger.pathName, 'w'))
        finally:
            # Reset log - as old log is saved, allows new logs to be created.
            Logger.reset()
            Logger.pathName = ''

    #--------------------------------------------------------------------------

    def _revertToHomePath(self, exception):
        """
        Reverts the log file path to the home directory in case of error.

        @param exception: The exception that mandates to this reversion.
        @type  exception: Exception

        """
        Logger.pathName = os.path.join(os.getenv('HOME'),
                                       os.path.basename(Logger.pathName))
        Logger.addMessage(
            "<Warning> Writing log-file to %s, because of following error: %s"
            % (Logger.pathName, exception))

    #--------------------------------------------------------------------------

    @staticmethod
    def addMessage(message, alwaysLog=True, echoOff=False):
        """
        Log a message. This is displayed to stdout if in "echoOn" mode, and
        also written to the log file if not in "isVerbose" mode.

        @param message:   The message to log.
        @type  message:   str
        @param alwaysLog: If False, then this message will only be logged when
                          isVerbose is True.
        @type  alwaysLog: bool
        @param echoOff:   Don't display this message to the screen - just write
                          it to the log file.
        @type  echoOff:   bool

        """
        if Logger.isVerbose or alwaysLog:
            utils.WordWrapper.indent = 26
            time = str(mx.DateTime.now())
            Logger._logTime.append(time)
            Logger._logText.append("# %s: %s\n" % (time, message))
            if Logger.echoOn and not echoOff:
                try:
                    if Logger.reqClearLine:
                        sys.stdout.write("\33[2K\r")
                        sys.stdout.flush()
                        Logger.reqClearLine = False

                    print("# %s: %s" % (time, utils.WordWrapper.wrap(message)))
                except IOError:
                    # If we can't print to stdout, there's no point sending an
                    # exception to stdout, hopefully the message will be
                    # written to file anyway - not worth interrupting the task,
                    # but note in the log that it failed.
                    Logger._logText[-1] = \
                      Logger._logText[-1].replace('# ', '! ', 1) \
                      + " [print to screen failed]"

            # Update log file if defined and the log isn't verbose
            if not Logger.isVerbose and Logger.pathName:
                try:
                    file(Logger.pathName, 'a').writelines(Logger._logText[-1])
                except IOError:
                    # If there's an error do nothing now, the log is still
                    # written upon destruction
                    pass

    #--------------------------------------------------------------------------

    @staticmethod
    def dump(handle):
        """
        Write all messages that have been currently logged.

        @param handle: File handle to which to send the messages.
        @type  handle: file

        """
        handle.writelines(Logger._logText)
        try:
            filePath = handle.name
        except AttributeError:
            pass  # Not dumping to file system so no need to adjust permissions
        else:
            try:
                os.chmod(handle.name, 0644)
            except (OSError, IOError) as error:
                print("Cannot chmod: %s" % error)

#------------------------------------------------------------------------------

class ForLoopMonitor(object):
    """
    Monitors lengthy For-Loops by logging progress at 10% intervals. (Following
    an initial 1% interval in case loop is very time consuming). Also displayed
    at each marker is an estimated time until completion (ETC). Two estimates
    are provided, one based the overall speed of the loop, the other based on
    the latest speed of the loop since the last marker.
    """
    #--------------------------------------------------------------------------
    # Define class constants (access as ForLoopMonitor.varName)

    #: List of progress display markers - 1% then every 10%.
    markers = [1] + range(10, 101, 10)

    #--------------------------------------------------------------------------
    # Define private member variable default values (access prohibited)

    _end = 0            #: Total number of loops required.
    _isLogged = True    #: Progress through for-loop should be logged?
    _isOnDemand = False #: Return progress on demand?
    _lastPercent = 0.0  #: Percentage complete at last time marker.
    _lastTime = 0.0     #: Time at last marker.
    _marked = dict.fromkeys(markers, False)
    """ Dictionary of flags denoting which markers have been passed. """
    _progress = 0       #: Current loop number.
    _startTime = 0.0    #: Time at which for-loop began.
    _threadID = ''      #: Identification string for this thread.

    #--------------------------------------------------------------------------

    def __init__(self, aList, minLoops=2, threadID=''):
        """
        Initialise monitor by passing it the list to be iterated over, so the
        total number loops can be determined. This must be called immediately
        before the start of the loop as timing starts on initialisation.

        @param aList:      Sequence that the For-Loop is looping over; or its
                           length.
        @type  aList:      list or int
        @param minLoops:   Minimum number of loops for logging of progress.
        @type  minLoops:   int
        @param threadID:   An ID string to identify current thread.
        @type  threadID:   str

        """
        self._end = aList if isinstance(aList, int) else len(aList)
        if self._end < minLoops:
            self._isLogged = False
            return

        if threadID:
            self._threadID = threadID + ': '

        # Set-up a local copy of the marked dictionary for this object
        self._marked = dict(ForLoopMonitor._marked)
        self._startTime = time.time()
        self._lastTime = self._startTime

    #--------------------------------------------------------------------------

    def postMessage(self, msg=''):
        """
        Log a progress update on demand; at end of loop-body.
        
        @param msg: Display this message first.
        @type  msg: str
        
        """
        if self._isLogged:
            self._isOnDemand = True
            etc = self.testForOutput()
            msg += " (%s of %s" % (self._progress, self._end)
            if self._progress != self._end:
                msg += "; " + etc

            Logger.addMessage(msg + ")")

    #--------------------------------------------------------------------------

    def preMessage(self, msg='', offset=0):
        """
        Log a progress update on demand; at start of loop-body.
        
        @param msg: Display this message first.
        @type  msg: str
        @param offset: Optional positional offset for the X of Y message.
        @type  offset: int
        
        """
        if not self._isLogged:
            return

        if not self._isOnDemand:
            self._isOnDemand = True
            etc = ''
        else:
            etc = "(%s)" % self.testForOutput()

        msg += " %s of %s..." % (self._progress + 1 + offset,
                                 self._end + offset)
        if etc:
            msg += " " + etc

        Logger.addMessage(msg)

    #--------------------------------------------------------------------------

    def testForOutput(self, msg=''):
        """ 
        Log progress update when sufficient progress is made.
        
        @param msg: Display this message along with a continuous update.
        @type  msg: str
        
        """
        if not self._isLogged:
            return

        self._progress += 1
        percentDone = 100.0 * self._progress / self._end
        atNextMarker = self._atNextMarker(percentDone)
        isContinuous = msg and not Logger.isVerbose

        if atNextMarker or self._isOnDemand or isContinuous:
            now = time.time()
            overallETA = now + ((now - self._startTime) / percentDone *
                                (100 - percentDone))

            if self._isOnDemand or atNextMarker:
                latestETA = now + ((now - self._lastTime) /
                  (percentDone - self._lastPercent) * (100 - percentDone))

                self._lastTime = now
                self._lastPercent = percentDone

            if isContinuous and not atNextMarker:
                etcMsg = "Overall ETC " + time.ctime(overallETA)
            else:
                msg = ''
                etcMsg = "ETC (latest) " + time.ctime(latestETA)

                # Support word wrap
                if utils.WordWrapper.wrapWidth and not self._threadID \
                  and not self._isOnDemand:
                    etcMsg += '\n' + ' ' * 44

                etcMsg += " (overall) " + time.ctime(overallETA)

            if self._isOnDemand:
                return etcMsg

            progressFmt = "Progress = " + ("%3d" if atNextMarker else "%d")
            progressFmt += "%% "
            if msg:
                msg += ' '

            msg = self._threadID + msg + progressFmt % int(percentDone)
            msg += etcMsg

            if atNextMarker:
                Logger.addMessage(msg)
            else:
                try:
                    numCols = int(os.popen("stty size").read().split()[-1])
                    sys.stdout.write("\33[2K\r" + msg[:numCols])
                    sys.stdout.flush()
                except IOError:
                    pass  # Not worth failing over a live update message
                else:
                    Logger.reqClearLine = True

    #--------------------------------------------------------------------------

    def _atNextMarker(self, percent):
        """
        @param percent: Total progress made as a percentage.
        @type  percent: float

        @return: True if progress is sufficient to warrant logging a progress
                 update message
        @rtype:  bool

        """
        for marker in ForLoopMonitor.markers:
            if not self._marked[marker] and percent >= marker:
                self._marked[marker] = True
                return True

        return False

#------------------------------------------------------------------------------
# Utilities
"""
  General utility functions. Mostly concerning manipulation of Python objects,
   and the file system.

   @author: I.A. Bond
   @org:    WFAU, IfA, University of Edinburgh

   @newfield contributors: Contributors, Contributors (Alphabetical Order)
   @contributors: R.S. Collins, N.J.G. Cross, N.C. Hambly, E. Sutorius
"""
#------------------------------------------------------------------------------

def ensureDirExist(aDir):
    """
    If the supplied directory does not exist then create it.

    @param aDir: Full path to the directory.
    @type  aDir: str

    """
    if not os.path.exists(aDir):
        os.umask(0000)
        os.makedirs(aDir, mode=0775)

#------------------------------------------------------------------------------

def makeDateTime(time=None):
    """
    Returns an archive date/time data type, defaulting to the current time if
    no input argument is given. This defines the archive date/time data type,
    and is presently set to the mx.DateTime defined type. This function defines
    the time system for the archive (which is UTC).

    @param time: If given, specify a time in the format:
                 "2005-01-29 23:59:59.99".
    @type  time: str

    @return: A date/time in the archive defined type.
    @rtype:  mx.DateTime

    """
    if time:
        return mx.DateTime.DateTimeFrom(time)
    else:
        return mx.DateTime.utc()


#------------------------------------------------------------------------------
# Entry point for DbCopy

if __name__ == "__main__":
    # Define additional command-line options
    CLI.progArgs["comment"] = "Running DbCopy"
    CLI.progArgs.remove("database")
    CLI.progOpts += [
        CLI.Option('A', "average", "calculate max/min/avg for all columns:\n"
                   "\t\t\t\t 'x' (max)\n"
                   "\t\t\t\t 'n' (min)\n"
                   "\t\t\t\t 'g' (avg)\n"
                   "\t\t\t\t 'm' (max/min)\n"
                   "\t\t\t\t 'a' (max/min/avg)\n",
                   "STR", ''),
        CLI.Option('C', "cleanup", "clean up given directory:\n"
                   "\t\t\t\t 'h' (.xml files in hbm/)\n"
                   "\t\t\t\t 's' (.sql files in sql/)\n"
                   "\t\t\t\t 'l' (schema.jar in lib/)\n"
                   "\t\t\t\t 'j' (.java files in hbm/src/schema/)\n"
                   "\t\t\t\t 'c' (.class files in hbm/classes/schema/)\n"
                   "\t\t\t\t 'o' (.csv outgest files)\n"
                   "\t\t\t\t 'v' (files in views/ and excluded/)\n",
                   "STR", ''),
        CLI.Option('F', "fixcode", "fix code for next stage processing:\n"
                   "\t\t\t\t 'j' (java: Fix constructor in java files.)\n"
                   "\t\t\t\t 'x:dbName' (xml: Fix attribute names in xml files, needs DBname.\n"
                   "\t\t\t\t 'l' (linebreak: Fix linebreaks in data files.)\n",
                   "STR", ''),
        CLI.Option('H', "hbm2java", "convert hbm into java."),
        CLI.Option('J', "compilejava", "compile java files."),
        CLI.Option('S', "schemacreate", "create the MySql schema:\n"
                   "\t\t\t\t 'c' (create scema)\n"
                   "\t\t\t\t 's' (split schema)\n",
                   "STR", ''),
        CLI.Option('O', "outgest", "outgest from MS Sql DB."),
        CLI.Option('T', "transfer", "transfer data to lsstuk1"
                   " (needs 'database')."),
        CLI.Option('d', "database",
                   "database to connect to. combination of server "
                   "(mysql/qserv/ramses5) and db (LSSTtest/UKIDSSDR8PLUS).",
                   "STR", 'ramses16.WSA',
                   isValOK=lambda x: '.' in x),
        CLI.Option('l', "xferlog", "xferlog of files to be processed",
                   "LOGFILE", ''),
        CLI.Option('p', "pattern", "restrict outgest to tables with pattern "
                   "given in code"),
        CLI.Option('m', "mode", "outgest mode ('mysql' or 'sqlserver')",
                   "STR", "mysql",
                   isValOK=lambda x: x in ["mysql", "sqlserver"]),
        CLI.Option('y', "type", "outgest data type ('bin' or 'csv')",
                   "STR", "csv",
                   isValOK=lambda x: x in ["bin", "csv"]),
        CLI.Option('r', "redo", "redo processing.")]
        
    cli = CLI(DbCopy, "$Revision: 11114 $", checkSVN=False)

    if cli.getOpt("fixcode") and cli.getOpt("fixcode").startswith('x') \
           and len(cli.getOpt("fixcode").split(':')) != 2:
        Logger.addMessage("Fixing xml needs dbName as well.")
        sys.exit()

    Logger.addMessage(cli.getProgDetails())
    dbcopy = DbCopy(
        cli.getOpt("curator"),
        cli.getOpt("cleanup"),
        cli.getOpt("fixcode"),
        cli.getOpt("hbm2java"),
        cli.getOpt("compilejava"),
        cli.getOpt("schemacreate"),
        cli.getOpt("outgest"),
        cli.getOpt("transfer"),
        cli.getOpt("average"),
        cli.getOpt("redo"),
        cli.getOpt("mode"),
        cli.getOpt("type"),
        cli.getOpt("pattern"),
        cli.getOpt("test"),
        cli.getOpt("xferlog"),
        cli.getOpt("database"),
        cli.getArg("comment"))
    dbcopy.run()
        
