import shutil
import os
import sqlite3
import csv
from Application_Logging.logger import App_Logger

class DBOperations:
    def __init__(self):
        self.path = "Training_Database/"
        self.goodRaw_MainFile_path = "Training_Raw_Validated_File/Good_Raw_MainFile"
        self.badRaw_MainFile_path = "Training_Raw_Validated_File/Bad_Raw_MainFile"
        self.goodRaw_AdditionalFile_path = "Training_Raw_Validated_File/Good_Raw_AdditionalFile"
        self.badRaw_AdditionalFile_path = "Training_Raw_Validated_File/Bad_Raw_AdditionalFile"
        self.logger = App_Logger()

    def DatabaseConnection(self,database_name):
        try:
            con = sqlite3.connect(self.path + database_name + '.db')
            file = open('Training_Logs/DataBaseConnection.txt','a+')
            self.logger.log(file,'Database Connection to %s Successfully' % database_name + '.db')
            file.close()
        except ConnectionError:
            file = open('Training_Logs/DataBaseConnection.txt','a+')
            self.logger.log(file,'Error while connecting to database:: %s' % str(ConnectionError))
            file.close()
            raise ConnectionError
        return con

    def createTable_MainFile(self,database,colname_MainFile):
        try:
            con = self.DatabaseConnection(database)

            c = con.cursor()
            c.execute("SELECT count(name)  FROM sqlite_master WHERE type = 'table' AND name = 'MainFile_Good_Raw_Data'")
            if c.fetchone()[0] == 1:
                con.close()
                file = open('Training_Logs/DbTableCreateLog.txt', 'a+')
                self.logger.log(file,'MainFile_Good_Raw_Data Table Created Successfully !!')
                file.close()
            else:
                for key in colname_MainFile.keys():
                    type = colname_MainFile[key]
                    try:
                        con.execute('ALTER TABLE MainFile_Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,dataType=type))
                    except:
                        con.execute('CREATE TABLE MainFile_Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type))
                con.close()

        except Exception as e:
            file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file,'Error while creating Table:: %s' % e)
            file.close()
            con.close()
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % database)
            file.close()
            raise e

    def createTable_AdditionalFile(self,database,colname_AdditionalFile):
        try:
            con = self.DatabaseConnection(database)
            c = con.cursor()
            c.execute("SELECT count(name)  FROM sqlite_master WHERE type = 'table' AND name = 'AdditionalFile_Good_Raw_Data'")
            if c.fetchone()[0] == 1:
                con.close()
                file = open('Training_Logs/DbTableCreateLog.txt', 'a+')
                self.logger.log(file,'AdditionalFile_Good_Raw_Data Table Created Successfully !!')
                file.close()
            else:
                for key in colname_AdditionalFile.keys():
                    type = colname_AdditionalFile[key]
                    try:
                        con.execute('ALTER TABLE AdditionalFile_Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,dataType=type))
                    except:
                        con.execute('CREATE TABLE AdditionalFile_Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type))
                con.close()

        except Exception as e:
            file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file,'Error while creating Table:: %s' % e)
            file.close()
            con.close()
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % database)
            file.close()
            raise e

    def InsertIntoTableGoodData_MainFile(self,database):
        con = self.DatabaseConnection(database)
        c = con.cursor()
        MainFile_goodDataPath = self.goodRaw_MainFile_path
        only_files = [f for f in os.listdir(MainFile_goodDataPath)]
        log_file = open("Training_Logs/DbInsertLog.txt", 'a+')
        for file in only_files:
            try:
                with open(MainFile_goodDataPath + '/' + file, 'r') as f:
                    #next(f)
                    dr = csv.DictReader(f)
                    to_dict = [(int(i['user_id']),i['signup_time'],i['purchase_time'],int(i['purchase_value']),i['device_id'],i['source'],i['browser'],i['sex'],int(i['age']),float(i['ip_address']),int(i['class'])) for i in dr]
                try:
                    insert = """INSERT INTO MainFile_Good_Raw_Data VALUES (?,?,?,?,?,?,?,?,?,?,?);"""
                    c.executemany(insert,to_dict)
                    con.commit()
                    self.logger.log(log_file, "%s File Loaded Successfully in MainFile_Good_Raw_Data Table" % file)
                except Exception as e:
                    self.logger.log(log_file,'Error while Inserting into MainFile_Good_Raw_Data Table %s' % str(e))
            except Exception as e:
                    con.rollback()
                    self.logger.log(log_file,'Error while Inserting into MainFile_Good_Raw_Data %s' % str(e))
                    shutil.move(self.goodRaw_MainFile_path + '/' + file,self.badRaw_MainFile_path)
                    self.logger.log(log_file,'Main File Moved Successfully after Error in Insertion into Database')
                    log_file.close()
                    con.close()
        con.close()
        log_file.close()

    def InsertIntoTableGoodData_AdditionalFile(self,database):
        con = self.DatabaseConnection(database)
        c = con.cursor()
        AdditionalFile_goodDataPath = self.goodRaw_AdditionalFile_path
        only_files = [f for f in os.listdir(AdditionalFile_goodDataPath)]
        log_file = open("Training_Logs/DbInsertLog.txt", 'a+')
        for file in only_files:
            try:
                with open(AdditionalFile_goodDataPath + '/' + file, 'r') as f:
                    # next(f)
                    dr = csv.DictReader(f)
                    to_dict = [(float(i['lower_bound_ip_address']),float(i['upper_bound_ip_address']),i['country']) for i in dr]
                try:
                    insert = """INSERT INTO AdditionalFile_Good_Raw_Data VALUES (?,?,?);"""
                    c.executemany(insert, to_dict)
                    con.commit()
                    self.logger.log(log_file, "%s File Loaded Successfully in AdditionalFile_Good_Raw_Data Table" % file)
                except Exception as e:
                    self.logger.log(log_file, 'Error while Inserting into MainFile_Good_Raw_Data Table %s' % str(e))
            except Exception as e:
                con.rollback()
                self.logger.log(log_file, 'Error while Inserting into AdditionalFile_Good_Raw_Data %s' % str(e))
                shutil.move(self.goodRaw_AdditionalFile_path + '/' + file, self.badRaw_AdditionalFile_path)
                self.logger.log(log_file, 'Additional File Moved Successfully after Error in Insertion into Database')
                log_file.close()
                con.close()
        con.close()
        log_file.close()

    def SelectingDataFromTableIntoCSV_MainFile(self,database):
        self.TrainingfileFromDB_Dir ='TrainingFileFromDB'
        self.MainFilePath = 'MainFile'
        self.MainFile_Name = 'InputFile.csv'
        log_file = open("Training_Logs/ExportToCsv.txt", 'a+')
        try:
            con = self.DatabaseConnection(database)
            sql_select = 'SELECT * FROM MainFile_Good_Raw_Data'
            cursor = con.cursor()

            cursor.execute(sql_select)
            results = cursor.fetchall()
            header =[i[0] for i in cursor.description]

            if not os.path.isdir(self.TrainingfileFromDB_Dir + '/' + self.MainFilePath):
                os.makedirs(os.path.join(self.TrainingfileFromDB_Dir,self.MainFilePath))

            csvFile = csv.writer(open(self.TrainingfileFromDB_Dir + '/' + self.MainFilePath + '/' + self.MainFile_Name,'w',newline=''),delimiter=',',
                                 lineterminator='\r\n',quoting=csv.QUOTE_ALL,escapechar='\\')
            csvFile.writerow(header)
            csvFile.writerows(results)
            self.logger.log(log_file,'MainFile Exported as .csv Format Successfully')
            log_file.close()
        except Exception as e:
            self.logger.log(log_file,'MainFile Exporting Failed:: %s' % e)
            log_file.close()

    def SelectingDataFromTableIntoCSV_AdditionalFile(self,database):
        self.TrainingfileFromDB_Dir ='TrainingFileFromDB'
        self.AdditionalFilePath = 'AdditionalFile'
        self.AdditionalFile_Name = 'AdditionalFile.csv'
        log_file = open("Training_Logs/ExportToCsv.txt", 'a+')
        try:
            con = self.DatabaseConnection(database)
            sql_select = 'SELECT * FROM AdditionalFile_Good_Raw_Data'
            cursor = con.cursor()

            cursor.execute(sql_select)
            results = cursor.fetchall()
            header =[i[0] for i in cursor.description]

            if not os.path.isdir(self.TrainingfileFromDB_Dir + '/' + self.AdditionalFilePath):
                os.makedirs(os.path.join(self.TrainingfileFromDB_Dir,self.AdditionalFilePath))

            csvFile = csv.writer(open(self.TrainingfileFromDB_Dir + '/' + self.AdditionalFilePath + '/' + self.AdditionalFile_Name, 'w', newline=''),
                delimiter=',',lineterminator='\r\n', quoting=csv.QUOTE_ALL, escapechar='\\')
            csvFile.writerow(header)
            csvFile.writerows(results)
            self.logger.log(log_file,'AdditionalFile Exported as .csv Format Successfully')
            log_file.close()
        except Exception as e:
            self.logger.log(log_file,'AdditionalFile Exporting Failed:: %s' % e)
            log_file.close()

