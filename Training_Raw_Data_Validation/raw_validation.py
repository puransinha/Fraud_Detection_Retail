import os
import pandas as pd
import numpy as np
import shutil
import json
import re
from datetime import datetime
from Application_Logging.logger import App_Logger

class Raw_Data_Validation:
    def __init__(self,mainfile_path,additionalfile_path):
        self.batch_directory_MainFile = mainfile_path
        self.batch_directory_AdditionalFile = additionalfile_path
        self.schema_path = 'schema_Training.json'
        self.logger = App_Logger()

    def fetch_values_from_schema(self):
        try:
            with open(self.schema_path,'r') as r:
                dic = json.load(r)
                r.close()
            main_file = dic['SampleFileName_Main']
            additional_file = dic['SampleFileName_Additional']
            main_lengthofdatestampinfile = dic['Main_LengthOfDateStampInFile']
            additional_lengthofdatestampinfile = dic['Additional_LengthOfDateStampInFile']
            main_lengthoftimestampinfile = dic['Main_LengthOfTimeStampInFile']
            additional_lengthoftimestampinfile = dic['Additional_LengthOfTimeStampInFile']
            no_col_mainfile = dic['NumberOfColumns_MainFile']
            no_col_additionalfile = dic['NumberOfColumns_AdditionalFile']
            mainfile_col_name = dic['MainFile_ColName']
            additionalfile_colname = dic['AdditionalFile_ColName']
 
            file = open('Training_Logs/valuesfromschema_Validation_Log.txt','a+')
            message = "Number of Columns in Main File:: %s" % mainfile_col_name + "Number of Columns in Additional File:: %s" %additionalfile_colname + "\n" + "MainFile Length of DateStamp::%s" %main_lengthofdatestampinfile + "\n" + "MainFile Length of TimeStamp:: %s" %main_lengthoftimestampinfile
            self.logger.log(file,message)
            file.close()
        except ValueError:
            file = open('Training_Logs/valuesfromschema_Validation_Log.txt', 'a+')
            self.logger.log(file,'Value Error : Value not Found inside schema_Training.json')
            file.close()
            raise ValueError

        except KeyError:
            file = open('Training_Logs/valuesfromschema_Validation_Log.txt', 'a+')
            self.logger.log(file,'Key Error : Key Value Error Incorrect Key Passed !!')
            file.close()
            raise KeyError

        except Exception as e:
            file = open('Training_Logs/valuesfromschema_Validation_Log.txt', 'a+')
            self.logger.log(file,str(e))
            file.close()
            raise e
        return main_file,additional_file,main_lengthofdatestampinfile,main_lengthoftimestampinfile,additional_lengthofdatestampinfile,additional_lengthoftimestampinfile,mainfile_col_name,additionalfile_colname,no_col_mainfile,no_col_additionalfile

    def mainfile_manualRegexCreation(self):
        regex = "['Fraud_Data_']+['\_'']+[\d_]+[\d]+\.csv"
        return  regex

    def additionalfile_manualRegexCreation(self):
        regex = "['IPAddress_To_Country_']+['\_'']+[\d_]+[\d]+\.csv"
        return regex


    def createDirectoryFor_GoodBadRawData_MainFile(self):
        try:
            path = os.path.join("Training_Raw_Validated_File/", "Good_Raw_MainFile/")
            if not os.path.isdir(path):
                os.makedirs(path)

            path = os.path.join("Training_Raw_Validated_File/", "Bad_Raw_MainFile/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,'Error while creating MainFile Good and Bad Directory %s' % ex)
            file.close()
            raise OSError

    def createDirectoryFor_GoodBadRawData_AdditionalFile(self):
        try:
            path = os.path.join("Training_Raw_Validated_File/","Good_Raw_AdditionalFile")
            if not os.path.isdir(path):
                os.makedirs(path)

            path = os.path.join("Training_Raw_Validated_File/", "Bad_Raw_AdditionalFile")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,'Error while creating Additional Good and Bad Directory %s' % ex)
            file.close()
            raise OSError

    def deleteExistingGoodDataTrainingDir_MainFile(self):
        try:
            path = "Training_Raw_Validated_File/"
            if os.path.isdir(path + 'Good_Raw_MainFile/'):
                shutil.rmtree(path + 'Good_Raw_MainFile/')
                file = open('Training_Logs/General_Log.txt','a+')
                self.logger.log(file,'Good Raw Main File Directory deleted Sucessfully !!!')
                file.close()

        except OSError as ex:
            file = open('Training_Logs/General_Log.txt','a+')
            self.logger.log(file,'Error while deleting Main File Good Raw Directory: %s' % ex)
            file.close()
            raise OSError

    def deleteExistingGoodDataTrainingDir_AdditionalFile(self):
        try:
            path = "Training_Raw_Validated_File/"
            if os.path.isdir(path + 'Good_Raw_AdditionalFile/'):
                shutil.rmtree(path + 'Good_Raw_AdditionalFile/')
                file = open('Training_Logs/General_Log.txt','a+')
                self.logger.log(file,'Good Raw Main File Directory deleted Sucessfully !!!')
                file.close()


        except OSError as ex:
            file = open('Training_Logs/General_Log.txt','a+')
            self.logger.log(file,'Error while deleting Good Raw Directory: %s' % ex)
            file.close()
            raise OSError

    def deleteExistingBadDataTrainingDir_MainFile(self):
        try:
            path = "Training_Raw_Validated_File/"
            if os.path.isdir(path + 'Bad_Raw_MainFile/'):
                shutil.rmtree(path + 'Bad_Raw_MainFile/')
                file = open('Training_Logs/General_Log.txt','a+')
                self.logger.log(file,'Bad Raw Additional Directory deleted Sucessfully !!!')
                file.close()

        except OSError as ex:
            file = open('Training_Logs/General_Log.txt','a+')
            self.logger.log(file,'Error while deleting Main File Bad Raw Directory: %s' % ex)
            file.close()
            raise OSError

    def deleteExistingBadDataTrainingDir_AdditionalFile(self):
        try:
            path = "Training_Raw_Validated_File/"
            if os.path.isdir(path + 'Bad_Raw_AdditionalFile/'):
                shutil.rmtree(path + 'Bad_Raw_AdditionalFile/')
                file = open('Training_Logs/General_Log.txt','a+')
                self.logger.log(file,'Bad Raw Additional Directory deleted Sucessfully !!!')
                file.close()
        except OSError as ex:
            file = open('Training_Logs/General_Log.txt','a+')
            self.logger.log(file,'Error while deleting Additional Bad Raw Directory: %s' % ex)
            file.close()
            raise OSError

    def moveBadFilesToArchiveBad_MainFile(self):
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            source = 'Training_Raw_Validated_File/Bad_Raw_MainFile/'
            if os.path.isdir(source):
                path = 'TrainingArchiveBadData_MainFile'
                if not os.path.isdir(path):
                    os.makedirs(path)
                destination = 'TrainingArchiveBadData_MainFile/Bad_Data_' + str(date)+"_"+str(time)

                if not os.path.isdir(destination):
                    os.makedirs(destination)
                files = os.listdir(source)

                for f in files:
                    if f not in os.listdir(destination):
                        shutil.move(source + f,destination)
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, 'Bad Main files moved to archive')
            path = "Training_Raw_Validated_File"
            if os.path.isdir(path + 'Bad_Raw_MainFile/'):
                shutil.rmtree(path + 'Bad_Raw_MainFile/')
            self.logger.log(file,'Bad Raw Main Files Data Directory Removed Successfully!!')
            file.close()

        except Exception as e:
            file = open("Training_Logs/General_Log.txt", 'a+')
            self.logger.log(file,'Error while moving bad main files to Archive::%s' % e)
            file.close()
            raise e

    def moveBadFilesToArchiveBad_AdditionalFile(self):
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            source = 'Training_Raw_Validated_File/Bad_Raw_AdditionalFile/'
            if os.path.isdir(source):
                path = 'TrainingArchiveBadData_AdditionalFile'
                if not os.path.isdir(path):
                    os.makedirs(path)
                destination = 'TrainingArchiveBadData_AdditionalFile/Bad_Data_' + str(date)+"_"+str(time)

                if not os.path.isdir(destination):
                    os.makedirs(destination)
                files = os.listdir(source)

                for f in files:
                    if f not in os.listdir(destination):
                        shutil.move(source + f,destination)
            file = open("Training_Logs/General_Log.txt", 'a+')
            self.logger.log(file, 'Bad Additional files moved to archive')
            path = "Training_Raw_Validated_File"
            if os.path.isdir(path + 'Bad_Raw_AdditionalFile/'):
                shutil.rmtree(path + 'Bad_Raw_AdditionalFile/')
            self.logger.log(file,'Bad Raw Additional Files Data Directory Removed Successfully!!')
            file.close()

        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,'Error while moving bad main files to Archive::%s' % e)
            file.close()
            raise e

    def validationFileNameRaw_MainFile(self,mainfile_Regex,main_lengthofdatestampinfile,main_lengthoftimestampinfile):
        self.deleteExistingBadDataTrainingDir_MainFile()
        self.deleteExistingGoodDataTrainingDir_MainFile()
        self.createDirectoryFor_GoodBadRawData_MainFile()

        onlyfiles = [f for f in os.listdir(self.batch_directory_MainFile)]

        try:
            file = open("Training_Logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                if (re.match(mainfile_Regex,filename)):
                    split = re.split('.csv',filename)
                    split = re.split('_',split[0])
                    if len(split[2]) == main_lengthofdatestampinfile:
                        if len(split[3]) == main_lengthoftimestampinfile:
                            shutil.copy("Training_Batch_Files/Main_File/" + filename,"Training_Raw_Validated_File/Good_Raw_MainFile")
                            self.logger.log(file,'Valid File Name !! File moved to GoodRaw_Main Directory ::%s' % filename)
                        else:
                            shutil.copy("Training_Batch_Files/Main_File/" + filename,"Training_Raw_Validated_File/Bad_Raw_MainFile")
                            self.logger.log(file,'Invalid File Name!! File moved to Bad Raw Main File Directory')
                    else:
                        shutil.copy("Training_Batch_Files/Main_File/" + filename,"Training_Raw_Validated_File/Bad_Raw_MainFile")
                        self.logger.log(file, 'Invalid File Name!! File moved to Bad Raw Main File Directory')
                else:
                    shutil.copy("Training_Batch_Files/Main_File/" + filename,"Training_Raw_Validated_File/Bad_Raw_MainFile")
                    self.logger.log(file, 'Invalid File Name!! File moved to Bad Raw Main File Directory')
            file.close()
        except Exception as e:
            file = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(file, "Error occured while validating Main FileName %s" % e)
            file.close()
            raise e

    def validationFileNameRaw_AdditionalFile(self,additionalfile_Regex,additionalfile_lengthofdatestampinfile,additionalfile_lengthoftimestampinfile):
        self.deleteExistingBadDataTrainingDir_AdditionalFile()
        self.deleteExistingGoodDataTrainingDir_AdditionalFile()
        self.createDirectoryFor_GoodBadRawData_AdditionalFile()

        onlyfiles = [f for f in os.listdir(self.batch_directory_AdditionalFile)]

        try:
            file = open("Training_Logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                if (re.match(additionalfile_Regex,filename)):
                    split = re.split('.csv',filename)
                    split = re.split('_',split[0])
                    if len(split[3]) == additionalfile_lengthofdatestampinfile:
                        if len(split[4]) == additionalfile_lengthoftimestampinfile:
                            shutil.copy("Training_Batch_Files/Additional_File/" + filename,"Training_Raw_Validated_File/Good_Raw_AdditionalFile")
                            self.logger.log(file,'Valid File Name !! File moved to GoodRaw_Additional Directory ::%s' % filename)
                        else:
                            shutil.copy("Training_Batch_Files/Additional_File/" + filename,"Training_Raw_Validated_File/Bad_Raw_AdditionalFile")
                            self.logger.log(file,'Invalid File Name!! File moved to Bad Raw Additional File Directory')
                    else:
                        shutil.copy("Training_Batch_Files/Additional_File/" + filename,"Training_Raw_Validated_File/Bad_Raw_AdditionalFile")
                        self.logger.log(file, 'Invalid File Name!! File moved to Bad Raw Additional File Directory')
                else:
                    shutil.copy("Training_Batch_Files/Additional_File/" + filename,"Training_Raw_Validated_File/Bad_Raw_AdditionalFile")
                    self.logger.log(file, 'Invalid File Name!! File moved to Bad Raw Additional File Directory')
            file.close()
        except Exception as e:
            file = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(file, "Error occured while validating Additional FileName %s" % e)
            file.close()
            raise e

    def validate_NoOfCol_MainFile(self,noofcol_mainfile):
        try:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            for file in os.listdir('Training_Raw_Validated_File/Good_Raw_MainFile/'):
                csv = pd.read_csv('Training_Raw_Validated_File/Good_Raw_MainFile/' + file)
                if csv.shape[1] == noofcol_mainfile:
                    pass
                else:
                    shutil.move('Training_Raw_Validated_File/Good_Raw_MainFile' + file,'Training_Raw_Validated_File/Bad_Raw_MainFile')
                    self.logger.log(f,'Invalid Column length for the file !! File moved to bad raw main Directory :: %s' % file)
                self.logger.log(f,'Main File Columns Length Validated Sucessfully')
            f.close()
        except OSError:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f,'Error Occured while moving file :: %s' %str(OSError))
            f.close()
            raise OSError

        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e

    def validate_NoOfCol_AdditionalFile(self,noofcol_additionalfile):
        try:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            for file in os.listdir('Training_Raw_Validated_File/Good_Raw_AdditionalFile/'):
                csv = pd.read_csv('Training_Raw_Validated_File/Good_Raw_AdditionalFile/' + file)
                if csv.shape[1] == noofcol_additionalfile:
                    pass
                else:
                    shutil.move('Training_Raw_Validated_File/Good_Raw_AdditionalFile' + file,'Training_Raw_Validated_File/Bad_Raw_AdditionalFile')
                    self.logger.log(f,'Invalid Column length for the file !! File moved to bad raw additional Directory :: %s' % file)
                self.logger.log(f,'Additional File Columns Length Validated Sucessfully')
            f.close()
        except OSError:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f,'Error Occured while moving file :: %s' %str(OSError))
            f.close()
            raise OSError

        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e




