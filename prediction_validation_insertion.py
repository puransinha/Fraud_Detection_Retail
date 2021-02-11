from Prediction_Raw_Data_Validation.prediction_raw_validation import Raw_Data_Validation
from Prediction_DataType_Validation.prediction_DataType_Validation import DBOperations
from Prediction_Data_Preprocessing_BeforeDB.prediction_preprocessing_beforeDB import preprocessing_beforeDB
from Application_Logging.logger import App_Logger

class Predict_Validation:
    def __init__(self,mainFilepath,additionalFilepath):
        self.raw_data = Raw_Data_Validation(mainFilepath,additionalFilepath)
        self.preproccesing_beforeDB = preprocessing_beforeDB()
        self.DbOperation = DBOperations()
        self.file_object = open('Prediction_Logs/Prediction_Main_Log.txt','a+')
        self.log_writer = App_Logger()

    def prediction_validation(self):
        try:
            self.log_writer.log(self.file_object,'Start of Raw Data Validation on Files !!')
            main_file,additional_file,mainFile_LengthofDataStampInFile,mainFile_LengthofTimeStampInFile,additional_LengthofDateStampInFile,additional_LengthofTimeStampInFile,mainFile_ColName,additionalFile_ColName,NoCol_MainFile,NoCol_AdditionalFile = self.raw_data.fetch_values_from_schema()
            mainFile_regex = self.raw_data.mainfile_manualRegexCreation()
            additionalFile_regex = self.raw_data.additionalfile_manualRegexCreation()
            self.raw_data.validationFileNameRaw_MainFile(mainFile_regex,mainFile_LengthofDataStampInFile,mainFile_LengthofTimeStampInFile)
            self.raw_data.validationFileNameRaw_AdditionalFile(additionalFile_regex,additional_LengthofDateStampInFile,additional_LengthofTimeStampInFile)
            self.raw_data.validate_NoOfCol_MainFile(NoCol_MainFile)
            self.raw_data.validate_NoOfCol_AdditionalFile(NoCol_AdditionalFile)
            self.log_writer.log(self.file_object,'Raw Data Validation Completed !!')
            self.log_writer.log(self.file_object,'Start of Data Preprocessing before DB')
            self.preproccesing_beforeDB.replaceMissingWithNull_MainFile()
            self.preproccesing_beforeDB.replaceMissingWithNull_AdditionalFile()
            self.log_writer.log(self.file_object,'Data Preprocessing before DB Completed !!')
            self.log_writer.log(self.file_object,'Start of Creating TrainingDatabase and Table based on given schema!!!')
            self.DbOperation.createTable_MainFile('Prediction',mainFile_ColName)
            self.DbOperation.createTable_AdditionalFile('Prediction',additionalFile_ColName)
            self.log_writer.log(self.file_object,'Creation of Table in Database Successfull !!!')
            self.log_writer.log(self.file_object, "Insertion of Data into Table started!!!!")
            self.DbOperation.InsertIntoTableGoodData_MainFile('Prediction')
            self.DbOperation.InsertIntoTableGoodData_AdditionalFile('Prediction')
            self.log_writer.log(self.file_object, "Insertion of Data into Tables Completed!!!!")
            self.log_writer.log(self.file_object, "Deleting Main and Additional File Good Data Folder!!!")
            self.raw_data.deleteExistingGoodDataTrainingDir_MainFile()
            self.raw_data.deleteExistingGoodDataTrainingDir_AdditionalFile()
            self.log_writer.log(self.file_object,'Main and Additional Good File Directory Deleted !!!')
            self.log_writer.log(self.file_object,'Starting moving bad files to Archive and deleting bad data directory')
            self.raw_data.moveBadFilesToArchiveBad_MainFile()
            self.raw_data.moveBadFilesToArchiveBad_AdditionalFile()
            self.log_writer.log(self.file_object,'Bad Files moved to Archive!! and Bad Directory Deleted !!')
            self.log_writer.log(self.file_object,'Raw Data Validation Completed Successfully')
            self.log_writer.log(self.file_object,'Exporting Data Into CSV File Started')
            self.DbOperation.SelectingDataFromTableIntoCSV_MainFile('Prediction')
            self.DbOperation.SelectingDataFromTableIntoCSV_AdditionalFile('Prediction')
            self.log_writer.log(self.file_object,'Data to CSV File Exported Successfull')
            self.log_writer.log(self.file_object,'End of Raw Data Validation!!!')
            self.file_object.close()
        except Exception as e:
            raise e
