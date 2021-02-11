import pandas as pd
import os
from Application_Logging.logger import App_Logger

class preprocessing_beforeDB:
    def __init__(self):
        self.goodData_MainFile_path = "Prediction_Raw_Validated_File/Good_Raw_MainFile"
        self.goodData_AdditionalFile_path = "Prediction_Raw_Validated_File/Good_Raw_AdditionalFile"
        self.logger = App_Logger()

    def replaceMissingWithNull_MainFile(self):
        try:
            f = open("Prediction_Logs/data_preprocessing_beforeDB.txt", "a+")
            only_files = [f for f in os.listdir(self.goodData_MainFile_path)]
            for file in only_files:
                csv = pd.read_csv(self.goodData_MainFile_path + "/" + file)
                csv.fillna('NULL',inplace=True)
                csv.to_csv(self.goodData_MainFile_path + "/" + file,index=None,header=True)
                self.logger.log(f,'Replace Missing values with Null Values in Good Raw Main File Successfully !!')
            f.close()
        except Exception as e:
            f = open("Prediction_Logs/data_preprocessing_beforeDB.txt", "a+")
            self.logger.log(f,'Replace missing with Null Values failed in Main File becasue:: %s' % str(e))
            f.close()

    def replaceMissingWithNull_AdditionalFile(self):
        f = open("Prediction_Logs/data_preprocessing_beforeDB.txt","a+")
        try:
            only_files = [f for f in os.listdir(self.goodData_AdditionalFile_path)]
            for file in only_files:
                csv = pd.read_csv(self.goodData_AdditionalFile_path + "/" + file)
                csv.fillna('NULL',inplace=True)
                csv.to_csv(self.goodData_AdditionalFile_path + "/" + file,index=None,header=True)
                self.logger.log(f,'Replace Missing values with Null Values in Additional Raw Main File Successfully !!')
        except Exception as e:
            self.logger.log(f,'Replace missing with Null Values failed in Additional File becasue:: %s' % e)
            f.close()
        f.close()
