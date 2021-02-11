import pandas as pd

class Data_Getter:
    def __init__(self,file_object,logger_object):
        self.training_mainFile = 'TrainingFileFromDB/MainFile/InputFile.csv'
        self.training_additionalFile = 'TrainingFileFromDB/AdditionalFile/AdditionalFile.csv'
        self.file_object = file_object
        self.logger_object = logger_object

    def get_data(self):
        self.logger_object.log(self.file_object,'Entered the get_data Method of Data_Getter Class')
        try:
            self.mainFile = pd.read_csv(self.training_mainFile)
            #print(type(self.mainFile) + ' In get_data() ')
            self.additionalFile = pd.read_csv(self.training_additionalFile)
            #print(type(self.additionalFile) + ' In get_data() ')
            self.logger_object.log(self.file_object,'Data Load Successfully')
            return self.mainFile,self.additionalFile
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_data method of the Data_Getter class. Exception message: ' + str(e))
            self.logger_object.log(self.file_object,'Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            raise e
