import os
import pickle
import shutil

class File_Operation:
    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.model_directory ='models/'

    def save_model(self, model,filename):
        self.logger_object.log(self.file_object,'Entered the save_model method of File Operation Class')
        try:
            path = os.path.join(self.model_directory,filename)
            if os.path.isdir(path):
                shutil.rmtree(self.model_directory)
                os.makedirs(path)
            else:
                os.makedirs(path)

            with open(path + '/' + filename + '.sav','wb') as f:
                pickle.dump(model,f)

            self.logger_object.log(self.file_object,'Model File' + filename + ' Saved.Exited the save_model method of file_operation class')
            return 'success'
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception Occured in save_model method of file_operation class %s' % str(e))
            self.logger_object.log(self.file_object,'Model File could not be saved')
            raise e

    def load_model(self,filename):
        self.logger_object.log(self.file_object,'Entered the load_model method of File Operation Class')
        try:
            with open(self.model_directory + filename + '/' + filename + '.sav','rb') as f:
                self.logger_object.log(self.file_object,'Model File' + filename + 'Loaded Successfully')
                return pickle.load(f)
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in load model method of model finder class:: %s' %str(e))
            self.logger_object.log(self.file_object,'Model File ' + filename + ' could not be loaded ')
            raise e


    def find_correct_model_file(self):
        self.logger_object.log(self.file_object,'Entered the find_correct_model_file method of the File_Operation class')
        try:
            #self.folder_name = self.model_directory
            model_dir = os.listdir(self.model_directory)
            model_name = os.listdir(self.model_directory + model_dir[0])
            model_name = model_name[0].split('.')[0]
            self.logger_object.log(self.file_object,'Exited the find_correct_model_file method of FileMethods Package')
            return model_name
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception Occured in finding_correct_model_file method :: %s' % str(e))
            self.logger_object.log(self.file_object,'Exited the find_correct_model_file method of FileMethods Package')
            raise e