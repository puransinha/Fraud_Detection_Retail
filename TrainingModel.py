from sklearn.model_selection import train_test_split
from Data_Ingestion import data_loader
from Data_Preprocessing import data_preprocessing
from best_model_finder import tuner
from File_Operations import file_methods
from Application_Logging.logger import App_Logger

class TrainModel:
    def __init__(self):
        self.log_writer = App_Logger()
        self.file_object = open('Training_Logs/ModelTrainingLog.txt','a+')

    def train_model(self):
        self.log_writer.log(self.file_object,'Start of Training')
        try:
            data_getter = data_loader.Data_Getter(self.file_object,self.log_writer)
            main_data,additional_data = data_getter.get_data()

            preprocessor = data_preprocessing.PreProcessor(self.file_object,self.log_writer)
            is_null_present = preprocessor.is_null_present(main_data)
            if is_null_present == True:
                main_data = preprocessor.impute_missing_values(main_data)
            main_data = preprocessor.map_ip_to_country(main_data,additional_data)
            main_data = preprocessor.difference_signup_and_purchase(main_data)
            main_data = preprocessor.encoding_browser(main_data)
            main_data = preprocessor.encoding_source(main_data)
            main_data = preprocessor.encoding_sex(main_data)
            main_data = preprocessor.count_frequency_encoding_country(main_data)
            main_data = preprocessor.remove_unwanted_cols(main_data)
            x,y = preprocessor.separate_label_feature(main_data,'class')

            x_train,x_test,y_train,y_test = train_test_split(x,y,test_size=0.3)

            #x_train,y_train = preprocessor.over_sampling_smote(x_train,y_train)

            model_finder = tuner.Model_Finder(self.file_object,self.log_writer)
            best_model_name,best_model = model_finder.get_best_model(x_train,y_train,x_test,y_test)

            file_op = file_methods.File_Operation(self.file_object,self.log_writer)
            save_model = file_op.save_model(best_model,best_model_name)

            self.log_writer.log(self.file_object,'Successfull End of Training')
            self.file_object.close()

        except Exception as e:
            self.log_writer.log(self.file_object,'Unsuccessfull End of Training')
            self.file_object.close()
            raise e

