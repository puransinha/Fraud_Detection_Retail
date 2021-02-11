import pandas as pd
from Data_Ingestion import data_loader_prediction
from Data_Preprocessing import data_preprocessing
from File_Operations import file_methods
from Application_Logging.logger import App_Logger
from Prediction_Raw_Data_Validation.prediction_raw_validation import Raw_Data_Validation

class Prediction:
    def __init__(self,mainFilePath,additionalFilePath):
        self.log_writer = App_Logger()
        self.file_object = open('Prediction_Logs/PredictionLog.txt','a+')
        if mainFilePath is not None and additionalFilePath is not None:
            self.pred_data_val = Raw_Data_Validation(mainFilePath,additionalFilePath)

    def predict_from_model(self):
        self.log_writer.log(self.file_object,'Start of Prediction')
        try:
            self.pred_data_val.deletePredictionFile()
            data_getter = data_loader_prediction.Data_Getter(self.file_object,self.log_writer)
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
            main_data,unwanted_data = preprocessor.remove_unwanted_cols(main_data,return_unwanted_data=True)
            #x,y = preprocessor.separate_label_feature(main_data,'class')

            #x_train,x_test,y_train,y_test = train_test_split(x,y,test_size=0.3)

            #x_train,y_train = preprocessor.over_sampling_smote(x_train,y_train)

            #model_finder = tuner.Model_Finder(self.file_object,self.log_writer)
            #best_model_name,best_model = model_finder.get_best_model(x_train,y_train,x_test,y_test)

            file_loader = file_methods.File_Operation(self.file_object,self.log_writer)
            #save_model = file_op.save_model(best_model,best_model_name)
            model_name = file_loader.find_correct_model_file()
            model = file_loader.load_model(model_name)
            result = list(model.predict(main_data))
            data = list(zip(unwanted_data['user_id'],unwanted_data['signup_time'],
                    unwanted_data['purchase_time'],unwanted_data['device_id'],unwanted_data['source'],unwanted_data['browser'],
                    unwanted_data['sex'],unwanted_data['ip_address'],unwanted_data['Country'],result))
            result = pd.DataFrame(data,columns=['user_id','signup_time','purchase_time','device_id','source','browser','sex','ip_address','Country','Prediction'])
            path = "Prediction_Output_File/Prediction.csv"
            result.to_csv(path,header=True,mode='a+')
            self.log_writer.log(self.file_object,'Successfull End of Prediction')
            self.file_object.close()
        except Exception as e:
            self.log_writer.log(self.file_object,'Error Occured while doing the Prediction !! Error :: %s' %str(e))
            self.file_object.close()
            raise e
        return path,result.head().to_json(orient="records")


class Prediction_Row:
    def __init__(self):
        self.log_writer = App_Logger()
        self.file_object = open('Prediction_Logs/PredictionLog.txt', 'a+')
        # self.datarow = pd.DataFrame({'signup_time': self.signup_time, 'purchase_time': self.purchase_time,
        #                              'purchase_value': self.purchase_value, 'source': self.source,
        #                              'browser': self.browser, 'sex': self.sex, 'age': self.age,
        #                              'ip_address': self.ip_address})
        #print(self.datarow)

    def predictRow(self,datarow):
        self.log_writer.log(self.file_object, 'Start of DataRow Prediction')
        self.datarow = datarow
        try:
            preprocessor = data_preprocessing.PreProcessorRow(self.datarow,self.file_object,self.log_writer)
            self.datarow = preprocessor.row_map_ip_to_country(self.datarow)
            self.datarow = preprocessor.row_difference_signup_and_purchase(self.datarow)
            self.datarow = preprocessor.row_encoding_browser(self.datarow)
            self.datarow = preprocessor.row_encoding_source(self.datarow)
            self.datarow = preprocessor.row_encoding_sex(self.datarow)
            self.datarow = preprocessor.row_count_frequency_encoding_country(self.datarow)
            self.datarow = preprocessor.row_remove_unwanted_cols(self.datarow)

            file_loader = file_methods.File_Operation(self.file_object, self.log_writer)
            model_name = file_loader.find_correct_model_file()
            model = file_loader.load_model(model_name)
            self.datarow['purchase_value'] = pd.to_numeric(self.datarow['purchase_value'])
            self.datarow['age'] = pd.to_numeric(self.datarow['age'])
            self.datarow = self.datarow.reindex(
                ['purchase_value', 'age', 'd_day', 'd_hour', 'd_minutes', 'd_seconds', 'FireFox', 'IE', 'Opera',
                 'Safari', 'Direct', 'SEO', 'M', 'country_encode'], axis=1)
            result = model.predict(self.datarow)

            self.log_writer.log(self.file_object, 'Successfull End of DataRow Prediction')
            self.file_object.close()
        except Exception as e:
            self.log_writer.log(self.file_object, 'Error Occured while doing the DataRaw Prediction !! Error :: %s' % str(e))
            self.file_object.close()
            raise e
        return str(result[0])

