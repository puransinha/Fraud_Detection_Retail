from collections import Counter
from imblearn.over_sampling import SMOTE
import pandas as pd
import numpy as np
import math
from rangetree import RangeTree
from sklearn.impute import KNNImputer
from datetime import datetime

class PreProcessor:
    def __init__(self, fileobj, loggerobj):
        self.file_object = fileobj
        self.logger_object = loggerobj

    def is_null_present(self, data):
        self.logger_object.log(self.file_object,'Entered the IsNull Present method of Data Proprocessing ')
        self.null_present = False

        try:
            self.null_counts = data.isna().sum()
            for i in self.null_counts:
                if i > 0:
                    self.null_present = True
                    break
                if (self.null_present):
                    df_with_null = pd.DataFrame()
                    df_with_null['columns'] = data.columns
                    df_with_null['missingValuesCount'] =np.asarray(data.isna().sum())
                    df_with_null.to_csv('preprocessing_data/null_values.csv')
                    self.logger_object.log(self.file_object,'Missing Values Found')
            return self.null_present
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception Occured while performing is_null_present method %s' % e)
            self.logger_object.log(self.file_object,'Finding Missing Values Failed due to Exception occured')
            raise e


    def impute_missing_values(self,data):
        self.logger_object.log(self.file_object, 'Entered the Impute_Missing_Values  method of Data Proprocessing ')
        self.data = data
        try:
            imputer = KNNImputer(n_neighbors=3,weights='uniform',missing_values=np.nan)
            self.new_array = imputer.fit_transform(self.data)
            self.new_data = pd.DataFrame(data=self.new_data,columns=self.data.columns)
            self.logger_object.log(self.file_object,'Imputing missing values Successful.')
            return self.new_data
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in impute_missing_values method Exception message:  %s' + str(e))
            self.logger_object.log(self.file_object,'Imputing missing values failed.')
            raise e

    def map_ip_to_country(self,main_data,additional_data):
        self.logger_object.log(self.file_object,'Entered to Map Ip Address to Corresponding Country')
        main_data = main_data
        add_data = additional_data
        try:
            main_data['IP_Address'] = main_data['ip_address'].apply(lambda x:math.floor(x))
            add_data['Lower_IP'] = add_data['lower_bound_ip_address'].apply(lambda x:math.floor(x))
            add_data['Upper_IP'] = add_data['upper_bound_ip_address'].apply(lambda x:math.floor(x))
            rt = RangeTree()
            for lower,upper,country in zip(add_data['Lower_IP'],add_data['Upper_IP'],add_data['country']):
                rt[lower:upper] = country
            countries = []
            current_tym = datetime.now()
            for ip in main_data['IP_Address']:
                try:
                    countries.append(rt[ip])
                except:
                    countries.append('No Country Found')
            execution_tym = datetime.now() - current_tym
            main_data['Country'] = countries
            self.logger_object.log(self.file_object,'Mapping of IP Address to Corresponding Country Successfull in %s' % execution_tym)
            return main_data
        except Exception as e:
            self.logger_object.log(self.file_object,'Error occured while mapping IP Address to Corresponding Country :: %s' % str(e))
            raise e

    def difference_signup_and_purchase(self,data):
        self.logger_object.log(self.file_object,'Entered to Finding Difference time between SignUp Time and Purchase Time')
        self.data = data

        try:
            self.data['signup_time'] = pd.to_datetime(self.data['signup_time'])
            self.data['purchase_time'] = pd.to_datetime(self.data['purchase_time'])

            signup_df = pd.DataFrame()
            purchase_df = pd.DataFrame()
            tym_difference_df = pd.DataFrame()

            signup_df['s_day'] = self.data['signup_time'].dt.day
            signup_df['s_month'] = self.data['signup_time'].dt.month
            signup_df['s_year'] = self.data['signup_time'].dt.year
            signup_df['s_hour'] = self.data['signup_time'].dt.hour
            signup_df['s_minute'] = self.data['signup_time'].dt.minute
            signup_df['s_seconds'] = self.data['signup_time'].dt.second

            purchase_df['p_day'] = self.data['purchase_time'].dt.day
            purchase_df['p_month'] = self.data['purchase_time'].dt.month
            purchase_df['p_year'] = self.data['purchase_time'].dt.year
            purchase_df['p_hour'] = self.data['purchase_time'].dt.hour
            purchase_df['p_minute'] = self.data['purchase_time'].dt.minute
            purchase_df['p_seconds'] = self.data['purchase_time'].dt.second

            tym_difference_df['d_day'] = purchase_df['p_day'] - signup_df['s_day']
            tym_difference_df['d_month'] = purchase_df['p_month'] - signup_df['s_month']
            tym_difference_df['d_year'] = purchase_df['p_year'] - signup_df['s_year']
            tym_difference_df['d_hour'] = purchase_df['p_hour'] - signup_df['s_hour']
            tym_difference_df['d_minutes'] = purchase_df['p_minute'] - signup_df['s_minute']
            tym_difference_df['d_seconds'] = purchase_df['p_seconds'] - signup_df['s_seconds']

            tym_difference_df['d_day'] = tym_difference_df['d_day'].apply(lambda x: abs(x))
            tym_difference_df['d_month'] = tym_difference_df['d_month'].apply(lambda x: abs(x))
            tym_difference_df['d_year'] = tym_difference_df['d_year'].apply(lambda x: abs(x))
            tym_difference_df['d_minutes'] = tym_difference_df['d_minutes'].apply(lambda x: abs(x))
            tym_difference_df['d_seconds'] = tym_difference_df['d_seconds'].apply(lambda x: abs(x))
            tym_difference_df['d_hour'] = tym_difference_df['d_hour'].apply(lambda x: abs(x))

            self.data = pd.concat([self.data, tym_difference_df], axis=1)

            self.logger_object.log(self.file_object,
                                   'Finding Difference between Signup and Purchase Time Completed Successfully !!')
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,'Error while finding differnece between Purchase and Signup Time %s' % str(e))
            raise e

    def encoding_browser(self,data):
        self.logger_object.log(self.file_object,'Entered to perform One-Hot Encoding on Browser Feature')
        self.data = data

        try:
            browser_df = pd.get_dummies(self.data['browser'],drop_first=True)
            self.data = pd.concat([self.data,browser_df],axis=1)
            self.logger_object.log(self.file_object,'One-Hot Encoding of Browser Feature Successfully Completed')
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,'Error while performing One-Hot Encoding over Browser feature:: %s' %str(e))
            raise e

    def encoding_source(self,data):
        self.logger_object.log(self.file_object,'Entered to perform One-Hot Encoding on Source Feature')
        self.data = data

        try:
            source_df = pd.get_dummies(self.data['source'],drop_first=True)
            self.data = pd.concat([self.data,source_df],axis=1)
            self.logger_object.log(self.file_object,'One-Hot Encoding of Source Feature Successfully Completed')
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,'Error while performing One-Hot Encoding over Source feature:: %s' % str(e))
            raise e

    def encoding_sex(self,data):
        self.logger_object.log(self.file_object,'Entered to perform One-Hot Encoding on Sex Feature')
        self.data = data

        try:
            sex_df = pd.get_dummies(self.data['sex'],drop_first=True)
            self.data = pd.concat([self.data,sex_df],axis=1)
            self.logger_object.log(self.file_object,'One-Hot Encoding of Sex Feature Successfully Completed')
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,'Error while performing One-Hot Encoding over Sex feature:: %s' % str(e))
            raise e


    def count_frequency_encoding_country(self,data):
        self.logger_object.log(self.file_object,'Count Frequency Encoding of Country Feature Started')
        self.data  = data

        try:
            country_map = self.data['Country'].value_counts().to_dict()
            self.data['country_encode'] = self.data['Country'].map(country_map)
            self.logger_object.log(self.file_object,'Count Frequency Encoding of Country Feature Successfully Completed')
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,'Error while performing Count Frequency Encoding over Country feature:: %s' % str(e))
            raise e

    def remove_unwanted_cols(self,data,return_unwanted_data=False):
        self.logger_object.log(self.file_object,'Removing Unwanted Columns Started !!')
        self.df = data

        try:
            self.data = self.df.drop(['user_id','signup_time','purchase_time','device_id','source',
                                         'browser','sex','ip_address','IP_Address','Country','d_month','d_year'],axis=1)

            if return_unwanted_data == True:
                self.unwanted_data = self.df[['user_id','signup_time','purchase_time','device_id','source',
                                         'browser','sex','ip_address','Country']]

            self.logger_object.log(self.file_object,'Unwanted Columns Deleted Successfully !!')
            #self.logger_object.log(self.file_object,'Sample Feature with 1 Row')
            #self.logger_object.log(self.file_object,str(self.data.head(1)))

            if return_unwanted_data == True:
                return self.data,self.unwanted_data
            else:
                return self.data

        except Exception as e:
            self.logger_object.log(self.file_object,'Error occured while removing unwanted columns :: %s' %str(e))
            raise e

    def separate_label_feature(self,data,label_col_name):
        self.logger_object.log(self.file_object,'Entered to Separate Label Feature Method')
        self.data = data
        try:
            self.x = self.data.drop(label_col_name,axis=1)
            self.y = self.data[label_col_name]
            self.logger_object.log(self.file_object,'Label Separation Done Successfully')
            return self.x,self.y
        except Exception as e:
            self.logger_object.log(self.file_object,'Error Occured while separating labels column :: %s' % str(e))
            raise e


    def over_sampling_smote(self,x,y):
        self.logger_object.log(self.file_object,'Entered to SMOTE Oversampling Method')
        self.x = x
        self.y = y
        self.smote = SMOTE()
        try:
            x_smote,y_smote = self.smote.fit_sample(x,y)
            self.logger_object.log(self.file_object,'Before SMOTE Oversampling:: %s' %str(Counter(y)))
            self.logger_object.log(self.file_object,'After SMOTE Oversampling:: %s' %str(Counter(y_smote)))
            return x_smote,y_smote
        except Exception as e:
            self.logger_object.log(self.file_object,'Error while performing SMOTE Oversampling %s' %str(e))
            raise e

class PreProcessorRow:
    def __init__(self, datarow, fileobj, loggerobj):
        self.file_object = fileobj
        self.logger_object = loggerobj
        self.datarow = datarow

    def row_map_ip_to_country(self,datarow):
        self.datarow = datarow
        self.logger_object.log(self.file_object,'Entered to DataRow Map Ip Address to Corresponding Country')
        try:
            self.datarow['Country'] = 'No Country Found'
            self.logger_object.log(self.file_object,'Mapping of IP Address to Corresponding Country Successfull')
            return self.datarow
        except Exception as e:
            self.logger_object.log(self.file_object,'Error occured while DataRow mapping IP Address to Corresponding Country :: %s' % str(e))
            raise e

    def row_difference_signup_and_purchase(self,datarow):
        self.datarow = datarow
        self.logger_object.log(self.file_object,'Entered DataRow to Finding Difference time between SignUp Time and Purchase Time')

        try:
            self.datarow['signup_time'] = pd.to_datetime(self.datarow['signup_time'])
            self.datarow['purchase_time'] = pd.to_datetime(self.datarow['purchase_time'])
            self.datarow['signup_time'] = pd.to_datetime(self.datarow['signup_time'].dt.strftime('%Y-%m-%d %H:%M:%S'))
            self.datarow['purchase_time'] = pd.to_datetime(self.datarow['purchase_time'].dt.strftime('%Y-%m-%d %H:%M:%S'))

            signup_df = pd.DataFrame()
            purchase_df = pd.DataFrame()
            tym_difference_df = pd.DataFrame()

            signup_df['s_day'] = self.datarow['signup_time'].dt.day
            signup_df['s_month'] = self.datarow['signup_time'].dt.month
            signup_df['s_year'] = self.datarow['signup_time'].dt.year
            signup_df['s_hour'] = self.datarow['signup_time'].dt.hour
            signup_df['s_minute'] = self.datarow['signup_time'].dt.minute
            signup_df['s_seconds'] = self.datarow['signup_time'].dt.second

            purchase_df['p_day'] = self.datarow['purchase_time'].dt.day
            purchase_df['p_month'] = self.datarow['purchase_time'].dt.month
            purchase_df['p_year'] = self.datarow['purchase_time'].dt.year
            purchase_df['p_hour'] = self.datarow['purchase_time'].dt.hour
            purchase_df['p_minute'] = self.datarow['purchase_time'].dt.minute
            purchase_df['p_seconds'] = self.datarow['purchase_time'].dt.second

            tym_difference_df['d_day'] = purchase_df['p_day'] - signup_df['s_day']
            tym_difference_df['d_month'] = purchase_df['p_month'] - signup_df['s_month']
            tym_difference_df['d_year'] = purchase_df['p_year'] - signup_df['s_year']
            tym_difference_df['d_hour'] = purchase_df['p_hour'] - signup_df['s_hour']
            tym_difference_df['d_minutes'] = purchase_df['p_minute'] - signup_df['s_minute']
            tym_difference_df['d_seconds'] = purchase_df['p_seconds'] - signup_df['s_seconds']

            tym_difference_df['d_day'] = tym_difference_df['d_day'].apply(lambda x: abs(x))
            tym_difference_df['d_month'] = tym_difference_df['d_month'].apply(lambda x: abs(x))
            tym_difference_df['d_year'] = tym_difference_df['d_year'].apply(lambda x: abs(x))
            tym_difference_df['d_minutes'] = tym_difference_df['d_minutes'].apply(lambda x: abs(x))
            tym_difference_df['d_seconds'] = tym_difference_df['d_seconds'].apply(lambda x: abs(x))
            tym_difference_df['d_hour'] = tym_difference_df['d_hour'].apply(lambda x: abs(x))

            self.datarow = pd.concat([self.datarow, tym_difference_df], axis=1)

            self.logger_object.log(self.file_object,
                                   'Finding Difference between Signup and Purchase Time Completed Successfully !!')
            return self.datarow
        except Exception as e:
            self.logger_object.log(self.file_object,'Error while finding DataRow differnece between Purchase and Signup Time %s' % str(e))
            raise e

    def row_encoding_browser(self,datarow):
        self.datarow = datarow
        self.logger_object.log(self.file_object,'Entered to perform DataRow One-Hot Encoding on Browser Feature')

        try:
            #browser_df = pd.get_dummies(self.datarow['browser'],drop_first=True)
            #self.datarow = pd.concat([self.datarow,browser_df],axis=1)
            source = self.datarow['source']
            if str(source) == 'SEO':
                self.datarow['SEO'] = 1
                self.datarow['Direct'] = 0
            elif str(source) == 'Direct':
                self.datarow['SEO'] = 0
                self.datarow['Direct'] = 1
            else:
                self.datarow['SEO'] = 0
                self.datarow['Direct'] = 0

            self.logger_object.log(self.file_object,'One-Hot Encoding of Browser Feature Successfully Completed')
            return self.datarow
        except Exception as e:
            self.logger_object.log(self.file_object,'Error while performing DataRow One-Hot Encoding over Browser feature:: %s' %str(e))
            raise e

    def row_encoding_source(self,datarow):
        self.datarow = datarow
        self.logger_object.log(self.file_object,'Entered to Data Row perform One-Hot Encoding on Source Feature')

        try:
            #source_df = pd.get_dummies(self.datarow['source'],drop_first=True)
            #self.datarow = pd.concat([self.datarow,source_df],axis=1)
            browser = self.datarow['browser']
            if str(browser) == 'Opera':
                self.datarow['Opera'] = 1
                self.datarow['Safari'] = 0
                self.datarow['IE'] = 0
                self.datarow['FireFox'] = 0
            elif str(browser) == 'Safari':
                self.datarow['Opera'] = 0
                self.datarow['Safari'] = 1
                self.datarow['IE'] = 0
                self.datarow['FireFox'] = 0
            elif str(browser) == 'IE':
                self.datarow['Opera'] = 0
                self.datarow['Safari'] = 0
                self.datarow['IE'] = 1
                self.datarow['FireFox'] = 0
            elif str(browser) == 'FireFox':
                self.datarow['Opera'] = 0
                self.datarow['Safari'] = 0
                self.datarow['IE'] = 0
                self.datarow['FireFox'] = 1
            else:
                self.datarow['Opera'] = 0
                self.datarow['Safari'] = 0
                self.datarow['IE'] = 0
                self.datarow['FireFox'] = 0

            self.logger_object.log(self.file_object,'One-Hot Encoding of Source Feature Successfully Completed')
            return self.datarow
        except Exception as e:
            self.logger_object.log(self.file_object,'Error while performing DataRow One-Hot Encoding over Source feature:: %s' % str(e))
            raise e

    def row_encoding_sex(self,datarow):
        self.datarow = datarow
        self.logger_object.log(self.file_object,'Entered to perform DataRow One-Hot Encoding on Sex Feature')

        try:
            #sex_df = pd.get_dummies(self.datarow['sex'],drop_first=True)
            #self.datarow = pd.concat([self.datarow,sex_df],axis=1)
            sex = self.datarow['sex']
            if str(sex) == 'M':
                self.datarow['M'] = 1
            else :
                self.datarow['M'] = 0
            self.logger_object.log(self.file_object,'One-Hot Encoding of Sex Feature Successfully Completed')
            return self.datarow
        except Exception as e:
            self.logger_object.log(self.file_object,'Error while performing DataRow One-Hot Encoding over Sex feature:: %s' % str(e))
            raise e


    def row_count_frequency_encoding_country(self,datarow):
        self.datarow = datarow
        self.logger_object.log(self.file_object,'Count Frequency Encoding of Country Feature Started')

        try:
            country_map = self.datarow['Country'].value_counts().to_dict()
            self.datarow['country_encode'] = self.datarow['Country'].map(country_map)
            self.logger_object.log(self.file_object,'Count Frequency Encoding of Country Feature Successfully Completed')
            return self.datarow
        except Exception as e:
            self.logger_object.log(self.file_object,'Error while performing Data Row Count Frequency Encoding over Country feature:: %s' % str(e))
            raise e

    def row_remove_unwanted_cols(self,datarow,return_unwanted_data=False):
        self.logger_object.log(self.file_object,'Removing DataRow Unwanted Columns Started !!')
        self.datarow = datarow
        try:
            self.data = self.datarow.drop(['signup_time','purchase_time','source',
                                         'browser','sex','ip_address','Country','d_month','d_year'],axis=1)

            if return_unwanted_data == True:
                self.unwanted_data = self.datarow[['user_id','signup_time','purchase_time','device_id','source',
                                         'browser','sex','ip_address','Country']]

            self.logger_object.log(self.file_object,'Unwanted Columns Deleted Successfully !!')
            #self.logger_object.log(self.file_object,'Sample Feature with 1 Row')
            #self.logger_object.log(self.file_object,str(self.data.head(1)))
            print(self.datarow)

            if return_unwanted_data == True:
                return self.data,self.unwanted_data
            else:
                return self.data

        except Exception as e:
            self.logger_object.log(self.file_object,'Error occured while DataRow removing unwanted columns :: %s' %str(e))
            raise e
