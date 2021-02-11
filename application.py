from wsgiref import simple_server
from flask import Flask,render_template,request,Response,send_file
import os
import shutil
from flask_cors import cross_origin,CORS
from TrainingModel import TrainModel
from predict_from_model import Prediction
from predict_from_model import Prediction_Row
from training_validation_insertion import Train_Validation
from prediction_validation_insertion import Predict_Validation
import flask_monitoringdashboard as dashboard
import json
import pandas as pd

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

application = Flask(__name__)
dashboard.bind(application)
CORS(application)

@application.route("/", methods=['GET'])
@cross_origin()
def home():
    country_list = pd.read_csv('assets/countrylist.csv')
    country_list = pd.Series(country_list['0'])
    country_list = country_list.tolist()
    return render_template('index.html',country_list=country_list)

@application.route("/train", methods=['POST'])
@cross_origin()
def trainRouteClient():
    try:
        if request.json['mainFilePath'] and request.json['additionalFilePath'] is not None:
            MainFile_path = request.json['mainFilePath']
            AdditionalFile_path = request.json['additionalFilePath']

            trainValidation_object = Train_Validation(MainFile_path,AdditionalFile_path)
            trainValidation_object.training_validation()

            trainModel_object = TrainModel()
            trainModel_object.train_model()
    except ValueError:
        return Response('Error Occured! %s' %str(ValueError))
    except KeyError:
        return Response('Error Occured! %s' %str(KeyError))
    except Exception as e:
        return Response('Error Occured! %s' %str(e))
    return Response('Training Successfull !!!')


@application.route("/predictBatch",methods=['POST'])
@cross_origin()
def predictBatchRoute():
    try:
        if request.method == 'POST':
            #batchpath = request.form['batchpath']
            print(request.files)
            cwd = os.getcwd()
            try:
                if 'file' in request.files:
                    batch_file = request.files['file']
                    #path = os.path.join(os.getcwd(),)
                    if os.path.exists('Prediction_Batch_Files/Main_File'):
                        file = os.listdir('Prediction_Batch_Files/Main_File')
                        if not len(file) == 0:
                            os.remove('Prediction_Batch_Files/Main_File/' + file[0])
                    else:
                        pass

                    if os.path.exists('Prediction_Database'):
                        file = os.listdir('Prediction_Database')
                        if not len(file) == 0:
                            os.remove('Prediction_Database/' + file[0])
                    else:
                        pass

                    if os.path.exists('Prediction_Logs'):
                        file = os.listdir('Prediction_Logs')
                        if not len(file) == 0:
                            for f in file:
                                os.remove('Prediction_Logs/' + f)
                    else:
                        pass

                    if os.path.exists('Prediction_Output_File'):
                        file = os.listdir('Prediction_Output_File')
                        if not len(file) == 0:
                            os.remove('Prediction_Output_File/' + file[0])
                    else:
                        pass

                    if os.path.exists('Prediction_Raw_Validated_File/Bad_Raw_MainFile'):
                        file = os.listdir('Prediction_Raw_Validated_File/Bad_Raw_MainFile')
                        if not len(file) == 0:
                            os.remove('Prediction_Raw_Validated_File/Bad_Raw_MainFile/' + file[0])
                    else:
                        pass

                    if os.path.exists('Prediction_Raw_Validated_File/Bad_Raw_AdditionalFile'):
                        file = os.listdir('Prediction_Raw_Validated_File/Bad_Raw_AdditionalFile')
                        if not len(file) == 0:
                            os.remove('Prediction_Raw_Validated_File/Bad_Raw_AdditionalFile/' + file[0])
                    else:
                        pass

                    if os.path.exists('PredictionArchiveBadData_MainFile'):
                        shutil.rmtree('PredictionArchiveBadData_MainFile')
                    else:
                        pass

                    if os.path.exists('PredictionArchiveBadData_AdditionalFile'):
                        shutil.rmtree('PredictionArchiveBadData_AdditionalFile')
                    else:
                        pass

                    if os.path.exists('PredictionFileFromDB/MainFile'):
                        file = os.listdir('PredictionFileFromDB/MainFile')
                        if not len(file) == 0:
                            os.remove('PredictionFileFromDB/MainFile/' + file[0])
                    else:
                        pass

                    if os.path.exists('PredictionFileFromDB/AdditionalFile'):
                        file = os.listdir('PredictionFileFromDB/AdditionalFile')
                        if not len(file) == 0:
                            os.remove('PredictionFileFromDB/AdditionalFile/' + file[0])
                    else:
                        pass

                    batch_file.save('Prediction_Batch_Files/Main_File/' + batch_file.filename)
                    #print('Uploaded Successfully !!')
            except Exception as e:
                print(e)

            except OSError as o:
                print(str(o))

            MainFilePath = 'Prediction_Batch_Files' + '/' + 'Main_File' + '/' + batch_file.filename
            AdditionalFilePath = 'Prediction_Batch_Files' + '/' + 'Additional_File' + '/' + 'IPAddress_To_Country_09122020_120156.csv'

            prediction_val = Predict_Validation('Prediction_Batch_Files/Main_File', 'Prediction_Batch_Files/Additional_File')
            prediction_val.prediction_validation()

            pred = Prediction(MainFilePath, AdditionalFilePath)
            path, json_predictions = pred.predict_from_model()
            return Response("Prediction File Created at !! " + str(path) + "and Few of the Predictions are" + str(
                json.loads(json_predictions)))
            return Response('Done')
        else:
            print('None Request Matched')
    except ValueError:
        return Response('Error Occured! %s' % str(ValueError))
    except KeyError:
        return Response('Error Occured! %s' % str(KeyError))
    except Exception as e:
        return Response('Error Occured! %s' % str(e))

@application.route("/download",methods=['GET','POST'])
@cross_origin()
def download_prediction():
    try:
        file = os.listdir('Prediction_Output_File')
        file = str(file[0])
        return send_file('Prediction_Output_File/' + file,as_attachment=True)
    except Exception as e:
        print(str(e))


@application.route("/predictJSON",methods=['POST'])
@cross_origin()
def predictRouteClient():
    try:
        if request.json is not None:
            if request.json['mainFilePath'] and request.json['additionalFilePath'] is not None:
                MainFilePath = request.json['mainFilePath']
                AdditionalFilePath = request.json['additionalFilePath']

                prediction_val = Predict_Validation(MainFilePath,AdditionalFilePath)
                prediction_val.prediction_validation()

                pred = Prediction(MainFilePath,AdditionalFilePath)
                path,json_predictions = pred.predict_from_model()
                return Response("Prediction File Created at !! " + str(path) + "and Few of the Predictions are" + str(json.loads(json_predictions)))
        else:
            print('Nothing Request Matched')
    except ValueError :
        return Response('Error Occured! %s' %str(ValueError))
    except KeyError :
        return Response('Error Occured! %s' %str(KeyError))
    except Exception as e:
        return Response('Error Occured! %s' %str(e))



@application.route("/predictRow",methods=['POST'])
@cross_origin()
def predictRowRoute():
    try:
        if request.form is not None:
            if request.form['signuptime'] is not None and request.form['purchasetime'] is not None and request.form['purchasevalue'] is not None and request.form['source'] is not None and request.form['browser'] is not None and request.form['sex'] is not None and request.form['age'] is not None and request.form['ipaddr'] is not None:
                signup_time = request.form['signuptime']
                purchase_time = request.form['purchasetime']
                purchase_value = request.form['purchasevalue']
                source = request.form['source']
                browser = request.form['browser']
                sex = request.form['sex']
                age = request.form['age']
                ipaddr = request.form['ipaddr']
                c = request.form['country']

                datarow = pd.DataFrame(
                    [[signup_time, purchase_time, purchase_value, source, browser, sex, age, ipaddr]],
                    columns=['signup_time', 'purchase_time', 'purchase_value', 'source', 'browser', 'sex', 'age',
                             'ip_address'])

                predict = Prediction_Row()
                result = predict.predictRow(datarow)
                if int(result) == 1:
                    result_str = 'Fraudulent Transaction'
                elif int(result) == 0:
                    result_str = 'Non-Fraudulent Transaction'
                else:
                    result_str = 'Not Able to predict'
                return Response('Prediction is: ' + str(result_str))
        else:
            print('Nothing Passed')
    except ValueError:
        return Response('Error Occured! %s' %str(ValueError))

    except KeyError :
        return Response('Error Occured! %s' %str(KeyError))

    except Exception as e:
        return Response('Error Occured! %s' %str(e))



if __name__ =="__main__":
    host = '0.0.0.0'
    port = 5000
    #application.run()
    httpd = simple_server.make_server(host,port,application)
    httpd.serve_forever()
