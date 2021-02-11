import numpy as np
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score,recall_score,precision_score,f1_score,roc_curve

class Model_Finder:
    def __init__(self,fileobject,loggerobject):
        self.fileobject = fileobject
        self.loggerobject = loggerobject
        self.rf = RandomForestClassifier()
        self.xg = XGBClassifier(objective='binary:logistic')

    def get_best_params_for_RandomForest(self,train_x,train_y):
        self.loggerobject.log(self.fileobject,'Entered the get_best_params_for_RandomForest Method')
        try:
            self.param_grid = {
                "n_estimators" : [int(x) for x in np.linspace(start=80,stop=140,num=4)],
                #"criterion" : ['gini','entropy'],
                #"max_features" : ['auto','sqrt','log2',None],
                #"max_depth" : [int(x) for x in np.linspace(start=10,stop=16,num=3)],
                #"min_samples_split" : [int(x) for x in np.linspace(start=1,stop=5,num=3)],
                "min_samples_leaf" : [int(x) for x in np.linspace(start=1,stop=5,num=3)]
            }

            self.grid = GridSearchCV(estimator=self.rf,param_grid=self.param_grid,cv=2,n_jobs=-1,verbose=3)
            self.grid.fit(train_x,train_y)

            #self.criterion = self.grid.best_params_['criterion']
            self.n_estimators = self.grid.best_params_['n_estimators']
            #self.max_features = self.grid.best_params_['max_features']
            #self.min_samples_split = self.grid.best_params_['min_samples_split']
            #self.max_depth = self.grid.best_params_['max_depth']
            self.min_samples_leaf = self.grid.best_params_['min_samples_leaf']

            self.rf = RandomForestClassifier(n_estimators=self.n_estimators,min_samples_leaf=self.min_samples_leaf)
            self.rf.fit(train_x,train_y)
            self.loggerobject.log(self.fileobject,'RandomForest Best params:' + str(self.grid.best_params_) + 'Exited the Best Params of Random Forest Class')
            return self.rf
        except Exception as e:
            self.loggerobject.log(self.fileobject,'Exception Occured in get_best_params_for_randomForest ::%s' % str(e))
            self.loggerobject.log(self.fileobject,'RandomForest Parameter Tuning Failed.Exited !!')
            raise e


    def get_best_params_for_XGBoost(self,train_x,train_y):
        self.loggerobject.log(self.fileobject,'Entered the get_best_params_for_xgboost method')
        try:
            self.param_grid_xgboost = {
                "n_estimators" : [int(x) for x in np.linspace(start=60,stop=120,num=4)],
                #"booster" : ['gblinear','gbtree','dart'],
                #"eta" : [0.1,0.5,0.8],
                #"gamma" : [0,1,2],
                #"max_depth" : [int(x) for x in np.linspace(start=12,stop=16,num=3)],
                #"min_child_weight" : [int(x) for x in np.linspace(start=0,stop=9,num=3)],
                "max_delta_step" : [int(x) for x in np.linspace(start=0,stop=9,num=3)]
            }

            self.grid_xg = GridSearchCV(estimator=self.xg,param_grid=self.param_grid_xgboost,cv=2,n_jobs=-1,verbose=100)
            self.grid_xg.fit(train_x,train_y)

            self.n_estimators_xg = self.grid_xg.best_params_['n_estimators']
            #self.booster_xg = self.grid_xg.best_params_['booster']
            #self.eta_xg = self.grid_xg.best_params_['eta']
            #self.gamma = self.grid_xg.best_params_['gamma']
            #self.max_depth_xg = self.grid_xg.best_params_['max_depth']
            #self.min_child_weight_xg = self.grid_xg.best_params_['min_child_weight']
            self.max_delta_step_xg = self.grid_xg.best_params_['max_delta_step']

            self.xg = XGBClassifier(objective='binary:logistic',n_estimators=self.n_estimators_xg,
                                    max_delta_step=self.max_delta_step_xg)

            self.xg.fit(train_x,train_y)
            self.loggerobject.log(self.fileobject,'XGBoost best params:' + str(self.grid_xg.best_params_) + 'Exited the best_params_for_XGBoost')
            return self.xg
        except Exception as e:
            self.loggerobject.log(self.fileobject,'Exception occured in get_best_params_xgboost :: %s' % (e))
            self.loggerobject.log(self.fileobject,'XGBoost parameter Tuning Failed,Exited !!')
            raise e

    def calculate_geometric_mean(self,fpr,tpr,threshold):
        self.gmeans = np.sqrt(tpr * (1 - fpr))
        self.ix = np.argmax(self.gmeans)
        return threshold[self.ix]

    def get_best_model(self,train_x,train_y,test_x,test_y):
        self.loggerobject.log(self.fileobject,'Entered the get_best_model of best_model_finder class')
        try:
            self.random_forest = self.get_best_params_for_RandomForest(train_x,train_y)
            self.prediction_rf = self.random_forest.predict(test_x)
            self.prediction_probab_rf = self.random_forest.predict_proba(test_x)
            self.prediction_probab_rf = self.prediction_probab_rf[:,1]
            self.rf_fpr,self.rf_tpr,self.rf_threshold = roc_curve(test_y,self.prediction_probab_rf)
            self.rf_best_threshold = self.calculate_geometric_mean(self.rf_fpr,self.rf_tpr,self.rf_threshold)
            self.rf_best_threshold = self.rf_best_threshold + 0.05
            self.loggerobject.log(self.fileobject,'Best Decision Threshold Value for Random Forest is:: %s' %str(self.rf_best_threshold))

            self.prediction_probab_rf = self.prediction_probab_rf >= self.rf_best_threshold.astype('int')
            if (len(test_y.unique()) == 1):
                self.rf_recall = recall_score(test_y,self.prediction_probab_rf)
                self.rf_precision = precision_score(test_y,self.prediction_probab_rf)
                self.rf_f1_score = f1_score(test_y,self.prediction_probab_rf)
                self.loggerobject.log(self.fileobject,'Recall of Random Forest::' + str(self.rf_recall) +
                    '\t' + 'Precision of Random Forest::' + str(self.rf_precision) +
                    '\t' + 'F1 Score of Random Forest::' + str(self.rf_f1_score))
            else:
                self.rf_roc_auc_score = roc_auc_score(test_y,self.prediction_probab_rf)
                self.rf_recall = recall_score(test_y, self.prediction_probab_rf)
                self.rf_precision = precision_score(test_y, self.prediction_probab_rf)
                self.rf_f1_score = f1_score(test_y,self.prediction_probab_rf)
                self.loggerobject.log(self.fileobject,'ROC AUC Score of RandomForest ::' + str(self.rf_roc_auc_score)
                                      + '\t' + 'Recall of Random Forest::' + str(self.rf_recall) + '\t' +
                                      'Precision of Random Forest::' + str(self.rf_precision) + '\t' +
                                      'F1-Score of Random Forest ::' + str(self.rf_f1_score))

            self.xgboost = self.get_best_params_for_XGBoost(train_x,train_y)
            self.prediction_xg = self.xgboost.predict(test_x)
            self.prediction_probab_xg = self.xg.predict_proba(test_x)
            self.prediction_probab_xg = self.prediction_probab_xg[:, 1]
            self.xg_fpr, self.xg_tpr, self.xg_threshold = roc_curve(test_y, self.prediction_probab_xg)
            self.xg_best_threshold = self.calculate_geometric_mean(self.xg_fpr, self.xg_tpr, self.xg_threshold)
            self.xg_best_threshold = self.xg_best_threshold
            self.loggerobject.log(self.fileobject, 'Best Decision Threshold Value for XGBoost is:: %s' % str(
                self.xg_best_threshold))

            self.prediction_probab_xg = self.prediction_probab_xg >= self.xg_best_threshold
            if (len(test_y.unique()) == 1):
                self.xg_recall = recall_score(test_y, self.prediction_probab_xg)
                self.xg_precision = precision_score(test_y, self.prediction_probab_xg)
                self.xg_f1_score = f1_score(test_y,self.prediction_probab_xg)
                self.loggerobject.log(self.fileobject, 'Recall of XGBoost::' + str(
                    self.xg_recall) + '\t' + 'Precision of XGBoost::' + str(self.xg_precision) + '\t' +
                                      'F1-Score of XGBoost::' + str(self.xg_f1_score))
            else:
                self.xg_roc_auc_score = roc_auc_score(test_y, self.prediction_probab_xg)
                self.xg_recall = recall_score(test_y, self.prediction_probab_xg)
                self.xg_precision = precision_score(test_y, self.prediction_probab_xg)
                self.xg_f1_score = f1_score(test_y, self.prediction_probab_xg)
                self.loggerobject.log(self.fileobject, 'ROC AUC Score of XGBoost ::' + str(self.xg_roc_auc_score) +
                                      '\t' + 'Recall of XGBoost::' + str(self.xg_recall) +
                                      '\t' + 'Precision of XGBoost::' + str(self.xg_precision) +
                                      '\t' + 'F1-Score of XGBoost::' + str(self.xg_f1_score))

            if self.rf_f1_score < self.xg_f1_score:
                #file = open('Training_Logs/Best_Threshold_Value_For_RandomForest.txt','w')
                #self.loggerobject.log(file,str(self.rf_best_threshold))
                return 'XGBoost',self.xgboost
            else:
                #file = open('Training_Logs/Best Threshold_Value_For_XGBoost.txt','w')
                #self.loggerobject.log(file,str(self.xg_best_threshold))
                return 'RandomForest',self.random_forest

        except Exception as e:
            self.loggerobject.log(self.fileobject,'Exception Occured in get_best_model method of best_model_finder class')
            self.loggerobject.log(self.fileobject,'Model Selection Failed.Exited the best model finder method of Model Finder Class')
            raise e



