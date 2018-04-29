import sys,logging,pandas as pd
import simplejson
import pandas as pd
import numpy as np 
import spark_conf
# from os.path import join, dirname
from watson_developer_cloud import NaturalLanguageClassifierV1
from sklearn.preprocessing import LabelBinarizer
#import seaborn as sns
#from matplotlib import pyplot as plt
from sklearn.metrics import mean_squared_error
from sklearn import cross_validation, linear_model



class classifier:
    def __init__(self,sc):
        logging.info('entered into classifier class')


    def nlc(self):
        try:
    	    logging.info('entered into nlc function')
            natural_language_classifier = NaturalLanguageClassifierV1(username=spark_conf.classifier_input['username'],password=spark_conf.classifier_input['password'])
            classes=[]
            classifiers = natural_language_classifier.list()
            print(simplejson.dumps(classifiers, indent=2))

            status = natural_language_classifier.status('359f41x201-nlc-65743')
            print(simplejson.dumps(status, indent=2))

            df = pd.read_csv(spark_conf.classifier_input['input_file']+'reviews_%s.txt'% spark_conf.retrieved_time.replace(':','-'),sep='|')
            #df = input.toPandas()
            df2=df.copy(deep=True)

            if (status['status'] == 'Available'):
                        for i in range(0,len(df2.review_text),1):
                                line = df2.review_text[i]
                                classes.append(natural_language_classifier.classify('359f41x201-nlc-65743',line.decode("ISO-8859-1")))
                        with open(spark_conf.file_path['data_update_path']+'yelp_{}_{}.json'.format('Resto3','HOU'),'w') as f:
                            simplejson.dump(classes, f, indent=5)
                        return 1
            else:
                        logging.info("NO DATA AVAILABLE")
                        return 0
        except Exception as e:
            logging.info('error in nlc function  %s' %str(e))



    def confident_classifier(self):
        try:
            logging.info('Adding Confidence and class to the file')
            df3 = pd.read_json(spark_conf.file_path['data_update_path']+'yelp_{}_{}.json'.format('Resto3','HOU'))
            df4=df3.copy(deep='True')

            df5 = pd.read_csv(spark_conf.classifier_input['input_file']+'reviews_%s.txt'% spark_conf.retrieved_time.replace(':','-'),sep='|',encoding="ISO-8859-1")
            #df5 = cc_input.toPandas() 
            df6=df5.copy(deep='True')
            if (len(df6.review_text) > 0):
                class_name_list = []
                confidence_list= []
                for rows in df4.iterrows():
                    class_name_list.append(rows[1]['classes'][0]['class_name'])
                    confidence_list.append(rows[1]['classes'][0]['confidence'])
                df6['class_name'] = class_name_list
                df6["confidence"] = confidence_list
                print df6 
                df6.to_csv(spark_conf.classifier_input['output_file']+'Classified_Output_test.txt',sep='|',index=False,encoding="ISO-8859-1")    
                return 1
               
            else:
                print(" No Classification Available")
                return 0
        except Exception as e:
            logging.info('error in confident_classifier function  %s' %str(e))





    def binary_encoding(self):
        try:
            logging.info('performing binary encoding')

            other_CSV = pd.read_csv(spark_conf.classifier_input['output_file']+'Classified_Output_test.txt', sep  = '|', encoding = 'ISO-8859-1')
            #other_CSV = be_input
            print other_CSV
            other_CSV_0 = other_CSV.copy(deep="True")

            lb_style = LabelBinarizer()
            rating_text = lb_style.fit_transform(other_CSV["rating_text"])
            rating_text_df = pd.DataFrame(rating_text, columns=lb_style.classes_)
            other_CSV_1 = other_CSV.join(rating_text_df)
            print other_CSV_1

            lb_style = LabelBinarizer()
            user_foodie_level = lb_style.fit_transform(other_CSV["user_foodie_level"])
            user_foodie_level_df = pd.DataFrame(user_foodie_level, columns=lb_style.classes_)
            other_CSV_2 = other_CSV_1.join(user_foodie_level_df)
            print other_CSV_2

            lb_style = LabelBinarizer()
            class_name = lb_style.fit_transform(other_CSV["class_name"])
            class_name_df = pd.DataFrame(class_name, columns=lb_style.classes_)
            other_CSV_3 = other_CSV_2.join(class_name_df)
            other_CSV_3.to_csv(spark_conf.classifier_input['output_file']+'Encoded_Classified_test.txt',sep = "|", index=False, encoding = 'utf-8')
            return 1
        except Exception as e:
            logging.info('error in binary_encoding function  %s' %str(e))


    def linear_regression(self):

        try:
            logging.info('Performing Regression')
            dfi = pd.read_csv(spark_conf.file_path['data_update_path']+'Encoded_Classified_1.txt', sep  = '|', encoding = 'ISO-8859-1')
            dfi_test = pd.read_csv(spark_conf.classifier_input['output_file']+'Encoded_Classified_test.txt', sep  = '|', encoding = 'ISO-8859-1')
            #dfi_test = lr_input
            input_list=list(dfi_test)
            print input_list
            corr = dfi.corr()
            #sns.heatmap(corr,xticklabels=corr.columns,yticklabels=corr.columns)
            feature_cols = ['likes','comment_count','user_level_num','Average','Avoid!','Blah!','Good Enough','Great!','Insane!','Not rated','Very Bad','Well...','Big Foodie','Connoisseur','Foodie','Super Foodie','Bad Ambience','Bad Food','Bad Service','Good Ambience','Good Food','Good Service','Not Worthy']

            feature_cols_1 = list(set(input_list).intersection(feature_cols))
            print feature_cols_1

            X_train = dfi[:-1]
            print len(X_train)
            X_test  = dfi_test[0:]
            print len(X_test)
            y_train = dfi.confidence[:-1]
            print len(y_train)
            y_test  = dfi_test.confidence[0:]
            print len(y_test)

            X = X_train[feature_cols_1]
            y = y_train
            Xtest = X_test[feature_cols_1]

            regr = linear_model.Lasso(alpha=0.0000000001, fit_intercept=True, normalize=False, precompute=False, copy_X=True, max_iter=1000, tol=0.0001, warm_start=False, positive=False, random_state=None, selection='cyclic')
            regr.fit(X, y)


            shuffle = cross_validation.KFold(len(X), n_folds=10, shuffle=True, random_state=0)
            scores = cross_validation.cross_val_score(regr, X, y ,cv=shuffle)
            print("Accuracy: %.3f%% (%.3f%%)") % (scores.mean()*100.0, scores.std()*100.0)

            print regr.intercept_
            print (regr.coef_)

            print mean_squared_error(regr.predict(Xtest), y_test)**0.5
            print regr.predict(Xtest)
            print regr.score(X,y)

            se = pd.Series(regr.predict(Xtest))
            dfi_test['score'] = se.values

            dfi_test.to_csv(spark_conf.classifier_input['output_file']+'final_Output.txt',sep='|',encoding="ISO-8859-1") 
            return 1
        except Exception as e:
            logging.info('error in Linear_Regression function  %s' %str(e))
