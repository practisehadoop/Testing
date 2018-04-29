import json,sys
import pandas as pd
import numpy as np
sys.path.append('/bdaas/exe/zomato_reviews/conf_files/') 
# from os.path import join, dirname
from watson_developer_cloud import NaturalLanguageClassifierV1
from rev_conf import *

def nlc():
	try:
		logging.info('entered into nlc function')
		natural_language_classifier = NaturalLanguageClassifierV1(
		    username='76f01b31-8768-4816-a7e9-2c3626c237ab',
		    password='voRT5NVjHAZI')
		classes=[]
		classifiers = natural_language_classifier.list()
		#print(json.dumps(classifiers, indent=2))
		status = natural_language_classifier.status('359f41x201-nlc-65743')
		#print(json.dumps(status, indent=2))

		df = pd.read_csv(paths['output_dir']+'review_text_%s.txt'% retrieved_time.replace(':','-'),sep='|')
		if (status['status'] == 'Available'and len(df.review_text) > 0):
			for i in range(0,len(df.review_text),1):
				line = df.review_text[i]
				classes.append(natural_language_classifier.classify('359f41x201-nlc-65743',line.decode("ISO-8859-1")))
			with open('yelp_{}_{}.json'.format('Resto2','HOU'),'w') as f:
				json.dump(classes, f, indent=5)
		else:
			logging.info('No Data Available')
		return 1
	except Exception as e:
		logging.info('error in nlc function  %s' %str(e))
