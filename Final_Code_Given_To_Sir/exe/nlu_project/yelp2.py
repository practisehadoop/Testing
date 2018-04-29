import logging,sys,pandas as pd
sys.path.append('/bdaas/exe/zomato_reviews/conf_files/')
from rev_conf import *


def yelp_func():
	try:
		logging.info('entered into yelp func')
		df3 = pd.read_json("yelp_Resto2_HOU.json")
		df2 = pd.read_csv(paths['output_dir']+'review_text_%s.txt'% retrieved_time.replace(':','-'),sep='|',encoding="ISO-8859-1")
		if(len(df2.review_text) > 0):
			df2['Class'] = df3['top_class']
			df2['retrieved_time'] = retrieved_time.replace(':','-')
			df2.to_csv(paths['output_dir']+'review_text_%s.txt'% retrieved_time.replace(':','-'),sep='|',index=False,encoding="utf-8")
		else:
			logging.info('No Data Available')
		return 1
	except Exception as e:
		logging.info('error in yelp function  %s' %str(e))