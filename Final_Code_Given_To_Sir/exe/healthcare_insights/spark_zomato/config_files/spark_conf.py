import logging,sys,json,os,pip,subprocess
from datetime import tzinfo, timedelta, datetime
from subprocess import call, Popen, PIPE
system_datetime = datetime.now()
retrieved_time = system_datetime.strftime("%Y-%m-%d %H:%M:%S")
utctime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
### Check Values : 1 for Zomato, 2 for Yelp, 3 for 311 data. These are just used in sql queries in order to avoid repetition
#logging.basicConfig(filename='/bdaas/log/spark_conf.log',filemode='w',level = logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

#Main Local Path For Keeping all the Files
main_local_path = '/bdaas/exe/healthcare_insights/spark_zomato/'

##Main src path in Local
local_src_path = '/bdaas/src/'


##NLU Classifier Path
nlu_project_path ='/bdaas/exe/nlu_project/'


##Main HDFS Path For Keeping all the Files
main_hdfs_path = '/user/bdaas/src/'

##Libraries Required for running this application
required_libraries = ['pytz','requests','pandas']


## Value For Chaning The Permission of Directories Reccursively
permission_value = '777'

## 2 Python Versions For Checking in Linux
default_python_version1 = 'python2.7'
python_version2 = 'python2.6'


## 1 to execute program; 0 to not to execute
program_execution= {
	'zomato': '1',
	'yelp' : '1',
	'311_data':'0',
	'merge_data' :'1'
}




## 311 Data Link Where We download  the file using HTTP
links = ['311-Public-Data-Extract-monthly-clean.txt']


# 0 for not to download; 1 to download
zip_codes = {
	'url':'http://www2.census.gov/geo/docs/maps-data/data/gazetteer/2016_Gazetteer/2016_Gaz_zcta_national.zip',
	'value': '1',
	'name' : 'zomato'
}


zomato = {'apikey':'2359afb687c4d53d9c9cca273f36fc55',
	'search_api': 'https://developers.zomato.com/api/v2.1/search?',
	'zomato_reviews':'https://developers.zomato.com/api/v2.1/reviews?',
	'api_check':'https://developers.zomato.com/api/v2.1/categories?',
	'sort_by':'rating',
	'order_by':'asc',
	'radius': '16093',
	'category_by':'8,9,10',
	'zip_code' : '75038',
	'lat_value' : '',
	'long_value': '',
	'days' : '30'

}

values_311data = {
'days' : '60',
'file_download': '0'
}


classifier_input ={
	'input_file':local_src_path+'spark_dependencies/review_data/',
	'output_file':local_src_path+'spark_dependencies/classifier_data/',
	'username':'76f01b31-8768-4816-a7e9-2c3626c237ab',
	'password':'voRT5NVjHAZI'

}


file_path = {
	
	'conf_path' : main_local_path+'conf_files/',
	'otherfiles_path' : main_local_path+'other_files/',
	'zip_file':local_src_path+'spark_dependencies/zip_path/',
	'file_output': local_src_path+'spark_dependencies/zip_path/output/',
	'311_localdata':local_src_path + 'spark_dependencies/311_localdata/', 
	'log_file_path':'/bdaas/log/'
	#'searchdata_path':local_src_path+'spark_dependencies/search_data/',
	#'reviewdata_path': local_src_path+'spark_dependencies/review_data/',
	#'input_file':local_src_path+'spark_dependencies/review_data/',
	#'output_file':local_src_path+'spark_dependencies/classifier_data/'
}

name = {
	'zip_name' : 'zipcodes.zip',
	'zipfile_name' : 'zipcode.txt'

}

hdfs_path ={
	'zipfile': main_hdfs_path+'zipcode/',
	'validation_file':main_hdfs_path+'validation_data/',
	'zip_log':main_hdfs_path+'zip_log/',
	'zip_check':main_hdfs_path+'zip_check/',
	'zip_table': main_hdfs_path+'zip_date/',
	'temp_review_data': main_hdfs_path+'zomato/review_data/temporary/',
	'review_data':main_hdfs_path+'zomato/review_data/final/',
	'restaurant_data':main_hdfs_path+'zomato/restaurant_data/',
	'merged_restaurant_data':main_hdfs_path+'zomato/merged_restaurant_data/',
	'merged_review_data': main_hdfs_path+'zomato/merged_review_data/',
	'classifier_output':main_hdfs_path+'classifier_output/',
	'merged_classifier_data':main_hdfs_path+'merged_classifier_output/',
	'yelp_restaurnt_data' : main_hdfs_path+'yelp/restaurant_data/',
	'yelp_temp_review_data' : main_hdfs_path+'yelp/review_data/temporary/',
	'yelp_classifier_output':main_hdfs_path+'classifier_output/',
	'yelp_final_review_data': main_hdfs_path+'yelp/review_data/final/',
	'yelp_restaurnt_mergedata' : main_hdfs_path+'yelp/merged_restaurant_data/',
	'yelp_review_mergedata': main_hdfs_path+'yelp/merged_review_data/',
	'311_textfile':main_hdfs_path+'311_data/downloaded_textfile/',
	'311_querydata':main_hdfs_path+'311_data/query_data/',
	'merged311_querydata':main_hdfs_path+'311_data/query_mergedata/'

}

### Merge HDFS DATA LINKS
merge_paths =  [hdfs_path['merged_restaurant_data'] +"|"+hdfs_path['restaurant_data']+"|zomato_restaurant_mergedata.txt",
		  hdfs_path['merged_review_data']  +"|"+hdfs_path['review_data']+"|zomato_review_mergedata.txt",
		  hdfs_path['yelp_restaurnt_mergedata']  +"|"+hdfs_path['yelp_restaurnt_data']+"|yelp_restaurant_mergedata.txt",
		  hdfs_path['merged_classifier_data']  +"|"+hdfs_path['classifier_output']+"|classifier_mergedata.txt",
		  hdfs_path['yelp_review_mergedata']  +"|"+hdfs_path['yelp_final_review_data']+"|yelp_review_mergedata.txt",
		  hdfs_path['merged311_querydata']  +"|"+hdfs_path['311_querydata']+"|311_mergedata.txt"
		  ]




## Latitude and Longitude Values used in below yelp are only for API TOKEN Checking 
yelp = {
'API_HOST' : 'https://api.yelp.com',
'SEARCH_PATH' : '/v3/businesses/search',
'BUSINESS_PATH' : '/v3/businesses/', 
'REVIEW_PATH' : '/v3/businesses/id/reviews',
'TOKEN_PATH' : '/oauth2/token',
'GRANT_TYPE' : 'client_credentials',
'zip_code':['75052','75038'],
'term' : 'restaurants',
'sort_by' : 'rating',
'radius':'16093',
'access_token':'POYu9mAcanK4oiM6vbKgT97zPzUDzh6LHCF1Fbu2B6OK-NogXudtNW-Rt7ZqPNt4J68_OfeAxsjeIu31s-DKiqysX9VNqRP_bu8RJ54w0Oc3HOj9CsPX2KOFgunfWnYx',
'client_id' : 'oDl39gRV0sfK9erCHeY4JQ',
'client_secret' : 'QpMBqlkYPjTeLbgTakheLwmC0Zph7kohiCJ3GWYdnnTUf2GUXSjns3SorlPD9MNi',
'SEARCH_LIMIT' : '50',
'days' : '30',
'timezone_initial' : 'US/Pacific',
'timezone_final' : 'UTC',
'latitude':'32.898235',
'longitude':'-96.955223'
}





def utc_time():
	logging.info('entered into utc time func')
	utc_datetime = datetime.utcnow()
	utctime = utc_datetime.strftime("%Y-%m-%d %H:%M:%S")
	utc_file = utc_datetime.strftime("%Y-%m-%d%H:%M:%S.%fZ").replace(':','-')
	return utctime,utc_file

#path_check()
#dir_permission_change('777')
#77054 10 mil
#pip_installs()
#print file_path['conf_path']

	# def pip_installs(self,required_libraries,version='pip2.7'):
	# 	logging.info('entered into pip install function')
	# 	try:
	# 		from subprocess import check_output
	# 		pip27_path = subprocess.check_output(['sudo','find','/','-name',version])
	# 		lib_installs = [subprocess.call((['sudo',pip27_path.replace('\n',''),'install', i])) for i in required_libraries]
	# 	except:
	# 		self.backups(required_libraries)




###CODE FOR PIP INSTALLS
#PIP getpip.py download and installsudo python other_files/get-pip.py 
#PYPZ sudo pip install pytz
#requests sudo pip install requests
#Pandas sudo pip install pandas


		# if spark_conf.program_execution['yelp'] == '1':
		# 	logging.info('entered into yelp input values check')			
		# 	if spark_conf.yelp['latitude'] != '' and spark_conf.yelp['longitude'] != '':
		# 		logging.info('yelp input values are present')
		# 	else:
		# 		logging.info('yelp input values are not present')
		# 		sys.exit()
