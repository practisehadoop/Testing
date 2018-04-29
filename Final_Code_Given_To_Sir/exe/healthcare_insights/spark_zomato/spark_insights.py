



class insights:

	def __init__(self,sc):
		logging.info('in init function')

	def create_table(self):
		try:
			logging.info('entered create table main func')
			call(['hive','-f',spark_conf.file_path['otherfiles_path']+'create_table.hql'])
			return 1
		except Exception as e:
			logging.info('error in create_table func: %s' %str(e))

	def python_versioncheck(self,py_version):
		#try:
			logging.info('entered into python_versioncheck func')
			p = subprocess.Popen(['which',py_version], stdout=subprocess.PIPE);ver,err = p.communicate()
			if py_version[6:] in ver: self.pip_installs(spark_conf.required_libraries,pip_version='pip'+py_version[6:])
			if ver == '':print 'entered empty ver';self.python_versioncheck(spark_conf.python_version2)
		# except Exception as e:
		# 	logging.info('error in python_versioncheck func: %s' %str(e))


	def pip_installs(self,required_libraries,pip_version):
		logging.info('entered into backups install function')
		#try:
		pip_path = str()
		p = subprocess.Popen(['sudo','find','/','-name',pip_version], stdout=subprocess.PIPE);pip_path_old, err = p.communicate()
		if pip_path_old !='' and len(pip_path_old) >20:
			 for i in pip_path_old.split('\n'):
			   		if len(i) <= 22: 
			   			pip_path = pip_path + i;lib_installs = [subprocess.call((['sudo',pip_path.replace('\n',''),'install', i])) for i in required_libraries]
		# except Exception as e:
		# 	template = "An exception of type {0} occurred. Arguments:\n{1!r}"
		# 	message = template.format(type(e).__name__, e.args)
		# 	print (message)

	def dir_permission_change(self,value,main_local_path):
		logging.info('entered into dir dir_permission_change function')
		#try:
		call(['sudo','chmod','-R',value,main_local_path])
		#except Exception as e:
			#logging.info('error in dir_permission_change %s' %str(e))



	def path_check(self,file_path,hdfs_path):
		#try:
			logging.info('entered into local paths check')
			for key,value in file_path.iteritems():
				if os.path.exists(value):
					logging.info(str(value)+' path present')
				else:
					print value
					call(['sudo','mkdir','-p',value])
					logging.info(str(value)+' path created')

			for key,value in hdfs_path.iteritems():
				logging.info('entered into hdfs path check')
				try:
					a = call(['hdfs','dfs','-test','-d',value])
					if a == 1:call(['hdfs','dfs','-mkdir','-p',value])
				except:
					a = call(['hadoop','fs','-test','-d',value])
					if a == 1:call(['hadoop','fs','-mkdir','-p',value])
			return 1
	#except Exception as e:
		#logging.info('error in path check %s' %str(e))



	def api_checking(self,y):
		#try:
			if spark_conf.program_execution['zomato'] == '1':
				logging.info('entered into zomato token checking')
				print spark_conf.zomato['api_check'] + 'apikey='+str(spark_conf.zomato['apikey'])
				check =  json.loads(requests.get(spark_conf.zomato['api_check'] + 'apikey='+str(spark_conf.zomato['apikey'])).text)
				if 'message' in check:
					logging.info('message in api key '+str(check['message']))
					if 'code' in check:
						logging.info('code in api key '+str(check['code']))
						sys.exit()
			if spark_conf.program_execution['yelp'] == '1':
				logging.info('entered into yelp token checking')
				response = y.search(spark_conf.yelp['access_token'],spark_conf.yelp['term'],spark_conf.yelp['latitude'],spark_conf.yelp['longitude'],spark_conf.yelp['sort_by'],spark_conf.yelp['radius'])
				if 'error' in response:
					logging.info('error in token key '+ str(response['error']['code']))
					logging.info('error in token key '+ str(response['error']['description']))
					sys.exit()
		# except Exception as e:
		# 	logging.info('error in api_checking func: %s' %str(e))

	def inputvalues_check(self):
		#try:
			if spark_conf.program_execution['zomato'] == '1':
				logging.info('entered into zomato/yelp input values check')
				if spark_conf.zomato['zip_code'] != '' or (spark_conf.zomato['lat_value'] != ''  and spark_conf.zomato['long_value'] != ''):
					logging.info('zomato/yelp input values are present')
				else:
					logging.info('zomato/yelp input values are not present')
					sys.exit()
		# except Exception as e:
		# 	logging.info('error in inputvalues_check func: %s' %str(e))


	def merge_hdfs_data(self,paths,main_local_path):

		        
		for i in paths:
			checking = '' + 'hdfs',  'dfs', '-test',  '-f',  i.split('|')[0]+'*.txt' + ''
			checking_rest = '' + 'hdfs',  'dfs', '-test',  '-f', i.split('|')[1]+'*/p*'+ ''
			remove_data= ''+'hdfs', 'dfs', '-rm', '-r', i.split('|')[1]+'*' + ''
			full_path = ''+'hadoop','fs','-getmerge',i.split('|')[1]+'*/p*',main_local_path+i.split('|')[2]+''
			put_path= ''+'hdfs', 'dfs', '-put','-f' , main_local_path+i.split('|')[2], i.split('|')[0] +''
			merge = '' +'hdfs', 'dfs', '-getmerge', i.split('|')[0]+'*.txt', i.split('|')[1]+'*/p*',main_local_path+i.split('|')[2] + ''
			print checking,checking_rest,remove_data,full_path,put_path,merge		


			if 0 == call(checking): 
				print '++++++++++++++1'
				if 0 == call(checking_rest) and 0 == call(merge) and 0 == call(put_path): os.remove(main_local_path+i.split('|')[2]);call(remove_data)
				
			else:
				print '++++++++++++2'
				if 0 == call(checking_rest) and 0 == call(full_path) and 0 == call(put_path):os.remove(main_local_path+i.split('|')[2]);call(remove_data)




		



if __name__ == '__main__':
	#try:

		## Defining Spark Values
		spark_values = {'master':'yarn-client','app_name':'restaurant_insights','local_pyfiles_path':'/bdaas/exe/healthcare_insights/spark_zomato/'}
		##Required Imports
		import os,logging,sys,time,json,pytz,subprocess
		from subprocess import call, Popen, PIPE
		##Logging
		logging.basicConfig(filename='/bdaas/log/spark_insights.log',filemode='w',level = logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
		from datetime import datetime, time, timedelta
		##ALL Spark Imports
		from pyspark import SparkContext, SparkConf
		conf = SparkConf().setAppName(spark_values['app_name']).setMaster(spark_values['master'])
		sc = SparkContext(conf = conf,pyFiles=[spark_values['local_pyfiles_path']+'other_files/houston311_data.py',spark_values['local_pyfiles_path']+'other_files/spark_yelp.py','/bdaas/exe/nlu_project/spark_classifier_modified.py',spark_values['local_pyfiles_path']+'other_files/spark_zipcode.py',spark_values['local_pyfiles_path']+ 'other_files/spark_zomato.py',spark_values['local_pyfiles_path']+'config_files/spark_conf.py',spark_values['local_pyfiles_path']+'other_files/create_table.hql'])
		##Config File Imports
		import spark_conf;utc_datetime = datetime.utcnow()
		##Importing Other PY files and Spark libraries 
		#sc = SparkContext(conf = conf,pyFiles=[spark_conf.file_path['otherfiles_path']+'spark_yelp.py',spark_conf.nlu_project_path+'spark_classifier_modified.py',spark_conf.file_path['otherfiles_path']+'spark_zipcode.py',spark_conf.file_path['otherfiles_path']+'spark_zomato.py',spark_conf.file_path['conf_path']+'create_table.hql'])
		from pyspark.sql import Row, SQLContext,HiveContext;from pyspark.sql.functions import *
		sqlContext = SQLContext(sc);hiveContext = HiveContext(sc)
		## Creating Class Objects By Calling Them
		ins = insights(sc)
		## Checking Python Version in Linux
		#ins.python_versioncheck(spark_conf.default_python_version1)
		##Importing all other py files and classes and functions 
		import spark_zipcode,spark_zomato,spark_classifier_modified as spark_classifier,spark_yelp,houston311_data
		from spark_zipcode import *;from spark_zomato import *;from spark_classifier_modified import *;from spark_yelp import *;from houston311_data import *
		zp = zpcode(sc);sz =zomato(sc);cl = classifier(sc);y = yelp(sc); hdt = data_311(sc)
		## Calling Functions in Spark_conf and checking paths, PIP Installs, directory permission checks
		ins.path_check(spark_conf.file_path,spark_conf.hdfs_path);
		#ins.dir_permission_change(spark_conf.permission_value,spark_conf.main_local_path)
		## Calling Functions for API Checking and Input Values Check and Create Table HQL File
		#ins.api_checking(y);ins.inputvalues_check();#ins.create_table()
		
		## Checking the If Condition whether zomato or Yelp needs to be called based on 1/0


		## Checking using IF condition whether to download the zipcodes file from website 
		if spark_conf.zip_codes['value'] == '1':
			ud = zp.url_download(spark_conf.zip_codes['url'],spark_conf.file_path['zip_file'],spark_conf.name['zip_name'])
			if ud == 1:
				uz = zp.unzip_url(spark_conf.file_path['zip_file'],spark_conf.name['zip_name'],spark_conf.name['zipfile_name'],spark_conf.file_path['file_output'])
			if uz == 1:
					zp.zipdata_validation(sc,sqlContext,Row,spark_conf.file_path['file_output'],spark_conf.hdfs_path['zipfile'],spark_conf.name['zipfile_name'],spark_conf.hdfs_path['validation_file'])
					mr = zp.move_remove(spark_conf.file_path['zip_file'],spark_conf.name['zip_name'],spark_conf.file_path['file_output'],spark_conf.name['zipfile_name'],spark_conf.hdfs_path['zipfile'])

		if spark_conf.program_execution['zomato'] == '1':

				zp.lat_long(sc,cl,sqlContext,hiveContext,Row,sz,spark_conf.zomato['zip_code'],spark_conf.zomato['lat_value'],spark_conf.zomato['long_value'],spark_conf.hdfs_path['zipfile'],sz.input_date(spark_conf.zomato['days']))
			
		if spark_conf.program_execution['yelp'] == '1':

			for key,zipcode in spark_conf.yelp.iteritems():
				if key == 'zip_code':
					for y_zipcode in zipcode:
						print y_zipcode +'++++++++++++++++++++++++'
						yelp_zipcode,yelp_lat,yelp_long =	y.get_zipcode(sc,sqlContext,spark_conf.hdfs_path['zipfile'],y_zipcode)
						print yelp_zipcode,yelp_lat,yelp_long
						response = y.search(spark_conf.yelp['access_token'],spark_conf.yelp['term'],yelp_lat,yelp_long,spark_conf.yelp['sort_by'],spark_conf.yelp['radius'])
						jsondata = response.get('businesses')
						y.yelp_looping(sc,sqlContext,response,cl,yelp_zipcode,yelp_lat,yelp_long)

		if spark_conf.program_execution['311_data'] == '1':
			hdt.file_download(sc,sqlContext,Row,cl,sz,spark_conf.links,spark_conf.file_path['311_localdata'],spark_conf.hdfs_path['311_textfile'],spark_conf.hdfs_path['311_querydata'])
	
		if spark_conf.program_execution['merge_data'] == '1':
			ins.merge_hdfs_data(spark_conf.merge_paths,spark_conf.main_local_path)




	# except Exception as e:
	# 	logging.info('error in main func: %s' %str(e))
