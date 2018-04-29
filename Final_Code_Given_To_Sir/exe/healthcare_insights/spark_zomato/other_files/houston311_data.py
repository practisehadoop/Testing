
import logging,requests,sys,json,subprocess,time,spark_conf,pytz
from datetime import datetime, time, timedelta,tzinfo
from subprocess import PIPE,Popen,call
from pyspark.sql.functions import lit



def rdd_structure(data):
	s = str()
	for i in data:
			s = s + i.strip().replace('\n','').replace('\r','') + '|' 
	return s


class data_311:

	def __init__(self,sc):
		logging.info('entered into 311 data code init function')






	def dataframe_creation(self,sc,sqlContext,Row,cl,links,local_path,hdfs_path,querydata_path,date_311):
		try:
			logging.info('entered into dataframe_creation function')
			print date_311
			rdd_data = sc.textFile(hdfs_path);header = rdd_data.first()
			rdd_new = rdd_data.filter(lambda line: line != header).map(lambda x: x.split('|')).map(lambda z: Row(case_number =z[0],sr_location =z[1],county =z[2],district =z[3],neighborhood =z[4],tax_id =z[5],trash_quad =z[6],recycle_quad =z[7],trash_day =z[8],heavytrash_day =z[9],recycle_day =z[10],key_map =z[11],management_district =z[12],department =z[13],division =z[14],sr_type =z[15],queue =z[16],sla =z[17],status =z[18],sr_createdate =z[19],due_date =z[20],date_closed =z[21],overdue =z[22],title =z[23],x =z[24],y =z[25], latitude =z[26],longitude =z[27],channel_type =z[28]))
			#print rdd_new.map(lambda d: date_conversion(d)).take(10)
			df_data = sqlContext.createDataFrame(rdd_new).registerTempTable('311data')
			df_new = sqlContext.sql('select * from 311data where regexp_replace(lower(trim(sr_type))," ","") in ("waterleak","poordrainage","airpollution","crisiscleanup","deadanimalcollection","drainage","drainagesystemviolation","healthcode","heavytrashviolation","majorwaterleak","missedgarbagepickup","missedheavytrashpickup","missedyardewastepickup","pestcontrol","poolwaterqualitycontrol","sewermanhole","sewerwastewater","sidewalkrepair","streethazard","waterplaygroundrepair","waterquality","waterservice","waterorgroundpollution") and to_utc_timestamp (sr_createdate,"US/Central") > "%s" ' %date_311).select('case_number','sr_location' ,'county' ,'district' ,'neighborhood' ,'tax_id' ,'trash_quad' ,'recycle_quad','trash_day','heavytrash_day','recycle_day','key_map','management_district' ,'department' ,'division' ,'sr_type','queue','sla','status','sr_createdate' ,'due_date' ,'date_closed' ,'overdue' ,'title' ,'x' ,'y' , 'latitude' ,'longitude','channel_type')
			df_new.show()
			df_new.rdd.map(lambda y: list(y)).map(lambda z: rdd_structure(z)).coalesce(1).saveAsTextFile(querydata_path+'311query_data_%s.txt' %(spark_conf.utc_time()[1]))
	 		df_anc = df_new.withColumn('311_rating',lit('Nan')).withColumn('311_color',lit('Nan')).withColumn('311_rating_text',lit('Nan')).withColumn('311_likes',lit('Nan')).withColumn('311_comment_count',lit('Nan')).withColumn('311_user_name',lit('Nan')).withColumn('311_user_handle',lit('Nan')).withColumn('311_user_foodie_level',lit('Nan')).withColumn('311_user_level_num',lit('Nan')).withColumn('311_foodie_color',lit('Nan')).withColumn('311_profile_url',lit('Nan')).withColumn('311_user_image',lit('Nan')).withColumn('311_retrieved_time',lit(spark_conf.retrieved_time.replace(':','-'))).withColumnRenamed("sr_type", "review_text").select('tax_id','311_rating','case_number','review_text','311_color','date_closed','311_rating_text','sr_createdate','311_likes','311_comment_count','311_user_name','311_user_handle','311_user_foodie_level','311_user_level_num','311_foodie_color','311_profile_url','311_user_image','311_retrieved_time')
	 		return df_anc
 		except Exception as e:
			logging.info('error in dataframe_creation func: %s' %str(e))

	def file_download(self,sc,sqlContext,Row,cl,sz,links,local_path,hdfs_path,querydata_path):
		try:
			logging.info('entered into file_download function');import wget,os;l = str()
			for i in links:
				if spark_conf.values_311data['file_download'] == '1':
					print '++++++++++++entered++++++++++++'
					file_name = wget.download('http://hfdapp.houstontx.gov/311/'+i)
					call(['hdfs','dfs','-put','-f',i,hdfs_path])
					os.remove(i)
				return_value,return_date=self.log_checking(sc,sqlContext,Row,sz)
				if return_value in (0,1):
					df_anc = self.dataframe_creation(sc,sqlContext,Row,cl,links,local_path,hdfs_path,querydata_path,return_date)
					print df_anc.count()
					if df_anc.count() > 0:
						pass
				 		print 'Not Empty'
						if 1 == cl.nlc(df_anc):
							if 1 == cl.confident_classifier(sqlContext,df_anc)[1]:
								print 'finished'
								# Move command here

					l = l + 'Nan' + '|' + 'Nan' + '|' + 'Nan' + '|' + 'Nan' + '|' + '311_data' + '|' + str(spark_conf.utctime.replace(':','-')) + '|' + 'Nan' + '|' + 'Nan' '|' + '3'
					#l = l + str(00) + '|' + str(00) + '|' + 'Nan' + '|' + 'Nan' + '|' + '311_data' + '|' + str('2017-12-1 19-32-59') + '|' + 'Nan' + '|' + 'Nan' + '|' + 'Nan' 
					print (l)
					sc.parallelize([l]).saveAsTextFile(spark_conf.hdfs_path['zip_table']+'%s' %(spark_conf.utc_time()[1]))
			
		except Exception as e:
			logging.info('error in file_download func: %s' %str(e))				




			

	def log_checking(self,sc,sqlContext,Row,sz):
		try:
			ret = call(['hdfs','dfs','-test','-f',spark_conf.hdfs_path['zip_table']+'*/p*'])
			if ret == 0:
				logging.info('entered into log_checking res 0 if loop')
		 		zp_log = sc.textFile(spark_conf.hdfs_path['zip_table']+'*').map(lambda x: x.split('|')).map(lambda l: filter(None,l)).map(lambda z: Row(quered_zipcode = (z[0]) ,original_zipcode = (z[1]) ,lat_value = z[2],long_value = z[3],sm_name =z[4],retrieving_time =z[5],restaurant_id =z[6],count_val =z[7],check_value=z[8]))
		 		sqlContext.createDataFrame(zp_log).registerTempTable('temp_houston_log')
		 		f = sqlContext.sql('select count(*) as count from temp_houston_log where check_value = 3').collect()
				if f[0]['count'] > 0 :				
				 			dt_311 = sqlContext.sql('select max(cast(from_unixtime(unix_timestamp(retrieving_time,"yyyy-MM-dd HH-mm-ss")) as timestamp)) as max_date from temp_houston_log where check_value = 3').collect() 
							latest_date = dt_311[0]['max_date'];print '+++++++++++++++++++++++++++++++++++++++++++++already log exist++++++++++++++++++++++',latest_date
							return 1,latest_date
				else:
					return  0, sz.input_date(spark_conf.values_311data['days'])

			elif ret == 1:
					print 'no log'
					return 0, sz.input_date(spark_conf.values_311data['days'])
		except Exception as e:
			logging.info('error in log_checking func: %s' %str(e))

			
			

			
			
	
# .select('restaurant_id','rating', 'review_id','review_text','rating_color' ,'rating_time_friendly','rating_text','time_created','likes','comment_count','user_name','user_zomatohandle','user_foodie_level','user_level_num','foodie_color','profile_url','user_image','retrieved_time','class_name','confidence','score')
# def file_read():
# 	rdd_data = sc.textFile(hdfs_path).map(lambda x: x.split('|')).map(lambda z: Row(case_number =z[0],location =z[1],country =z[2],distinct_v =z[3],neighborhood =z[4],tax_id =z[5],trash_quad =z[6],recycle_quad =z[7],trash_day =z[8],heavytrash_day =z[9],recycle_day =z[10],key_map =z[11],management_district =z[12],dep =z[13],division =z[14],sr_type =z[15],queue =z[16],sla =z[17],status =z[18],sr_createdate =z[19],due_date =z[20],date_closed =z[21],overdue =z[22],title =z[23],x =z[24],y =z[25], latitude =z[26],longitude =z[27],channel_type =z[28]))
# 	df_data = sqlContext.createDataFrame(rdd_data) 
# 	df_data.registerTempTable('311data')
# 	sqlContext.sql('select * from 311data where regexp_replace(lower(trim(sr_type))," ","")="waterleak"').select('case_number',)

 




# if __name__=='__main__':
# 	from pyspark import SparkConf,SparkContext
# 	import os,logging,sys,time,json,pytz
# 	from datetime import datetime
# 	from subprocess import PIPE,Popen,call
# 	conf = SparkConf().setAppName('311').setMaster('yarn-client')
# 	sc = SparkContext(conf = conf,pyFiles=['/bdaas/exe/spark_zomato/other_files/spark_yelp.py','/bdaas/exe/nlu_project/spark_classifier.py','/bdaas/exe/spark_zomato/other_files/spark_zipcode.py','/bdaas/exe/spark_zomato/other_files/spark_zomato.py','/bdaas/exe/spark_zomato/conf_files/spark_conf.py','/bdaas/exe/spark_zomato/conf_files/create_table.hql'])
# 	from spark_conf import *
# 	from pyspark.sql import Row, SQLContext,HiveContext
# 	from pyspark.sql.functions import *
# 	sqlContext = SQLContext(sc)
# 	hiveContext = HiveContext(sc)
# 	#file_read()		
	

# 	file_download()
























# def data_cleaning(dt):
# 			s = str()
# 			#print dt.take(100)
# 			for i in dt.replace('\r','').strip().split('|')[56:100]:
# 			 	if 'warning'  in i.replace(' ','').lower().strip():
# 			 		index = i.replace(' ','').lower().strip().index('warning')
# 			 		s = s + i[0:index].replace('\r','').replace('---','').strip()

# 			 	if 'warning' not in i.replace(' ','').lower().strip():
# 					s = s + i.replace('\r','').replace('---','').strip() + '|'

# 			print s
# 			return s
