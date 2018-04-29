import logging,urllib2,zipfile,os,spark_conf,datetime
from subprocess import Popen, PIPE,call
from pyspark.sql.functions import lit





class zpcode:

	def __init__(self,sc):
		logging.info('entered into zip code init function')

	def get_zipcode(self,lat_val,long_val):
		logging.info('entered into get zip code function')
		from geopy.geocoders import Nominatim
		geolocator = Nominatim()
		val = lat_val + ',' + long_val
		print val
		#print geolocator.reverse(val)
		location = geolocator.reverse(val)
		print (location.raw['address']['postcode'].encode('utf-8').strip())
		return location.raw['address']['postcode']





	def url_download(self,url,zip_path,zip_name):
		try:
			logging.info('entered into url download')
			response = urllib2.urlopen(url)
			zipcontent = response.read()
			logging.info('completed downloading the url and reading it')
			with open(zip_path+zip_name, 'w') as f:
				f.write(zipcontent)
			logging.info('completed writing the zip content into file')
			return 1
		except Exception as e:
			logging.info('error in url_download func: %s' %str(e))


	def unzip_url(self,zip_path,zip_name,zipfile_name,file_output):
		try:
			logging.info('entered into unzip url')
			zip = zipfile.ZipFile(zip_path+zip_name)
			zip.extractall(file_output)
			zip.close()
			a = os.listdir(file_output);os.rename(file_output+a[0],file_output+zipfile_name)
			logging.info('completed writing to file')
			return 1
		except Exception as e:
			logging.info('error in unzip_url func: %s' %str(e))


	def zipdata_validation(self,sc,sqlContext,Row,file_output,zip_hdfspath,zipfile_name,validation_data):
		try:
			logging.info('entered into zip data validation')
			a = call(['hdfs','dfs','-test','-f',zip_hdfspath+zipfile_name])
			if a == 1:m = call(['hdfs','dfs','-put',file_output+zipfile_name,zip_hdfspath])
			if a == 0:
				b = call(['hdfs','dfs','-test','-f',zip_hdfspath+validation_data])
				if b == 1: call(['hdfs','dfs','-put',file_output+zipfile_name,validation_data])
				sqlContext.sql('set spark.sql.shufftle.partitions = 50')
				data_o = sc.textFile(zip_hdfspath).map(lambda x: x.split('\t')).filter(lambda x: (x[0]).lower() != 'geoid' and x[0] != '').map(lambda y: Row(geoid = int(y[0]),aland = int(y[1]),awater = int(y[2]),aland_sqmi = float(y[3]), awater_sqmi = float(y[4]), intptlat = float(y[5]),intptlong = float(y[6])))
				data_of = sqlContext.createDataFrame(data_o);data_of.registerTempTable('zip_data')
				data_v = sc.textFile(validation_data).map(lambda x: x.split('\t')).filter(lambda x: (x[0]).lower() != 'geoid'  and x != '').map(lambda y: Row(gid = int(y[0]),land = int(y[1]),water = int(y[2]),land_sqmi = float(y[3]), water_sqmi = float(y[4]), lat = float(y[5]),long = float(y[6])))
				data_vf = sqlContext.createDataFrame(data_v);data_vf.registerTempTable('vd_data')
				b = sqlContext.sql('select vd_data.gid from vd_data LEFT OUTER JOIN zip_data ON(zip_data.geoid = vd_data.gid) WHERE zip_data.geoid IS null')
				b.show()
				if len(b.collect()) > 0: 
					call(['hdfs','dfs','-put','-f',file_output+zipfile_name,zip_hdfspath])
				call(['hdfs','dfs','-rm','-r',validation_data+zipfile_name])
		except Exception as e:
			logging.info('error in zipdata_validation func: %s' %str(e))



	def move_remove(self,zip_path,zip_name,file_output,zipfile_name,zip_hdfspath):
		try:

			logging.info('entered into move remove function')
			
			call(['rm','-r',zip_path+zip_name]); call(['rm','-r',file_output+zipfile_name])
			logging.info('completed moving file to HDFS and removing it')
			return 1
		except Exception as e:
			logging.info('error in move_remove func: %s' %str(e))


	def lat_long(self,sc,cl,sqlContext,hiveContext,Row,sz,zipcode,lat,long,zip_hdfspath,comp_date):
		#try:
			logging.info('entered into lat long function')
			
			
			if zipcode != '':
				logging.info('entered into zipcode not empty if loop')
				data = sc.textFile(zip_hdfspath).map(lambda x: x.split('\t')).filter(lambda x: (x[0]).lower() != 'geoid' and x[0] != '').map(lambda y: Row(geoid = int(y[0]),aland = int(y[1]),awater = int(y[2]),aland_sqmi = float(y[3]), awater_sqmi = float(y[4]), intptlat = float(y[5]),intptlong = float(y[6])))
				sqlContext.createDataFrame(data).registerTempTable('new_data')
				q_str = 'select geoid,trim(intptlat) as lat,trim(intptlong) as long from new_data where geoid in ('+str(zipcode)+' )'
				sqlContext.sql(q_str).show()
				a = sqlContext.sql(q_str);values= a.collect()	
				print values;res_id = []
				for i in values:
						print i['geoid'],i['lat'],i['long']
						lc = sz.zomato_location(sc,sqlContext,i['geoid'],spark_conf.zomato['search_api'],i['lat'],i['long'],spark_conf.zomato['radius'],spark_conf.zomato['category_by'],spark_conf.zomato['sort_by'],spark_conf.zomato['order_by'],spark_conf.zomato['apikey'])
						res_id.append(lc.split('|'))
				print res_id
				for i in res_id:
					 	print len(i)

				ret = call(['hdfs','dfs','-test','-f',spark_conf.hdfs_path['zip_table']+'*/p*'])
				if ret == 0:
						#b = open(spark_conf.file_path['reviewdata_path']+'reviews_%s.txt'% spark_conf.retrieved_time.replace(':','-'),'a');h = 'restaurant_id|rating|review_id|review_text|rating_color|review_time_friendly|rating_text|time_stamp|likes|comment_count|user_name|user_zomatohandle|user_foodie_level|user_level_num|foodie_color|profile_url|profile_image|retrieved_time';b.write('%s\n' %h);b.close()
						zp_log = sc.textFile(spark_conf.hdfs_path['zip_table']+'*').map(lambda x: x.split('|')).map(lambda l: filter(None,l)).map(lambda z: Row(quered_zipcode = (z[0]) ,original_zipcode = (z[1]) ,lat_value = z[2],long_value = z[3],sm_name =z[4],retrieving_time =z[5],restaurant_id =z[6],count_val =z[7],check_value=z[8]))
						sqlContext.createDataFrame(zp_log).registerTempTable('temp_zomato_log')
						f = sqlContext.sql('select count(*) as count from temp_zomato_log').collect()
						if f[0]['count'] > 0 :
							for i in values:
								print i['geoid'],i['lat'],i['long']
								c = sqlContext.sql("select count(*) as count from temp_zomato_log where quered_zipcode = ('%s') and check_value = 1" %i['geoid']).collect()
								print c[0]['count']
								if c[0]['count'] > 0:
									dt = sqlContext.sql("select max(cast(from_unixtime(unix_timestamp(retrieving_time,'yyyy-MM-ddHH-mm-ss')) as timestamp)) as max_date from temp_zomato_log where quered_zipcode = ('%s')  and check_value = 1" %i['geoid']).collect()
									previous_date= dt[0]['max_date']
									print previous_date
									sz.loop(sc,cl,res_id,sqlContext,previous_date,i['geoid'],i['lat'],i['long'])
								else:
									if len(res_id) > 0:
										sz.loop(sc,cl,res_id,sqlContext,comp_date,i['geoid'],i['lat'],i['long'])
					 				



				else:
					logging.info('entered into else loop')
					

					if len(res_id) > 0:
						print 'entered outside else loop'
						for i in values:
							print i['geoid'],i['lat'],i['long']
						 	sz.loop(sc,cl,res_id,sqlContext,comp_date,str(),i['lat'],i['long'])



			# if zipcode == '' and (spark_conf.zomato['lat_value'] != ''  and spark_conf.zomato['long_value'] != ''):
			# 	logging.info('entered into loop where zipcode is empty and lat and long values are given')
			# 	### Open Street Implementation
			# 	zip_code = self.get_zipcode(spark_conf.zomato['lat_value'],spark_conf.zomato['long_value']);res_id = []
			# 	lc = sz.zomato_location(sc,sqlContext,zip_code,spark_conf.zomato['search_api'],spark_conf.zomato['lat_value'],spark_conf.zomato['long_value'],spark_conf.zomato['radius'],spark_conf.zomato['category_by'],spark_conf.zomato['sort_by'],spark_conf.zomato['order_by'],spark_conf.zomato['apikey'])
			# 	res_id.append(lc.split('|'))
			# 	print res_id;print len(res_id);print 'entered'
			# 	ret = call(['hdfs','dfs','-test','-f',spark_conf.hdfs_path['zip_table']+'*/p*'])
			# 	if ret == 0:
			# 		zp_log = sc.textFile(spark_conf.hdfs_path['zip_table']+'*').map(lambda x: x.split('|')).map(lambda l: filter(None,l)).map(lambda z: Row(quered_zipcode = int(z[0]) ,original_zipcode = int(z[1]) ,lat_value = z[2],long_value = z[3],sm_name =z[4],retrieving_time =z[5],restaurant_id =z[6],count_val =z[7]))
			# 		sqlContext.createDataFrame(zp_log).registerTempTable('temp_zomato_log')
			# 		f = sqlContext.sql('select count(*) as count from temp_zomato_log').collect()
			# 		if f[0]['count'] > 0 :
			# 			q_str = "select count(*) as count from temp_zomato_log where trim(lat_value) = '%s' and trim(long_value) = '%s' "  %(spark_conf.zomato['lat_value'],spark_conf.zomato['long_value']) 
			# 			print q_str
			# 			c = sqlContext.sql(q_str).collect()
			# 			print c[0]['count']
			# 			if c[0]['count'] > 0:
			# 				dt = sqlContext.sql("select max(cast(from_unixtime(unix_timestamp(retrieving_time,'yyyy-MM-ddHH-mm-ss')) as timestamp)) as max_date from temp_zomato_log where trim(lat_value) = trim('%s') and trim(long_value) = trim('%s') "  %(spark_conf.zomato['lat_value'],spark_conf.zomato['long_value']) ).collect()
			# 				previous_date= dt[0]['max_date']
			# 				print previous_date
			# 				sz.loop(sc,cl,res_id,sqlContext,previous_date,zip_code,spark_conf.zomato['lat_value'],spark_conf.zomato['long_value'])
			# 			else:
			# 				if len(res_id) > 0:
									
			# 						sz.loop(sc,cl,res_id,sqlContext,comp_date,zip_code,spark_conf.zomato['lat_value'],spark_conf.zomato['long_value'])



			# 	else:
			# 		if len(res_id) > 0:
			# 				sz.loop(sc,cl,res_id,sqlContext,comp_date,str(),spark_conf.zomato['lat_value'],spark_conf.zomato['long_value'])

		#except Exception as e:
			#logging.info('error in lat long func: %s' %str(e))