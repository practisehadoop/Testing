import logging,requests,sys,json,subprocess,time,pytz,spark_conf,spark_classifier_modified,uuid
from datetime import datetime,tzinfo, time, timedelta;from spark_classifier_modified import *
from subprocess import PIPE,Popen,call;from pyspark.sql.types import *
from pyspark.sql import Row;from pyspark.sql.functions import lit
from urllib2 import HTTPError;from urllib import quote;from urllib import urlencode




class yelp:
	def __init__(self,sc):
		logging.info('entered into init function of yelp')

	
	def get_zipcode(self,sc,sqlContext,zip_hdfspath,yelp_zipcode):
		logging.info('entered into get zip code function')
		data = sc.textFile(zip_hdfspath).map(lambda x: x.split('\t')).filter(lambda x: (x[0]).lower() != 'geoid' and x[0] != '').map(lambda y: Row(geoid = int(y[0]),aland = int(y[1]),awater = int(y[2]),aland_sqmi = float(y[3]), awater_sqmi = float(y[4]), intptlat = float(y[5]),intptlong = float(y[6])))
		sqlContext.createDataFrame(data).registerTempTable('new_data')
		q_str = 'select geoid,trim(intptlat) as lat,trim(intptlong) as long from new_data where geoid in ('+str(yelp_zipcode)+' )'
		data =  sqlContext.sql(q_str).count()
		if data > 0:
			re_data = sqlContext.sql(q_str).collect() 	
			return re_data[0]['geoid'],re_data[0]['lat'],re_data[0]['long']



	def input_date(self):
			try:
				ZERO = timedelta(0)
				class UTC(tzinfo):
				  def utcoffset(self, dt):
				    return ZERO
				  def tzname(self, dt):
				    return "UTC"
				  def dst(self, dt):
				    return ZERO
				utc = UTC()
				pre_date = datetime.now(utc) -timedelta(days= int(spark_conf.yelp['days']))
				print pre_date
				return pre_date
			except Exception as e:
				logging.info('error in update_date function  %s' %str(e))

	def convert_datetime_timezone(self,dt, tz1, tz2):

			tz1 = pytz.timezone(tz1);tz2 = pytz.timezone(tz2)
			dt =  datetime.strptime(dt,"%Y-%m-%d %H:%M:%S")
			dt = tz1.localize(dt);dt = dt.astimezone(tz2)
			return dt



	def obtain_bearer_token(self,host, path):
		logging.info('entered into obtain_bearer_token')
		url = '{0}{1}'.format(host, quote(path.encode('utf8')))

		data = urlencode({
		    'client_id': spark_conf.yelp['client_id'],
		    'client_secret': spark_conf.yelp['client_secret'] ,
		    'grant_type': GRANT_TYPE,
		})
		headers = {
		    'content-type': 'application/x-www-form-urlencoded',
		}
		response = requests.request('POST', url, data=data, headers=headers)
		bearer_token = response.json()['access_token']
		print response.json()
		print bearer_token
		return bearer_token




	def request(self,host, path, bearer_token, url_params=None):
		logging.info('entered into request function')
		url_params = url_params or {}
		url = '{0}{1}'.format(host, quote(path.encode('utf8')))
		headers = {
		    'Authorization': 'Bearer %s' % bearer_token,
		}

		print(u'Querying {0} ...'.format(url))

		response = requests.request('GET', url, headers=headers, params=url_params)
		print response.json()
		data = response.json()
		
		return data



	def search(self,bearer_token, term, latitude,longitude,sort_by,radius):
		logging.info('entered into search function')
		url_params = {
		    'term': term.replace(' ', '+'),
		    'latitude': latitude.replace(' ', '+'),
		    'longitude': longitude.replace(' ', '+'),
		    'limit': spark_conf.yelp['SEARCH_LIMIT'],
		    'radius':radius
		}
		print url_params
		#print request(API_HOST, SEARCH_PATH, bearer_token, url_params=url_params)
		return self.request(spark_conf.yelp['API_HOST'],spark_conf.yelp['SEARCH_PATH'],bearer_token, url_params=url_params)


	def yelp_restaurant(self,sc,sqlContext,jsondata):
		try:
			logging.info('entered into yelp restaurant');yr = [];ylp = str()
			if 'businesses' in jsondata:
				if len(jsondata['businesses']) > 0:
						if 'businesses' in jsondata:
							print 'entered into business'
							for i in jsondata['businesses']:
								print 'entered loop';yp = str()
								yp = yp + str(spark_conf.yelp['access_token']) + '|'
								if 'id' in i: yp = yp + (i['id']) + '|'
								else: yp = yp + 'unknown' + '|'
								if 'name' in i: yp = yp + (i['name']).encode('ascii','ignore').strip() + '|'
								else: yp = yp + 'unknown' + '|'
								if 'url' in i: yp = yp + (i['url']).encode('utf-8').strip() + '|'
								else: yp = yp + 'unknown' + '|'
								if 'image_url' in i: yp = yp + (i['image_url']).encode('utf-8').strip() + '|'
								else: yp = yp + 'unknown' + '|'
								if 'location' in i: 
									print 'location'
									if 'address' in i['location']: yp = yp + (i['location']['address']).encode('utf-8').strip() + '|'
									else: yp = yp + 'unknown' + '|'
									if 'state' in i['location']: yp = yp + (i['location']['state']).encode('utf-8').strip() + '|'
									else: yp = yp + 'unknown' + '|'
									if 'city' in i['location']: yp = yp + (i['location']['city']).encode('utf-8').strip() + '|'
									else: yp = yp + 'unknown' + '|'
									if 'city_id' in i['location']: yp = yp + (i['location']['city_id']).strip() + '|'
									else: yp = yp + 'unknown' + '|'
								if 'region' in jsondata:
								 	if 'latitude' in jsondata['region']['center']: yp = yp + str(jsondata['region']['center']['latitude']) + '|'
								 	else: yp = yp + 'unknown' + '|'
								 	if 'longitude' in jsondata['region']['center']: yp = yp + str(jsondata['region']['center']['longitude']) + '|'
								 	else: yp = yp + 'unknown' + '|'
								if 'location' in i: 
									if 'zip_code' in i['location']: yp = yp + str(i['location']['zip_code']) + '|'
									else: yp = yp + 'unknown' + '|'
									if 'country' in i['location']: yp = yp + (i['location']['country']).encode('utf-8').strip() + '|'
									else: yp = yp + 'unknown' + '|'
								if 'is_closed' in i: yp = yp + str(i['is_closed']) + '|'
								else: yp = yp + 'unknown' + '|'
								if 'rating' in i: yp = yp + str(i['rating']) + '|'
								else: yp = yp + 'unknown' + '|'
								if 'rating_text' in i: yp = yp + str(i['rating_text']) + '|'
								else: yp = yp + 'unknown' + '|'
								if 'rating_color' in i: yp = yp + str(i['rating_color']) + '|'
								else: yp = yp + 'unknown' + '|'
								if 'review_count' in i: yp = yp + str(i['review_count']) + '|'
								else: yp = yp + 'unknown' + '|'				
								yp = yp +  str(spark_conf.retrieved_time.replace(':','-'))
								#if 'total' in jsondata:yp = yp + str(jsondata['total']) + '|'
								#else: yp = yp + 'unknown' + '|'
								# if 'price' in i: yp = yp + str(i['price']) + '|'
								# else: yp = yp + 'unknown' + '|'
								# if 'distance' in i: yp = yp + str(i['distance']) + '|'
								# else: yp = yp + 'unknown' + '|'
								# if 'phone' in i: yp = yp + str(i['phone']) + '|'
								# else: yp = yp + 'unknown' + '|'
								
								# if 'display_phone' in i: yp = yp + (i['display_phone']).encode('utf-8').strip() + '|'
								# else: yp = yp + 'unknown' + '|'
								
								
								# if 'coordinates' in i:
								# 	print 'entered coordinates'
								# 	if 'latitude' in i['coordinates']:yp = yp + str(i['coordinates']['latitude']) + '|'
								# 	else: yp = yp + 'unknown' + '|'
								# 	if 'longitude' in i['coordinates']:yp = yp + str(i['coordinates']['longitude']) + '|'
								# 	else: yp = yp + 'unknown' + '|'
								
								
								yr.append(yp.encode('utf-8'))

						
						#print yr
						#f = open('/bdaas/log/res_yelp.txt','a');f.write("%s" %yp.encode('utf-8'));f.close()
						if len(yr) > 0:
							yr_rdd = sc.parallelize(yr).filter(lambda y: y != '').map(lambda x: x.split('|')).map(lambda x: str(x[0])+'|'+str(x[1])+'|'+ str(x[2])+'|'+ str(x[3])+'|'+ str(x[4])+'|'+ str(x[5])+'|'+ str(x[6])+'|'+ str(x[7])+'|'+str(x[8])+'|'+ str(x[9])+'|'+ str(x[10])+'|'+ str(x[11])+'|'+ str(x[12])+'|'+ str(x[13])+'|'+ str(x[14])+'|'+ str(x[15]) +'|'+ str(x[16]) +'|'+ str(x[17]) +'|'+ str(x[18])).saveAsTextFile(spark_conf.hdfs_path['yelp_restaurnt_data']+'%s.txt' %(spark_conf.utc_time()[1]))
							yr_r = sc.parallelize(yr).map(lambda x: x.split('|')).map(lambda y: Row(api_key = y[0],res_id = y[1],res_name = y[2],res_url = y[3],utm_source = y[4],res_location=y[5],res_locality=y[6],res_city=y[7],res_cityid=y[8],res_latitude=y[9],res_longitude=y[10],zip_code=y[11],country_id =y[12],locality_verbose=y[13],agg_rating=y[14],res_rating_text=y[15],rating_color=y[16],votes=y[17],retrieved_time=y[18]))
							df_res = sqlContext.createDataFrame(yr_r)
							for i in df_res.select('res_id','zip_code').collect():
								ylp = ylp + i['res_id'] + ',' + i['zip_code']  + '|'


							#print ylp.encode('utf-8')
							return ylp.encode('utf-8')
		except Exception as e:
			logging.info('error in yelp restaurant function  %s' %str(e))


	def yelp_reviews(self,sc,sqlContext,review_data,r_id,zip_code,yl,c_date):
		try:
			logging.info('entered into yelp reviews');yr = []
			if 'reviews' in review_data:
				if len(review_data['reviews']) > 0:
					for i in review_data['reviews'] :
						print 'entered reviews loop'; rd = str()
						print self.convert_datetime_timezone(i['time_created'],spark_conf.yelp['timezone_initial'] , spark_conf.yelp['timezone_final']); print c_date.replace(tzinfo=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')
						if self.convert_datetime_timezone(i['time_created'],spark_conf.yelp['timezone_initial'] , spark_conf.yelp['timezone_final']) > c_date.replace(tzinfo=pytz.UTC):
							rd = rd + r_id + '|'
							if 'time_created' in i: rd = rd + self.convert_datetime_timezone(i['time_created'],spark_conf.yelp['timezone_initial'] , spark_conf.yelp['timezone_final']).strftime("%Y-%m-%d %H:%M:%S") + '|'
							else: rd = rd + 'unknown' + '|'
							rd = rd + str(uuid.uuid4())+'_'+ self.convert_datetime_timezone(i['time_created'],spark_conf.yelp['timezone_initial'] , spark_conf.yelp['timezone_final']).strftime("%Y-%m-%d %H:%M:%S") + '|'
							if 'rating' in i: rd = rd + str(i['rating']) + '|'
							else: rd = rd + 'unknown' + '|'
							if 'url' in i: rd = rd + i['url'].encode('utf-8') + '|'
							else: rd = rd + 'unknown' + '|'
							if 'text' in i: rd = rd + i['text'].replace('\r','').replace('\n','').encode('utf-8').strip() + '|'
							else: rd = rd + 'unknown' + '|'
							if 'user' in i:
								if 'name' in i['user']: rd = rd + i['user']['name'].encode('utf-8') + '|'
								else: rd = rd + 'unknown' + '|'
								if 'image_url' in i['user'] : rd = rd + str(i['user']['image_url']) + '|'
								else: rd = rd + 'unknown' + '|'
							rd = rd + str(spark_conf.utctime) 
							yr.append(rd)
						#f = open('/bdaas/log/res_rev.txt','a');f.write("%s" %rd.encode('utf-8'));f.close()
							
				print len(yr)				
				yl =  yl + str(spark_conf.utctime) + '|' + r_id +'|' +  str(len(yr)) + '|' + '2'
				print yl
				sc.parallelize([yl.strip()]).saveAsTextFile(spark_conf.hdfs_path['zip_table']+'%s_%s_%s.txt' %(zip_code,r_id,spark_conf.utc_time()[1]))

				if len(yr) > 0:
						if str(sc.parallelize(yr).isEmpty()) == 'False':
							rdd_r = sc.parallelize(yr).filter(lambda y: y != '').map(lambda x: x.split('|')).map(lambda x: str(x[0])+'|'+str(x[1])+'|'+ str(x[2])+'|'+ str(x[3])+'|'+ str(x[4])+'|'+ str(x[5])+'|'+ str(x[6]) + '|' + str(x[7])+'|'+str(x[8])).saveAsTextFile(spark_conf.hdfs_path['yelp_temp_review_data']+'%s_%s.txt' %(zip_code,spark_conf.utc_time()[1]))
		
		except Exception as e:
			logging.info('error in yelp reviews function  %s' %str(e))				




	def yelp_looping(self,sc,sqlContext,jsondata,cl,zip_code,lat_v,long_v):
		try:
			logging.info('entered into yelp looping')
			url_params = {}
			r_id = self.yelp_restaurant(sc,sqlContext,jsondata)
			print r_id
			res = call(['hdfs','dfs','-test','-f',spark_conf.hdfs_path['zip_table']+'*/p*'])
			if res == 0:
				zp_log = sc.textFile(spark_conf.hdfs_path['zip_table']+'*').map(lambda x: x.split('|')).map(lambda l: filter(None,l)).map(lambda z: Row(quered_zipcode = (z[0]) ,original_zipcode = (z[1]) ,lat_value = z[2],long_value = z[3],sm_name =z[4],retrieving_time =z[5],restaurant_id =z[6],count_val =z[7],check_value=int(z[8])))
				sqlContext.createDataFrame(zp_log).registerTempTable('temp_yelp_log')
				f = sqlContext.sql('select count(*) as count from temp_yelp_log').collect()
				if f[0]['count'] > 0 :
					q_str = "select count(*) as count from temp_yelp_log where trim(lat_value) = trim('%s') and trim(long_value) = trim('%s') and check_value = 2"  %(lat_v,long_v) 
					print q_str
					c = sqlContext.sql(q_str).collect()
					print c[0]['count']
					if c[0]['count'] > 0:
							dt = sqlContext.sql("select max(cast(from_unixtime(unix_timestamp(retrieving_time,'yyyy-MM-dd HH:mm:ss')) as timestamp)) as max_date from temp_yelp_log where trim(lat_value) = trim('%s') and trim(long_value) = trim('%s') and check_value = 2"  %(lat_v,long_v)).collect()
							print dt
							previous_date= dt[0]['max_date']
							print previous_date
							for i in r_id.split('|'):
								print i;print i.decode('ascii', 'ignore').encode('ascii');print '1st loop'
								if i != '':
									review_path= spark_conf.yelp['REVIEW_PATH'].replace('id',i.split(',')[0].decode('ascii', 'ignore').encode('ascii'))
									print review_path;yl = str()
									yl = yl + str(zip_code) + '|' + i.split(',')[1] + '|' + str(lat_v)+ '|' + str(long_v) + '|' + spark_conf.yelp['API_HOST'] + '|'
									print yl
									review_data =  self.request(spark_conf.yelp['API_HOST'],review_path,spark_conf.yelp['access_token'], url_params=url_params)
									self.yelp_reviews(sc,sqlContext,review_data,i.split(',')[0],zip_code,yl,previous_date)
					else:

						if len(r_id) > 0:
							for i in r_id.split('|'):
								print i;print i.decode('ascii', 'ignore').encode('ascii');print '2nd loop'
								if i != '':
									review_path= spark_conf.yelp['REVIEW_PATH'].replace('id',i.split(',')[0].decode('ascii', 'ignore').encode('ascii'))
									print review_path;yl = str()
									yl = yl + str(zip_code) + '|' + i.split(',')[1] + '|' + str(lat_v)+ '|' + str(long_v) + '|' + spark_conf.yelp['API_HOST'] + '|'
									review_data =  self.request(spark_conf.yelp['API_HOST'],review_path,spark_conf.yelp['access_token'], url_params=url_params)
									self.yelp_reviews(sc,sqlContext,review_data,i.split(',')[0],zip_code,yl,self.input_date())





			else:
				if len(r_id) > 0:
					for i in r_id.split('|'):
						print i;print i.decode('ascii', 'ignore').encode('ascii');print '3rd loop'
						if i != '':
							review_path= spark_conf.yelp['REVIEW_PATH'].replace('id',i.split(',')[0].decode('ascii', 'ignore').encode('ascii'))
							print review_path;yl = str()
							yl = yl + str(zip_code) + '|' + i.split(',')[1] + '|' + str(lat_v)+ '|' + str(long_v) + '|' + spark_conf.yelp['API_HOST'] + '|'
							review_data =  self.request(spark_conf.yelp['API_HOST'],review_path,spark_conf.yelp['access_token'], url_params=url_params)
							self.yelp_reviews(sc,sqlContext,review_data,i.split(',')[0],zip_code,yl,self.input_date())





			d = call(['hdfs','dfs','-test','-f',spark_conf.hdfs_path['yelp_temp_review_data']+'*/p*'])
			if d == 0:
					r_path = spark_conf.hdfs_path['yelp_temp_review_data']+'*'
					print r_path
					rdd_data = sc.textFile(r_path, use_unicode=False)
					if rdd_data.count() > 0:
						print rdd_data.count()
						rdd_r= rdd_data.filter(lambda y: y != '').map(lambda x: x.split('|')).map(lambda z: Row(restaurant_id=z[0], time_created = z[1],review_id=z[2], rating = z[3],url= z[4], review_text = z[5],user_name=z[6],user_image=z[7],retrieved_time=z[8]))
						df_old = sqlContext.createDataFrame(rdd_r);df_new=df_old.select('*').withColumn('rating_color', lit('None').cast(StringType())).withColumn('rating_time_friendly', lit('None').cast(StringType())).withColumn('rating_text', lit('None').cast(StringType())).withColumn('likes', lit('None').cast(StringType())).withColumn('comment_count' , lit('None').cast(StringType())).withColumn('user_zomatohandle' , lit('None').cast(StringType())).withColumn('user_foodie_level' , lit('None').cast(StringType())).withColumn('user_level_num' , lit('None').cast(StringType())).withColumn('foodie_color' , lit('None').cast(StringType())).withColumn('profile_url', lit('None').cast(StringType()))
						data_r = df_new.select('restaurant_id','rating', 'review_id','review_text','rating_color' ,'rating_time_friendly','rating_text','time_created','likes','comment_count','user_name','user_zomatohandle','user_foodie_level','user_level_num','foodie_color','profile_url','user_image','retrieved_time')	
						data_r.show()
						if 1 == cl.nlc(data_r):                                                                
							if 1 == cl.confident_classifier(sqlContext,data_r)[1]:
								sqlContext.createDataFrame(cl.confident_classifier(sqlContext,data_r)[0]).withColumn('score', lit('None').cast(StringType())).select('comment_count','foodie_color','likes','user_image','profile_url','rating','rating_color','rating_text','rating_time_friendly', 'restaurant_id','retrieved_time', 'review_id','review_text','time_created','user_foodie_level','user_level_num','user_name','user_zomatohandle','class_name','confidence','score').rdd.map(lambda x: list(x)).map(lambda y: filter_data(y)).saveAsTextFile(spark_conf.hdfs_path['classifier_output']+ '%s.txt' %spark_conf.utc_time()[1])
								call(['hdfs','dfs','-mv',spark_conf.hdfs_path['yelp_temp_review_data']+'*',spark_conf.hdfs_path['yelp_final_review_data']])

		except Exception as e:
			logging.info('error in yelp loop function  %s' %str(e))
















# if __name__ == "__main__":
# 	import os,logging,sys,time,pandas,json,requests,pytz;from subprocess import PIPE,Popen,call;from datetime import datetime, time, timedelta
# 	logging.basicConfig(filename='/bdaas/log/spark_yelp.log',filemode='w',level = logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
# 	from urllib2 import HTTPError;from urllib import quote;from urllib import urlencode
# 	from pyspark import SparkContext, SparkConf
# 	conf = SparkConf().setAppName('yelp')
# 	sc = SparkContext(conf = conf,pyFiles=['/bdaas/exe/spark_zomato/conf_files/spark_conf.py','/bdaas/exe/nlu_project/spark_classifier.py'])
# 	from pyspark.sql import Row, SQLContext,HiveContext
# 	from pyspark.sql.functions import lit; import spark_conf,spark_classifier
# 	sqlContext = HiveContext(sc)
# 	y = yelp(sc);from spark_classifier import *;cl = classifier(sc)
# 	#bearer_token = y.obtain_bearer_token(spark_conf.yelp['API_HOST'] ,spark_conf.yelp['TOKEN_PATH'])
# 	response = y.search(spark_conf.yelp['access_token'],spark_conf.yelp['term'],spark_conf.yelp['latitude'],spark_conf.yelp['longitude'],spark_conf.yelp['sort_by'])
# 	jsondata = response.get('businesses')
# 	jsondata = response
# 	with open('yelpdata.txt','w') as f:
# 			json.dump(jsondata,f)
# 	y.yelp_looping(jsondata,cl)
	


	#business_id = businesses[0]['id']
	#print business_id
