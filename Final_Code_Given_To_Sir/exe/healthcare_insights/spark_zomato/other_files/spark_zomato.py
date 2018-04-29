import logging,requests,sys,json,subprocess,time,spark_conf,pytz
from datetime import datetime, time, timedelta,tzinfo
from subprocess import PIPE,Popen,call
from pyspark.sql import Row

### map(lambda x : '\t'.join(str(d) for d in x)


class zomato:

	def __init__(self,sc):
		logging.info('entered into zomato code init function')
		



	def zomato_location(self,sc,sqlContext,zipcode,api_link,lat_value,long_value,radius,category_by,sort_by,order_by,apikey):
		#try:            
			logging.info('entered into zomato location');lc = str();p=[]
			print api_link+'lat='+str(lat_value)+'&lon='+str(long_value)+'&radius='+str(radius)+'&category='+str(category_by)+'&sort='+str(sort_by)+'&order='+str(order_by)+'&apikey='+apikey
			search = json.loads(requests.get(api_link+'lat='+str(lat_value)+'&lon='+str(long_value)+'&radius='+str(radius)+'&category='+str(category_by)+'&sort='+str(sort_by)+'&order='+str(order_by)+'&apikey='+apikey).text)
			#print search
			if 'restaurants' in search:
				if len(search['restaurants']) > 0:

					for i in search['restaurants']:   
						ser = str()                 
						if 'restaurant' in i:
							if 'apikey' in i['restaurant']: ser = ser + str(i['restaurant']['apikey']) + '|'
							else: ser = ser + 'unknown' + '|'
							if 'id' in i['restaurant']: ser = ser + str(i['restaurant']['id']) + '|'
							else: ser = ser + 'unknown' + '|'
							if 'name' in i['restaurant']: ser = ser +  str(i['restaurant']['name'].encode('ascii','ignore').decode('ascii')) + '|'
							else: ser = ser + 'unknown' + '|'
							if 'url' in i['restaurant']: ser = ser + str(i['restaurant']['url'].encode('ascii','ignore').decode('ascii')) + '|'
							else: ser = ser + 'unknown' + '|'
							if 'utm_source' in i['restaurant']: ser = ser + str(i['restaurant']['utm_source']) + '|'
							else: ser = ser + 'unknown' + '|'
						if 'location' in i['restaurant']:
							if 'address' in i['restaurant']['location']: ser = ser + i['restaurant']['location']['address'] + '|'
							else: ser = ser + 'unknown' + '|'
							if 'locality' in i['restaurant']['location']: ser = ser + i['restaurant']['location']['locality'] + '|'
							else: ser = ser + 'unknown' + '|'
							if 'city' in i['restaurant']['location']: ser = ser + i['restaurant']['location']['city'] + '|'
							else: ser = ser + 'unknown' + '|'
							if 'city_id' in i['restaurant']['location']: ser = ser + str(i['restaurant']['location']['city_id']) + '|'
							else: ser = ser + 'unknown' + '|'
							if 'latitude' in i['restaurant']['location']: ser = ser + i['restaurant']['location']['latitude'] + '|'
							else: ser = ser + 'unknown' + '|'
							if 'longitude' in i['restaurant']['location']: ser = ser + i['restaurant']['location']['longitude'] + '|'
							else: ser = ser + 'unknown' + '|'
							if 'zipcode' in i['restaurant']['location']: ser = ser + i['restaurant']['location']['zipcode'] + '|'
							else: ser = ser + 'unknown' + '|'
							if 'country_id' in i['restaurant']['location']: ser = ser + str(i['restaurant']['location']['country_id']) + '|'
							else: ser = ser + 'unknown' + '|'
							if 'locality_verbose' in i['restaurant']['location']: ser = ser + i['restaurant']['location']['locality_verbose'] + '|'
							else: ser = ser + 'unknown' + '|'
						if 'user_rating' in i['restaurant']:
							if 'aggregate_rating' in i['restaurant']['user_rating']: ser = ser + str(i['restaurant']['user_rating']['aggregate_rating']) + '|'
							else: ser = ser + 'unknown' + '|'
							if 'rating_text' in i['restaurant']['user_rating']: ser = ser + str(i['restaurant']['user_rating']['rating_text'].replace('\r','').replace('\n','')) + '|'
							else: ser = ser + 'unknown' + '|'
							if 'rating_color' in i['restaurant']['user_rating']: ser = ser + str(i['restaurant']['user_rating']['rating_color']) + '|'
							else: ser = ser + 'unknown' + '|'
							if 'votes' in i['restaurant']['user_rating']: ser = ser + str(i['restaurant']['user_rating']['votes']) + '|'
							else: ser = ser + 'unknown' + '|' 
						ser = ser + str(spark_conf.retrieved_time.replace(':','-')) 
						p.append(ser.encode('utf-8','replace'))
						#s  = open(spark_conf.file_path['searchdata_path']+'search_%s.txt'% spark_conf.retrieved_time.replace(':','-'),'a');s.write('%s' %ser.encode('utf-8','replace'));s.close()


				if len(p) > 0:
					res_save = sc.parallelize(p).filter(lambda y: y != '').map(lambda x: x.split('|')).map(lambda x: str(x[0])+'|'+str(x[1])+'|'+ str(x[2])+'|'+ str(x[3])+'|'+ str(x[4])+'|'+ str(x[5])+'|'+ str(x[6])+'|'+ str(x[7])+'|'+str(x[8])+'|'+ str(x[9])+'|'+ str(x[10])+'|'+ str(x[11])+'|'+ str(x[12])+'|'+ str(x[13])+'|'+ str(x[14])+'|'+ str(x[15]) +'|'+ str(x[16]) +'|'+ str(x[17]) +'|'+ str(x[18])).saveAsTextFile(spark_conf.hdfs_path['restaurant_data']+'%s_%s.txt' %(zipcode,spark_conf.utc_time()[1]))
					rdd_rest = sc.parallelize(p).map(lambda x: x.split('|')).map(lambda y: Row(apikey=y[0],rest_id = int(y[1]), rest_name = y[2],rest_url=y[3],rest_utmsource=y[4],rest_addr=y[5],rest_loclty=y[6],rest_city=y[7],res_cityid=y[8],rest_lat = y[9],rest_long=y[10],rest_zpcode=(y[11]),rest_countryid = y[12],loc_verbose=y[13],agg_rat =y[14],rating_text=y[15],rating_color=y[16],votes=y[17],rt=y[18]))
					df_rest = sqlContext.createDataFrame(rdd_rest)
					

					for i in df_rest.select('rest_id','rest_zpcode','rest_lat','rest_long').collect():
						lc = lc + str(zipcode) + ','+ str(i['rest_id'])+ ','+ str(i['rest_zpcode']) + ','+ str(i['rest_lat'])+ ','+ str(i['rest_long'])    + '|'
						
				
				#r print lc
				return lc
		#except Exception as e:
			#logging.info('error in zomato location function  %s' %str(e))




	def reviews_data(self,sc,cl,sqlContext,zip_code,api_link,j,apikey,comp_date,lt):
		#try:
			logging.info('entered into reviews data function')
			print api_link+'res_id='+str(j)+'&apikey='+str(apikey)
			reviews = json.loads(requests.get(api_link+'res_id='+str(j)+'&apikey='+str(apikey)).text);rd = []
			if 'user_reviews' in reviews:
				if len(reviews['user_reviews']) > 0:
					for k in reviews['user_reviews']:
						rev = str()
						if 'review' in k:
							#print type(datetime.utcfromtimestamp(k['review']['timestamp']));
							#print type(comp_date);
							print comp_date.replace(tzinfo=pytz.UTC);print datetime.utcfromtimestamp(k['review']['timestamp']).replace(tzinfo=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S');
							if datetime.utcfromtimestamp(k['review']['timestamp']).replace(tzinfo=pytz.UTC) > (comp_date).replace(tzinfo=pytz.UTC):
									rev = rev + str(j) + '|'
									if 'rating' in k['review']: rev = rev + str(k['review']['rating']) + '|'
									else: rev = rev + 'unknown' + '|'
									if 'id' in k['review']:rev = rev + str(k['review']['id']) + '|'
									else: rev = rev + 'unknown' + '|'
									if 'review_text' in k['review']: 
										rev = rev + (k['review']['review_text']).replace('\r','').replace('\n','').encode('utf-8').strip() + '|'
									else: rev = rev + 'unknown' + '|'
									if 'rating_color' in k['review']: rev = rev + (k['review']['rating_color']).encode('utf-8').strip() + '|'
									else: rev = rev + 'unknown' + '|'
									if 'rating_time_friendly' in k['review']: rev = rev + (k['review']['rating_time_friendly']) + '|'
									else: rev = rev + 'unknown' + '|'
									if 'rating_text' in k['review']: rev = rev + (k['review']['rating_text']).replace('\r','').replace('\n','').encode('utf-8').strip() + '|'
									else: rev = rev + 'unknown' + '|'
									if 'timestamp' in k['review']: rev = rev + (datetime.utcfromtimestamp(k['review']['timestamp']).replace(tzinfo=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S'))  + '|'
									else: rev = rev + 'unknown' + '|'
									if 'likes' in k['review']: rev = rev + str(k['review']['likes']) + '|'
									else: rev = rev + 'unknown' + '|'
									if 'comments_count' in k['review']: rev = rev + str(k['review']['comments_count']) + '|'
									else: rev = rev + 'unknown' + '|'
									if 'user' in k['review']:
										if 'name' in k['review']['user']: rev = rev + (k['review']['user']['name']).encode('utf-8').strip() + '|'
										else: rev = rev + 'unknown' + '|'
										if 'zomato_handle' in k['review']['user']: rev = rev + (k['review']['user']['zomato_handle']).encode('utf-8').strip() + '|'
										else: rev = rev + 'unknown' + '|'
										if 'foodie_level' in k['review']['user']: rev = rev + (k['review']['user']['foodie_level']).encode('utf-8').strip()+ '|'
										else: rev = rev + 'unknown' + '|'
										if 'foodie_level_num' in k['review']['user']: rev = rev + str(k['review']['user']['foodie_level_num']).strip() + '|'
										else: rev = rev + 'unknown' + '|'
										if 'foodie_color' in k['review']['user']: rev = rev + (k['review']['user']['foodie_color']).encode('utf-8').strip() + '|'
										else: rev = rev + 'unknown' + '|'
										if 'profile_url' in k['review']['user']: rev = rev + (k['review']['user']['profile_url']).encode('utf-8').strip() + '|'
										else: rev = rev + 'unknown' + '|'
										if 'profile_image' in k['review']['user']: rev = rev + (k['review']['user']['profile_image']).encode('utf-8').strip() + '|'
										else: rev = rev + 'unknown' + '|'
									rev = rev + str(spark_conf.retrieved_time.replace(':','-'))
									#f = open(spark_conf.file_path['reviewdata_path']+'reviews_%s.txt'% spark_conf.retrieved_time.replace(':','-'),'a')
									#f.write('%s\n' %rev);f.close()	
									rd.append(rev)

		
			print rd
			print len(rd)
			lt = lt + str(spark_conf.utctime.replace(':','-'))  +'|'+ str(j) +'|'+ str(len(rd)) + '|' + '1'
			print lt
			sc.parallelize([lt.strip()]).saveAsTextFile(spark_conf.hdfs_path['zip_table']+'%s_%s_%s.txt' %(zip_code,j,spark_conf.utc_time()[1]))


			if len(rd) > 0:
				
				if str(sc.parallelize(rd).isEmpty()) == 'False':
					rdd_r = sc.parallelize(rd).filter(lambda y: y != '').map(lambda x: x.split('|')).map(lambda x: str(x[0])+'|'+str(x[1])+'|'+ str(x[2])+'|'+ str(x[3])+'|'+ str(x[4])+'|'+ str(x[5])+'|'+ str(x[6])+'|'+ str(x[7])+'|'+str(x[8])+'|'+ str(x[9])+'|'+ str(x[10])+'|'+ str(x[11])+'|'+ str(x[12])+'|'+ str(x[13])+'|'+ str(x[14])+'|'+ str(x[15]) +'|'+ str(x[16]) +'|'+ str(x[17])).saveAsTextFile(spark_conf.hdfs_path['temp_review_data']+'%s_%s_%s.txt' %(zip_code,j,spark_conf.utc_time()[1]))
					logging.info('completed saving rdd datafiles in hdfs')
					
				else:
					return 'None'
			else:
				return 'None'

				

		#except Exception as e:
			#logging.info('error in reviews_data function  %s' %str(e))	

	def loop(self,sc,cl,l,sqlContext,comp_date,zip_new,latitude,longitude):
		#try:
			logging.info('entered into loop func')		
			for i in l:
				for j in i:
					if j != '':
						if zip_new != '':
							
							print('entered 1st if')
							if int(j.split(',')[0]) == int(zip_new):
								print j;print comp_date;print zip_new
								lt = str();lt = lt + str(j.split(',')[0]) +'|'+ str(j.split(',')[2]).strip()+'|'+ str(latitude)+'|'+ str(longitude) +  '|' +  str(spark_conf.zomato['zomato_reviews']) + '|'			
								print lt
								df_return = self.reviews_data(sc,cl,sqlContext,j.split(',')[0],spark_conf.zomato['zomato_reviews'],j.split(',')[1],spark_conf.zomato['apikey'],comp_date,lt)


						else:
							print zip_new
							print('second else entered')
							zip_new =  j.split(',')[0] 
							if j.split(',')[0] == zip_new:
								print j;print comp_date
								lt = str();lt = lt + str(j.split(',')[0]) +'|'+ str(j.split(',')[2]).strip()+'|'+ str(latitude)+'|'+ str(longitude) +  '|' +  str(spark_conf.zomato['zomato_reviews']) + '|'			
								zip_new = str()
								print lt
								df_return = self.reviews_data(sc,cl,sqlContext,j.split(',')[0],spark_conf.zomato['zomato_reviews'],j.split(',')[1],spark_conf.zomato['apikey'],comp_date,lt)




			## data_r.rdd.map(lambda x : '\t'.join(str(d.encode('utf-8')) for d in x)).saveAsTextFile('/bdaas/testing/test_new')
			r = call(['hdfs','dfs','-test','-f',spark_conf.hdfs_path['temp_review_data']+'*/p*'])
			if r == 0:
				r_path = spark_conf.hdfs_path['temp_review_data']+'*'
				print r_path
				rdd_data = sc.textFile(r_path, use_unicode=False)
				if rdd_data.count() > 0:
					print rdd_data.count()
					rdd_r= rdd_data.filter(lambda y: y != '').map(lambda x: x.split('|')).map(lambda z: Row(restaurant_id=z[0], rating = z[1], review_id = z[2],review_text = z[3],rating_color = z[4],rating_time_friendly=z[5],rating_text=z[6],time_stamp=z[7],likes=z[8],comment_count =z[9],user_name = z[10],user_zomatohandle=z[11],user_foodie_level = z[12],user_level_num=z[13],foodie_color=z[14],profile_url=z[15],profile_image=z[16],retrieved_time=z[17]))
					data_r = sqlContext.createDataFrame(rdd_r)
					#print data_r.dropDuplicates(['review_id']).count()
					data_new = data_r.dropDuplicates(['review_id'])
					n = cl.nlc(data_new)
					if n == 1:
						a,b = cl.confident_classifier(sqlContext,data_new)
						if b == 1 : 
							c,d = cl.binary_encoding(a)
							if d == 1: 
								f = cl.linear_regression(c,sqlContext,a)
								if f == 1:
									logging.info('moving data from temp review to review data folder')
									call(['hdfs','dfs','-mv',spark_conf.hdfs_path['temp_review_data']+'*',spark_conf.hdfs_path['review_data']])




		#except Exception as e:	
		#	logging.info('error in loop function  %s' %str(e))






	def input_date(self,days):
			try:
				logging.info('entered into zomato input date func')
				ZERO = timedelta(0)
				class UTC(tzinfo):
				  def utcoffset(self, dt):
				    return ZERO
				  def tzname(self, dt):
				    return "UTC"
				  def dst(self, dt):
				    return ZERO
				utc = UTC()
				pre_date = datetime.now(utc) -timedelta(days= int(days))
				logging.info('input date '+str(pre_date)+' '+str(type(pre_date)))

				return pre_date
			except Exception as e:
				logging.info('error in update_date function  %s' %str(e))














####
	




	