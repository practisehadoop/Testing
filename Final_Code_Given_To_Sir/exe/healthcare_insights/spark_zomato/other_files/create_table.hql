create database if not exists sample_hl7db;
use sample_hl7db; 
create external table if not exists restaurant_review_new(restaurant_id string, rating string,review_id string,review_text string,rating_color string,review_time_friendly string,rating_text string,time_stamp string,likes string,comment_count string,user_name string,user_zomatohandle string,user_foodie_level string,user_level_num string,foodie_color string, profile_url string,profile_image  string,retrieved_time string)
row format delimited
fields terminated by '|'
Location '/bdaas/src/zomato/review_data/final/';


create external table if not exists restaurant_data_new (apikey string,restaurant_id string, restaurant_name string,restaurant_url string,utm_source string,res_location string, res_locality string,res_city string,res_cityid string,res_latitude string,res_longitude string,res_zipcode string,country_id string,locality_verbose string,aggregate_rating string,res_rating_text string,rating_color string, votes string, retrieved_time string)
row format delimited
fields terminated by '|'
Location '/bdaas/src/zomato/restaurant_data/';

create external table if not exists zomato_log(
quered_zipcode string,
original_zipcode string,
lat string,
long string,
sm_name string,
retrieving_time string,
restaurant_id string,
count_val string,
check_value string
)
row format delimited
fields terminated by '|'
Location '/bdaas/src/zomato/zip_date/';


create external table if not exists dm_insights(
comment_count string,
foodie_color string,
likes string,
profile_image string,
profile_url string,
rating string,
rating_color string,
rating_text string,
rating_time_friendly string,
restaurant_id int,
retrieved_time string,
review_id int,
review_text string,
time_stamp string,
user_foodie_level string,
user_level_num string,
user_name string,
user_zomatohandle string,
class_name string,
confidence string,
score string

)
row format delimited
fields terminated by '|'
Location '/bdaas/src/zomato/classifier_output/';


create table if not exists restaurants_insights_new  as select 
rd.restaurant_id,
cast(r.review_id as int) as review_id,
r.review_text,
r.class_name as class_name,
r.confidence,
r.score,
cast(from_unixtime(unix_timestamp(rr.time_stamp,'yyyy-MM-dd HH:mm:ss')) as timestamp) as review_time,
cast(cast(from_unixtime(unix_timestamp(SUBSTR(r.retrieved_time,1,10),'yyyy-MM-dd')) as timestamp) as date) as retrieved_time ,
rd.apikey,
rd.restaurant_name,
rd.restaurant_url,
rd.utm_source ,
rd.res_location ,
rd.res_locality,
rd.res_city ,
rd.res_cityid ,
rd.res_latitude ,
rd.res_longitude ,
cast(rd.res_zipcode as int) as zipcode,
rd.country_id ,
rd.locality_verbose ,
cast(rd.aggregate_rating as float) as aggregate_rating,
rd.res_rating_text ,
rd.rating_color ,
rd.votes
from restaurant_review_new rr JOIN dm_insights r ON (rr.review_id == r.review_id) JOIN restaurant_data_new rd ON (rr.restaurant_id == rd.restaurant_id);









