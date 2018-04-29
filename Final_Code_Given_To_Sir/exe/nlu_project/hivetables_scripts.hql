
drop table if exists text_sentences_tone;
Create external table if not exists text_sentences_tone(
score string,
text_id string,
text_path string,
tone_category string,
tone_itd string,
tone_name string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
location 's3a://datamorphixdata/tmp/ph_reviews/text_sentences_tone';



drop table if exists taxonomies;
Create external table if not exists taxonomies(
text_id string,
taxonomy_text string,
taxonomy_score string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
location 's3a://datamorphixdata/tmp/ph_reviews/taxonomies';


drop table if exists text_document_tone;
Create external table if not exists text_document_tone (
score string,
text_id string,
tone_category string,
tone_id string,
tone_name string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
location 's3a://datamorphixdata/tmp/ph_reviews/text_document_tone';



drop table if exists texts;
create external table if not exists texts(
text_id string,
text string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '||'
LINES TERMINATED BY '\n'
location 's3a://datamorphixdata/tmp/ph_reviews/texts';



drop table if exists concepts;
create external table if not exists concepts
( text_id string,
concepts_text string,
concepts_relavance string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
Location 's3a://datamorphixdata/tmp/ph_reviews/concepts';



drop table if exists entities;
create external table if not exists entities
( text_id string,
entities_text string,
entities_type1 string,
entities_relavance string,
entities_sentiment string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
Location 's3a://datamorphixdata/tmp/ph_reviews/entities';



drop table if exists keywords;
create external table if not exists keywords
( text_id string,
key_word_text string,
key_word_relavance string,
key_word_sentiment string,
key_word_score string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
Location 's3a://datamorphixdata/tmp/ph_reviews/keywords';


drop table if exists relations;
create external table if not exists relations 
( text_id string,
relations_text string,
relations_subject string,
relations_action string,
relations_obj string,
relations_sentiment string,
relations_score string,
relation_loc_city string,
relations_loc_state string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
Location 's3a://datamorphixdata/tmp/ph_reviews/relations';


drop table if exists sentiments;
create external table if not exists sentiments
( text_id string,
sentiment_sentiment string,
sentiment_score string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
Location 's3a://datamorphixdata/tmp/ph_reviews/text_sentences_tone';


