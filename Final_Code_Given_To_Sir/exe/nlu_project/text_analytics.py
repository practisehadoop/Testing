
from alchemyapi import AlchemyAPI
from watson_developer_cloud import ToneAnalyzerV3
import json,time
import pandas as pd
from subprocess import Popen

## creating the objects of alchemy api
alchemyapi = AlchemyAPI()
tone_analyzer = ToneAnalyzerV3(
    password='sjpqTLj4QtpH',
    username='7c4a2e3b-2cbd-452e-9550-73100f107d87',
    version='2016-02-11')

cur_time = str(int(time.time()))
output_path = '/bdaas/src/nlu/'
texts_filepath = output_path+'texts/texts.txt'
entities_filepath = output_path+'entities/entities'+cur_time+'.csv'
keywords_filepath = output_path+'keywords/keywords'+cur_time+'.csv'
taxonomies_filepath = output_path+'taxonomies/taxonomies'+cur_time+'.csv'
sentiments_filepath = output_path+'sentiments/sentiments'+cur_time+'.csv'
relations_filepath = output_path+'relations/relations'+cur_time+'.csv'
concepts_filepath = output_path+'concepts/concepts'+cur_time+'.csv'
document_tone_filepath = output_path+'text_document_tone/text_document_tone'+cur_time+'.csv'
sentences_tone_filepath  = output_path+'text_sentences_tone/text_sentences_tone'+cur_time+'.csv'

entities_lst = []; keywords_lst = []; concpets_lst = []
relations_lst = []; taxonomy_lst = []; sentiment_lst = []
texts_lst = []; document_tone_lst = []; sentences_tone_lst = []
#api_key = '9c9936fbe2479e3341ff47bbaf21bea166b866f1'


def text_ana_entities(demo_text,count):
    response = alchemyapi.entities('text', demo_text, {'sentiment': 1})
    if response['status'] == 'OK':
        for entity in response['entities']:
            try: text = entity['text']
            except: text = 'unknown'
            try:type1 = entity['type']
            except: type1 = 'unknown'
            try:relavance = entity['relevance']
            except: relavance = 'unknown'
            try: sentiment = entity['sentiment']['type']
            except: sentiment = 'unknown'
            row = (str(count),text ,type1 ,relavance ,sentiment)
            entities_lst.append(row)


def text_ana_keywords(demo_text,count):
    response = alchemyapi.keywords('text', demo_text, {'sentiment': 1})
    if response['status'] == 'OK':
        for entity in response['keywords']:
            text = entity['text']
            relavance = entity['relevance']
            sentiment = entity['sentiment']['type']
            if 'score' in entity['sentiment']:
                score = entity['sentiment']['score']
            else: score = 0.0
            row = (str(count) ,text ,relavance ,sentiment,score)
            keywords_lst.append(row)


def text_ana_concepts(demo_text,count):
    response = alchemyapi.concepts('text', demo_text, {'showSourceText': 1})
    if response['status'] == 'OK':
        for concept in response['concepts']:
            text = concept['text']
            relavance = concept['relevance']
            row = (str(count),text ,relavance)
            concpets_lst.append(row)


def text_ana_relations(demo_text,count):
    response = alchemyapi.relations('text', demo_text, {'showSourceText': 1, 'sentiment' :1, 'keywords' :1, 'entities' :1,
                                                        'requireEntities' :1 , 'sentimentExcludeEntities' :1, 'disambiguate' :1,
                                                        'linkedData' :1, 'coreference' :1})
    if response['status'] == 'OK':
        for relation in response['relations']:
            text = relation['sentence']
            action = 'unknown'
            if 'action' in relation:
                action = relation['action']['text']
            subject = 'unknown'
            if 'subject' in relation:
                subject = relation['subject']['text']
            obj = 'unknown'
            if 'object' in relation:
                obj = relation['object']['text']
            sentiment = 'unknown'
            score = 0.0
            loc_city = 'unknown'
            loc_state = 'unknown'
            if 'location' in relation:
                loc = relation['location']
                if 'sentiment' in loc:
                    sentiment = loc['sentiment']['type']
                    score = loc['sentiment']['score']
                if 'entities' in loc:
                    for entity in loc['entities']:
                        if 'City' in entity['type']:
                            loc_city = entity['text']
                        if 'StateOrCounty' in entity['type']:
                            loc_state = entity['text']
            row = (str(count) ,text ,subject ,action ,obj ,sentiment ,score ,loc_city ,loc_state)
            relations_lst.append(row)


def text_ana_taxonomy(demo_text,count):
    response = alchemyapi.taxonomy('text', demo_text, {'showSourceText': 1})
    if response['status'] == 'OK':
        for category in response['taxonomy']:
            text = category['label']
            score = category['score']
            row = (str(count) ,text ,score)
            taxonomy_lst.append(row)


def text_ana_sentiment(demo_text,count):
    response = alchemyapi.sentiment('text', demo_text, {'showSourceText': 1})
    if response['status'] == 'OK':
        text = response['text']
        sentiment = response['docSentiment']['type']
        score = 0.0
        if 'score' in response['docSentiment']:
            score = response['docSentiment']['score']
        row = (str(count),sentiment,score)
        #print row
        sentiment_lst.append(row)

def document_tone(data,text_id):
    if 'document_tone' in data:
        doc_tone = json.loads(data)['document_tone'] #dict
        for categories in doc_tone['tone_categories']:
            doc_dict = dict()
            doc_dict['text_id'] = text_id
            doc_dict['tone_category'] = categories['category_id']
            scores = list()
            for tone in categories['tones']:scores.append(tone['score'])
            for tone in categories['tones']:
                if tone['score'] == max(scores) and max(scores) != 0:
                    doc_dict['tone_name'] = tone['tone_name']
                    doc_dict['tone_id'] = tone['tone_id']
                    doc_dict['score'] = tone['score']
                    document_tone_lst.append(doc_dict)

def sentence_tone(data,text_id):
    if 'sentences_tone' in data:
        sen_tone = json.loads(data)['sentences_tone'] #list
        for sen_text in sen_tone:
            sen_dict = dict()
            #sen_dict['input_to'] = sen_text['input_to']
            for categories in sen_text['tone_categories']:
                sen_dict['text_id'] = text_id
                sen_dict['text_part'] = sen_text['text']
                sen_dict['tone_category'] = categories['category_id']
                scores = list()
                for tone in categories['tones']: scores.append(tone['score'])
                for tone in categories['tones']:
                    if tone['score'] == max(scores) and max(scores) != 0 :
                        sen_dict['tone_name'] = tone['tone_name']
                        sen_dict['tone_id'] = tone['tone_id']
                        sen_dict['score'] = tone['score']
                        sentences_tone_lst.append(sen_dict)
                        sen_dict = dict()



## reading texts from the csv file from 'source_csv_filepath' and loading into list
fh = open(texts_filepath).readlines()
for line in fh:
    line.strip()
    texts_lst.append(line.split('|')[1].strip())

texts_lst = texts_lst[:]

## getting getting entities data
for count,text in enumerate(texts_lst):
    text_ana_entities(text.replace(r'\n',' ').strip(),count)
entities_df = pd.DataFrame(entities_lst, columns = ['text_id' ,'entities_text' ,'entities_type1', 'entities_relavance' ,'entities_sentiment'])
entities_df.to_csv(entities_filepath, sep='|', encoding='utf-8',index=False,mode='w',header=False)
entities_lst = [] #empty the list after writing the data to file

## getting keywords data
for count,text in enumerate(texts_lst):
    text_ana_keywords(text.replace(r'\n',' ').strip(),count) # args text = doc_text && count = rec_num
keywords_df = pd.DataFrame(keywords_lst, columns = ['text_id' ,'key_word_text', 'key_word_relavance' ,'key_word_sentiment', 'key_word_score'])
keywords_df.to_csv(keywords_filepath, sep='|', encoding='utf-8',index=False,mode='w',header=False)
keywords_lst = [] #empty the list after writing the data to file


## getting concepts data
for count,text in enumerate(texts_lst):
    text_ana_concepts(text.replace(r'\n',' ').strip(),count) # args text = doc_text && count = rec_num
concepts_df = pd.DataFrame(concpets_lst, columns = ['text_id' ,'concepts_text', 'concepts_relavance'])
concepts_df.to_csv(concepts_filepath, sep='|', encoding='utf-8',index=False,mode='w',header=False)
concpets_lst = [] #empty the list after writing the data to file

## getting relations data
for count,text in enumerate(texts_lst):
    text_ana_relations(text.replace(r'\n',' ').strip(),count) # args text = doc_text && count = rec_num
relations_df = pd.DataFrame(relations_lst, columns = ['text_id','realations_text', 'realations_subject', 'realations_action', 'realations_obj', 'realations_sentiment', 'realations_score', 'realations_loc_city', 'realations_loc_state'])
relations_df.to_csv(relations_filepath, sep='|', encoding='utf-8',index=False,mode='w',header=False)
relations_lst = [] #empty the list after writing the data to file

## getting taxonomy data
for count,text in enumerate(texts_lst):
    text_ana_taxonomy(text.replace(r'\n',' ').strip(),count) # args text = doc_text && count = rec_num
taxonomy_df = pd.DataFrame(taxonomy_lst, columns = ['text_id' ,'taxonomy_text' ,'taxonomy_score'])
taxonomy_df.to_csv(taxonomies_filepath, sep='|', encoding='utf-8',index=False,mode='w',header=False)
taxonomy_lst = [] #empty the list after writing the data to file

## getting sentiment data
for count,text in enumerate(texts_lst):
    text_ana_sentiment(text.replace(r'\n',' ').strip(),count) # args text = doc_text && count = rec_num
sentiment_df = pd.DataFrame(sentiment_lst, columns = ['text_id','sentiment_sentiment','sentiment_score'])
sentiment_df.to_csv(sentiments_filepath, sep='|', encoding='utf-8',index=False,mode='w',header=False)
sentiment_lst = [] #empty the list after writing the data to file

## getting tone analysis
for count, text in enumerate(texts_lst):
    try:data = json.dumps(tone_analyzer.tone(text=text))
    except:continue
    document_tone(data, count)
    sentence_tone(data, count)
doc_df = pd.DataFrame(document_tone_lst)
doc_df.to_csv(document_tone_filepath, sep='|', encoding='utf-8', index=False, mode='w',header=False)
document_tone_lst = []  # empty the list after writing the data to file
sens_df = pd.DataFrame(sentences_tone_lst)
sens_df.to_csv(sentences_tone_filepath, sep='|', encoding='utf-8', index=False, mode='w',header=False)
sentences_tone_lst = []  # empty the list after writing the data to file

