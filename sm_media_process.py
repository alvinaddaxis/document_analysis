from google.cloud import bigquery_storage
from google.cloud.bigquery_storage import types
from google.cloud import bigquery
import pandas
import pandas as pd
import json
import datetime
from pytz import timezone
from google.cloud import aiplatform
from google.cloud.aiplatform.gapic.schema import predict
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value
from google.protobuf.json_format import MessageToDict
from google.cloud import language
#from google.cloud import language_v1
import pytz
import six
import sys
from statistics import mode
from statistics import mean
from time import sleep
from google.cloud import translate_v2
import dateutil.parser
import os, glob
import os.path
from os import path
import csv



def predict_text_classification_single_label(
    project: str,
    endpoint_id: str,
    content: str,
    location: str = "us-central1",
    api_endpoint: str = "us-central1-aiplatform.googleapis.com",
):
    # The AI Platform services require regional API endpoints.
    client_options = {"api_endpoint": api_endpoint}
    # Initialize client that will be used to create and send requests.
    # This client only needs to be created once, and can be reused for multiple requests.
    client = aiplatform.gapic.PredictionServiceClient(client_options=client_options)
    instance = predict.instance.TextClassificationPredictionInstance(
        content=content,
    ).to_value()
    instances = [instance]
    parameters_dict = {}
    parameters = json_format.ParseDict(parameters_dict, Value())
    endpoint = client.endpoint_path(
        project=project, location=location, endpoint=endpoint_id
    )
    response = client.predict(
        endpoint=endpoint, instances=instances, parameters=parameters
    )
    print("response")
    print(" deployed_model_id:", response.deployed_model_id)

    predictions = response.predictions
    #for prediction in predictions:
        #print(" prediction:", dict(prediction))
    return response

def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)
def test():
    your_project_id = "test-stuff-292200"
    bqstorageclient = bigquery_storage.BigQueryReadClient()

    project_id = "sightgraphv2-dev"
    dataset_id = "sightgraph_dataset"
    table_id = "analysis_results"
    table = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

# Select columns to read with read options. If no read options are
# specified, the whole table is read.
    read_options = types.ReadSession.TableReadOptions(
        selected_fields=["uuid"]
    )

    parent = "projects/{}".format(your_project_id)

    requested_session = types.ReadSession(
        table=table,
    # Avro is also supported, but the Arrow data format is optimized to
    # work well with column-oriented data structures such as pandas
    # DataFrames.
        data_format=types.DataFormat.ARROW,
        read_options=read_options,
    )
    read_session = bqstorageclient.create_read_session(
        parent=parent, read_session=requested_session, max_stream_count=1,
    )

# This example reads from only a single stream. Read from multiple streams
# to fetch data faster. Note that the session may not contain any streams
# if there are no rows to read.
    stream = read_session.streams[0]
    reader = bqstorageclient.read_rows(stream.name)

# Parse all Arrow blocks and create a dataframe.
    frames = []
    for message in reader.rows().pages:
        frames.append(message.to_dataframe())
    dataframe = pandas.concat(frames)
    print(dataframe.head())
    

    
def get_class_df(df, local_time, checkrange, time_from, time_to):
    print('enter_class')
    new_df = {'account_name':[], 'source':[],'post_date':[],'post_id':[], 'post':[], 'score':[], 'magnitude':[], 'likes':[], 'comments':[], 'retweets/shares':[],'engagement':[], 'post_url':[], "non_temporal_label":[], "non_temporal_label_confidence":[], "temporal_label":[], "temporal_label_confidence":[]}
    df_nontemp = {"account_name":[], "source":[], "post_date":[], "post_id":[], "post":[], "Culture":[], "Access_to_information":[], "Hypermasculinity":[], "Lived_experience":[], "Training":[], "Bullying":[], "Tribalism":[]}
    df_temp = {"account_name":[], "source":[], "post_date":[], "post_id":[], "post":[], "Transition":[], "Service_deployment":[], "Separation":[], "Post_service_issues":[], "Service_training":[], "Pre_service":[]}
    #df = pd.read_csv('sentiments.csv')
    #print(df)
    for index, row in df.iterrows():
        print(row['sentence'])
        temp_label = ''
        nontemp_label =''
        x = predict_text_classification_single_label(project="test-stuff-292200",endpoint_id="5404926483263127552",location="us-central1",content=row['sentence'])
        y = predict_text_classification_single_label(project="test-stuff-292200",endpoint_id="6570690283682398208",location="us-central1",content=row['sentence'])
        temporal_classification_result = MessageToDict(x.__class__.pb(x))
        nontemporal_classification_result = MessageToDict(y.__class__.pb(y))
        max_temp_conf = max(temporal_classification_result['predictions'][0]['confidences'])
        temp_numtype = temporal_classification_result['predictions'][0]['confidences'].index(max_temp_conf)
        max_nontemp_conf = max(nontemporal_classification_result['predictions'][0]['confidences'])
        nontemp_numtype = nontemporal_classification_result['predictions'][0]['confidences'].index(max_nontemp_conf)
        if(temp_numtype==0):
            temp_label = 'Transition'
        elif(temp_numtype==1):
            temp_label = 'Service_deployment'
        elif(temp_numtype==2):
            temp_label = 'Separation'
        elif(temp_numtype==3):
            temp_label = 'Post_service_issues'
        elif(temp_numtype==4):
            temp_label = 'Service_training'
        elif(temp_numtype==5):
            temp_label = 'Pre_service'
        
        if(nontemp_numtype==0):
            nontemp_label = 'Culture'
        elif(nontemp_numtype==1):
            nontemp_label = 'Access_to_information'
        elif(nontemp_numtype==2):
            nontemp_label = 'Hypermasculinity'
        elif(nontemp_numtype==3):
            nontemp_label = 'Lived_experience'
        elif(nontemp_numtype==4):
            nontemp_label = 'Training'
        elif(nontemp_numtype==5):
            nontemp_label = 'Bullying'
        elif(nontemp_numtype==6):
            nontemp_label = 'Tribalism'
        new_df['account_name'].append(row['account_name'])
        new_df['source'].append(row['source'])
        new_df['post_date'].append(row['post_date'])
        new_df['post_id'].append(row['post_id'])
        new_df['post'].append(row['sentence'])
        new_df['score'].append(row['score'])
        new_df['magnitude'].append(row['magnitude'])
        new_df['non_temporal_label'].append(nontemp_label)
        new_df['non_temporal_label_confidence'].append(max_nontemp_conf)
        new_df['temporal_label'].append(temp_label)
        new_df['temporal_label_confidence'].append(max_temp_conf)
        new_df['likes'].append(row['likes'])
        new_df['comments'].append(row['comments'])
        new_df['retweets/shares'].append(row['retweets/shares'])
        new_df['engagement'].append(row['engagement'])
        new_df['post_url'].append(row['post_url'])
        df_nontemp['account_name'].append(row['account_name'])
        df_nontemp['source'].append(row['source'])
        df_nontemp['post_date'].append(row['post_date'])
        df_nontemp['post_id'].append(row['post_id'])
        df_nontemp['post'].append(row['sentence'])
        df_nontemp['Culture'].append(nontemporal_classification_result['predictions'][0]['confidences'][0])
        df_nontemp['Access_to_information'].append(nontemporal_classification_result['predictions'][0]['confidences'][1])
        df_nontemp['Hypermasculinity'].append(nontemporal_classification_result['predictions'][0]['confidences'][2])
        df_nontemp['Lived_experience'].append(nontemporal_classification_result['predictions'][0]['confidences'][3])
        df_nontemp['Training'].append(nontemporal_classification_result['predictions'][0]['confidences'][4])
        df_nontemp['Bullying'].append(nontemporal_classification_result['predictions'][0]['confidences'][5])
        df_nontemp['Tribalism'].append(nontemporal_classification_result['predictions'][0]['confidences'][6])
        df_temp['account_name'].append(row['account_name'])
        df_temp['source'].append(row['source'])
        df_temp['post_date'].append(row['post_date'])
        df_temp['post_id'].append(row['post_id'])
        df_temp['post'].append(row['sentence'])
        df_temp['Transition'].append(temporal_classification_result['predictions'][0]['confidences'][0])
        df_temp['Service_deployment'].append(temporal_classification_result['predictions'][0]['confidences'][1])
        df_temp['Separation'].append(temporal_classification_result['predictions'][0]['confidences'][2])
        df_temp['Post_service_issues'].append(temporal_classification_result['predictions'][0]['confidences'][3])
        df_temp['Service_training'].append(temporal_classification_result['predictions'][0]['confidences'][4])
        df_temp['Pre_service'].append(temporal_classification_result['predictions'][0]['confidences'][5])
        
        
    #print(df_nontemp)
    new_df = pd.DataFrame(data=new_df)
    df_nontemp = pd.DataFrame(data=df_nontemp)
    df_temp = pd.DataFrame(data=df_temp)
    
    if checkrange == True:
        new_df.to_csv("/home/jupyter/influencer_outputs/influencer_posts_with_labels_{localtime}_{timefrom}_{timeto}.csv".format(localtime=local_time, timefrom = time_from, timeto = time_to), index=False)
        df_nontemp.to_csv("/home/jupyter/influencer_outputs/influencer_non_temporal_labels_{localtime}_{timefrom}_{timeto}.csv".format(localtime=local_time, timefrom = time_from, timeto = time_to), index=False)
        df_temp.to_csv("/home/jupyter/influencer_outputs/influencer_temporal_labels_{localtime}_{timefrom}_{timeto}.csv".format(localtime=local_time, timefrom = time_from, timeto = time_to), index=False)
    else:
        new_df.to_csv("/home/jupyter/influencer_outputs/influencer_posts_with_labels_{localtime}_all.csv".format(localtime=local_time), index=False)
        df_nontemp.to_csv("/home/jupyter/influencer_outputs/influencer_non_temporal_labels_{localtime}_all.csv".format(localtime=local_time), index=False)
        df_temp.to_csv("/home/jupyter/influencer_outputs/influencer_temporal_labels_{localtime}_all.csv".format(localtime=local_time), index=False)
    return new_df
    
def get_class_csv(csv, local_time, checkrange, time_from, time_to):
    
    print('enter_class')
    new_df = {'account_name':[], 'source':[],'post_date':[],'post_id':[], 'post':[], 'score':[], 'magnitude':[], 'likes':[], 'comments':[], 'retweets/shares':[],'engagement':[], 'post_url':[], "non_temporal_label":[], "non_temporal_label_confidence":[], "temporal_label":[], "temporal_label_confidence":[]}
    df_nontemp = {"account_name":[], "source":[], "post_date":[], "post_id":[], "post":[], "Culture":[], "Access_to_information":[], "Hypermasculinity":[], "Lived_experience":[], "Training":[], "Bullying":[], "Tribalism":[]}
    df_temp = {"account_name":[], "source":[], "post_date":[], "post_id":[], "post":[], "Transition":[], "Service_deployment":[], "Separation":[], "Post_service_issues":[], "Service_training":[], "Pre_service":[]}
    df = pd.read_csv(csv)
    #print(df)
    for index, row in df.iterrows():
        print(row['sentence'])
        temp_label = ''
        nontemp_label =''
        x = predict_text_classification_single_label(project="test-stuff-292200",endpoint_id="5404926483263127552",location="us-central1",content=row['sentence'])
        y = predict_text_classification_single_label(project="test-stuff-292200",endpoint_id="6570690283682398208",location="us-central1",content=row['sentence'])
        temporal_classification_result = MessageToDict(x.__class__.pb(x))
        nontemporal_classification_result = MessageToDict(y.__class__.pb(y))
        max_temp_conf = max(temporal_classification_result['predictions'][0]['confidences'])
        temp_numtype = temporal_classification_result['predictions'][0]['confidences'].index(max_temp_conf)
        max_nontemp_conf = max(nontemporal_classification_result['predictions'][0]['confidences'])
        nontemp_numtype = nontemporal_classification_result['predictions'][0]['confidences'].index(max_nontemp_conf)
        if(temp_numtype==0):
            temp_label = 'Transition'
        elif(temp_numtype==1):
            temp_label = 'Service_deployment'
        elif(temp_numtype==2):
            temp_label = 'Separation'
        elif(temp_numtype==3):
            temp_label = 'Post_service_issues'
        elif(temp_numtype==4):
            temp_label = 'Service_training'
        elif(temp_numtype==5):
            temp_label = 'Pre_service'
        
        if(nontemp_numtype==0):
            nontemp_label = 'Culture'
        elif(nontemp_numtype==1):
            nontemp_label = 'Access_to_information'
        elif(nontemp_numtype==2):
            nontemp_label = 'Hypermasculinity'
        elif(nontemp_numtype==3):
            nontemp_label = 'Lived_experience'
        elif(nontemp_numtype==4):
            nontemp_label = 'Training'
        elif(nontemp_numtype==5):
            nontemp_label = 'Bullying'
        elif(nontemp_numtype==6):
            nontemp_label = 'Tribalism'
        new_df['account_name'].append(row['account_name'])
        new_df['source'].append(row['source'])
        new_df['post_date'].append(row['post_date'])
        new_df['post_id'].append(row['post_id'])
        new_df['post'].append(row['sentence'])
        new_df['score'].append(row['score'])
        new_df['magnitude'].append(row['magnitude'])
        new_df['non_temporal_label'].append(nontemp_label)
        new_df['non_temporal_label_confidence'].append(max_nontemp_conf)
        new_df['temporal_label'].append(temp_label)
        new_df['temporal_label_confidence'].append(max_temp_conf)
        new_df['likes'].append(row['likes'])
        new_df['comments'].append(row['comments'])
        new_df['retweets/shares'].append(row['retweets/shares'])
        new_df['engagement'].append(row['engagement'])
        new_df['post_url'].append(row['post_url'])
        df_nontemp['account_name'].append(row['account_name'])
        df_nontemp['source'].append(row['source'])
        df_nontemp['post_date'].append(row['post_date'])
        df_nontemp['post_id'].append(row['post_id'])
        df_nontemp['post'].append(row['sentence'])
        df_nontemp['Culture'].append(nontemporal_classification_result['predictions'][0]['confidences'][0])
        df_nontemp['Access_to_information'].append(nontemporal_classification_result['predictions'][0]['confidences'][1])
        df_nontemp['Hypermasculinity'].append(nontemporal_classification_result['predictions'][0]['confidences'][2])
        df_nontemp['Lived_experience'].append(nontemporal_classification_result['predictions'][0]['confidences'][3])
        df_nontemp['Training'].append(nontemporal_classification_result['predictions'][0]['confidences'][4])
        df_nontemp['Bullying'].append(nontemporal_classification_result['predictions'][0]['confidences'][5])
        df_nontemp['Tribalism'].append(nontemporal_classification_result['predictions'][0]['confidences'][6])
        df_temp['account_name'].append(row['account_name'])
        df_temp['source'].append(row['source'])
        df_temp['post_date'].append(row['post_date'])
        df_temp['post_id'].append(row['post_id'])
        df_temp['post'].append(row['sentence'])
        df_temp['Transition'].append(temporal_classification_result['predictions'][0]['confidences'][0])
        df_temp['Service_deployment'].append(temporal_classification_result['predictions'][0]['confidences'][1])
        df_temp['Separation'].append(temporal_classification_result['predictions'][0]['confidences'][2])
        df_temp['Post_service_issues'].append(temporal_classification_result['predictions'][0]['confidences'][3])
        df_temp['Service_training'].append(temporal_classification_result['predictions'][0]['confidences'][4])
        df_temp['Pre_service'].append(temporal_classification_result['predictions'][0]['confidences'][5])
        
        
    #print(df_nontemp)
    new_df = pd.DataFrame(data=new_df)
    df_nontemp = pd.DataFrame(data=df_nontemp)
    df_temp = pd.DataFrame(data=df_temp)
    if checkrange == True:
        new_df.to_csv("/home/jupyter/public_outputs/public_posts_with_labels_{localtime}_{timefrom}_{timeto}.csv".format(localtime=local_time, timefrom = time_from, timeto = time_to), index=False)
        df_nontemp.to_csv("/home/jupyter/public_outputs/public_non_temporal_labels_{localtime}_{timefrom}_{timeto}.csv".format(localtime=local_time, timefrom = time_from, timeto = time_to), index=False)
        df_temp.to_csv("/home/jupyter/public_outputs/public_temporal_labels_{localtime}_{timefrom}_{timeto}.csv".format(localtime=local_time, timefrom = time_from, timeto = time_to), index=False)
    else:
        new_df.to_csv("/home/jupyter/public_outputs/public_posts_with_labels_{localtime}_all.csv".format(localtime=local_time), index=False)
        df_nontemp.to_csv("/home/jupyter/public_outputs/public_non_temporal_labels_{localtime}_all.csv".format(localtime=local_time), index=False)
        df_temp.to_csv("/home/jupyter/public_outputs/public_temporal_labels_{localtime}_all.csv".format(localtime=local_time), index=False)
    return new_df
def get_raw_data():
    bqclient = bigquery.Client()

    # Download query results.
    query_string = """
    SELECT * FROM sightgraphv2-prod-rc.sightgraph_dataset.analysis_results
    """

    dataframe = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
        # Optionally, explicitly request to use the BigQuery Storage API. As of
        # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
        # API is used by default.
            create_bqstorage_client=True,
        )
    )
    #print(dataframe.head())
    
    return dataframe

def check_not_exist(data, id_list):
    num = 0
    for i in range(len(id_list)):
        if(data in id_list[i]):
            num += 1
    if(num == 0):
        return True
    else:
        return False
    return False

def utc_to_local(utc_dt):
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Australia/Sydney'))
    return pytz.timezone('Australia/Sydney').normalize(local_dt) # .normalize might be unnecessary
def aslocaltimestr(utc_dt):
    return utc_to_local(utc_dt).strftime('%Y-%m-%d %H:%M:%S')

def analyze_text_sentiment(text):
    client_1 = language.LanguageServiceClient()
    document = language.Document(content=text, type_=language.Document.Type.PLAIN_TEXT)

    response = client_1.analyze_sentiment(document=document)

    sentiment = response.document_sentiment
    #print(sentiment)
    # read the sentiment result to json str
    sentiment_json = sentiment.__class__.to_json(sentiment)
    #replace splaces
    sentiment_string = sentiment_json.replace("\n", "").replace("        ", " ").replace("      ", " ")
    #load json object in dict
    sentiment_result = json.loads(sentiment_string)
    sentiment_result['text']=text #add text into the dictrionary
    
    return sentiment_result

# Entitiy analysis using gcp nlp api
def analyze_text_entities(text):
    client_1 = language.LanguageServiceClient()
    document = language.Document(content=text, type_=language.Document.Type.PLAIN_TEXT)

    response = client_1.analyze_entities(document=document)

    # convert google protobuf format to dic
    entity_result = MessageToDict(response.__class__.pb(response))
    
    return entity_result

def analyze_entity_sentiment(text):
    client_1 = language.LanguageServiceClient()
    document = language.Document(content=text, type_=language.Document.Type.PLAIN_TEXT)
    # get the entities sentiment analysis results
    response = client_1.analyze_entity_sentiment(document=document)
    result_json = response.__class__.to_json(response)
    results = json.loads(result_json)
    return results




def process_df(beforeyesterday,yesterday, weekday, local_time):
    f = open('influencer_filter.json')
    influencer_filter = json.load(f)
    time_from = ''
    time_to = ''
    #if filter_keys in influencer_filter:
    if 'time_from' in influencer_filter:
        time_from = influencer_filter['time_from']
    if 'time_to' in influencer_filter:
        time_to = influencer_filter['time_to']
    if 'filter_keys' in influencer_filter:
        filter_keys = influencer_filter['filter_keys']
    checkrange = False
    if(time_from=='' or time_to == ''):
        checkrange = False
    else:
        checkrange = True
    before_list = [beforeyesterday, yesterday]
    content_list = []
    id_list = [] # no id
    name_list = [] # one user has multiple post_ids
    source_list = [] 
    entity_sentiments_list = [] 
    engage_list = [] #
    #name_date_list = []
    date_list = [] # need
    
    #hashtag
    hashtag_list = []
    hashtag_sentiments_list = []
    hashtag_occ = []
    #file = get_raw_data()
    
    df = pd.read_csv("deduplicated_influencer_posts.csv")
    #path = '/home/jupyter/input_files'
    if path.exists("/home/jupyter/input_files/remove.csv"):
        remove = pd.read_csv("/home/jupyter/input_files/remove.csv")
        remove_ids = []
        posts = []
        for index, row in remove.iterrows():
            if("'" in row['post_id'] and row['post_id'][-1] == "'"):
                remove_id = row['post_id'].replace("'", "")
            else:
                remove_id = row['post_id']
            remove_ids.append(int(remove_id))
        for index, row in df.iterrows():
            if(row['post_id'] in remove_ids):
                df = df.drop(index)
    #df.to_csv('raw_data.csv', index=False)
    #df = pd.read_csv('input.csv')
    dict_data = {"list":[]}
    df_hash_sentiments = {'hashtags':[], 'occurences':[], 'average_sentiments':[], 'post_ids':[]}
    df_entities = {'account_name':[], 'source':[],'post_date':[],'post_id':[], 'entity':[], 'type':[], 'salience':[]}
    df_sentiments = {'account_name':[], 'source':[],'post_date':[],'post_id':[], 'sentence':[], 'score':[], 'magnitude':[], 'likes':[], 'comments':[], 'retweets/shares':[],'engagement':[], 'post_url':[]}
    df_entity_sentiments_occ = {'entity':[], 'type':[], 'salience':[], 'sentiment':[], 'min_sentiment':[],'max_sentiment':[], 'occurence':[], 'post_ids':[]}
    #df_person_sentiments = {'account_name':[], 'source':[],'post_date':[],'post_id':[], 'entity':[], 'type':[], 'salience':[], 'sentiment':[], 'min_sentiment':[],'max_sentiment':[], 'occurence':[]}
    #df_entity_sentiments_positive = {'post_id':[], 'entity':[], 'type':[], 'salience':[], 'sentiment':[], 'min_sentiment':[],'max_sentiment':[], 'occurence':[]}
    #df_entity_sentiments_negative = {'post_id':[], 'entity':[], 'type':[], 'salience':[], 'sentiment':[], 'occurence':[]}
    #df_entity_sentiments_salience = {'post_id':[], 'entity':[], 'type':[], 'salience':[], 'sentiment':[], 'occurence':[]}
    #with open('test2.json', 'w') as file:
        #file.seek(0)
        #json.dump(dict_data, file, indent = 4)
    
    # Initialize the above lists
    # uid
    
    ## Preprocess:
    time_from_day = ''
    time_to_day = ''
    del_postid = []
    num_return = 0
    num_filter = 0
    num_twitter = 0
    num_facebook = 0
    num_ins = 0
    for index, row in df.iterrows():
        json_object = json.loads(row['data_json'])
        if(row['data_type']=='API-response'):
            post_date = json_object['posted']
            post_day = post_date.split(' ')[0]
            post_time = post_date.split(' ')[1]
            post_day = post_day.split('-')
            post_day = post_day[1] + '-' + post_day[2] + '-' + post_day[0]
            post_date = post_day + ' ' + post_time
            post_date_object = datetime.datetime.strptime(post_date, '%m-%d-%Y %H:%M:%S')
            local = aslocaltimestr(post_date_object)
            local_str = str(local)
            local_date = local_str.split(' ')[0]
            if checkrange == False:
                break
            else:
                time_from_day = time_from.split(' ')[0]
                time_from_date = time_from_day.split('-')
                time_from_date = time_from_date[1] + '-' + time_from_date[2]+ '-' + time_from_date[0]
                str_time_from = time_from_date + ' ' + time_from.split(' ')[1]
                time_from_object = datetime.datetime.strptime(str_time_from, '%m-%d-%Y %H:%M:%S')        
                time_to_day = time_to.split(' ')[0]
                time_to_date = time_to_day.split('-')
                time_to_date = time_to_date[1] + '-' + time_to_date[2]+ '-' + time_to_date[0]
                str_time_to = time_to_date + ' ' + time_to.split(' ')[1]
                time_to_object = datetime.datetime.strptime(str_time_to, '%m-%d-%Y %H:%M:%S')
                local_day = local.split(' ')[0]
                local_day = local_day.split('-')
                local_date = local_day[1] + '-' + local_day[2]+ '-' + local_day[0]
                str_local = local_date + ' ' + local.split(' ')[1]
                local_object = datetime.datetime.strptime(str_local, '%m-%d-%Y %H:%M:%S')
                if(local_object <= time_to_object and local_object >= time_from_object):
                    continue
                else:
                    del_postid.append(row['post_id'])
                    
    for index, row in df.iterrows():
        if row['post_id'] in del_postid:
            df = df.drop(index)
    for index, row in df.iterrows():
        if row['data_type'] =='API-response':
            num_return+=1
    for index, row in df.iterrows():
        json_object = json.loads(row['data_json'])
        if(row['data_type']=='API-response'):
            if(check_not_exist(json_object['postid'], content_list)):
                content_list.append({json_object['postid']: json_object['text']})
    for index, row in df.iterrows():
        contain_filter_key = False
        for i in range(len(content_list)):
            if row['post_id'] in content_list[i]:
                if len(filter_keys) > 0:
                    for j in range(len(filter_keys)):
                        if(filter_keys[j] in content_list[i][row['post_id']]):
                            contain_filter_key = True
        if contain_filter_key == True:
            df.drop(index, inplace=True)
        else:
            continue
    for index, row in df.iterrows():
        if(row['data_type']=='API-response'):
            num_filter += 1
            json_object = json.loads(row['data_json'])
            if(json_object['network']=='twitter'):
                num_twitter+=1
            elif(json_object['network']=='instagram'):
                num_ins += 1
            elif(json_object['network']=='facebook'):
                num_facebook +=1
      
    post_sentiments_list = []
    for index, row in df.iterrows():
        if(row['data_type']=='API-response'):
            if(check_not_exist(row['post_id'], post_sentiments_list)):
                post_sentiments_list.append({row['post_id']:''})
    for index, row in df.iterrows():
        if(row['data_type']=='NLP-analyze-sentiment'):
            json_object = json.loads(row['data_json'])
            for i in range(len(post_sentiments_list)):
                if(row['post_id'] in post_sentiments_list[i]):
                    if 'documentSentiment' in json_object:
                        score = json_object['documentSentiment']['score']
                    else:
                        score = float(json_object['score'].replace("%", ""))/100
                    post_sentiments_list[i][row['post_id']] = score
    num_neutral = 0
    num_positive = 0
    num_negative = 0
    
    # initialize the dfs
    if (path.exists("deduplicated_influencer_missing_posts.csv")):
        missing_file = pd.read_csv("deduplicated_influencer_missing_posts.csv")
        
        if path.exists("/home/jupyter/input_files/remove.csv"):
            remove = pd.read_csv("/home/jupyter/input_files/remove.csv")
            remove_ids = []
            posts = []
            for index, row in remove.iterrows():
                if("'" in row['post_id'] and row['post_id'][-1] == "'"):
                    remove_id = row['post_id'].replace("'", "")
                remove_ids.append(remove_id)
            for index, row in missing_file.iterrows():
                if(row['post_id'] in remove_ids):
                    missing_file = missing_file.drop(index)
        for index, row in missing_file.iterrows():
            account_name_key = 'account_name'
            if 'account_name' in row.keys():
                account_name_key = 'account_name'
            elif '\ufeffaccount_name' in row.keys():
                account_name_key = '\ufeffaccount_name'
            else:
                for key in list(row.keys()):
                    if 'account_name' in key:
                        account_name_key = key
            if(check_not_exist(row[account_name_key], name_list)):
                name_list.append({row[account_name_key]:[]})
            if(check_not_exist(row[account_name_key], source_list)):
                source_list.append({row[account_name_key]: row['source']})
        
        for index, row in missing_file.iterrows():
            text_list = str(row['post']).split(' ')
            for j in range(len(text_list)):
                if ('#' in text_list[j] and text_list[j][0] == '#'):
                    hashtag = text_list[j]
                    hashtag = hashtag.replace(',', '')
                    hashtag = hashtag.lower()
                    if(check_not_exist(hashtag, hashtag_list)):
                        hashtag_list.append({hashtag: {'sentiments':[], 'occurences':0, 'post_ids':[]}})
        
        for index, row in missing_file.iterrows():
            for i in range(len(hashtag_list)):
                for key in hashtag_list[i]:
                    if(key in row['post'].lower()):
                        hashtag_list[i][key]['post_ids'].append(row['post_id'])
                        num = row['post'].lower().count(key)
                        hashtag_list[i][key]['occurences'] += num
        
        for index, row in missing_file.iterrows():
            sentiment_result = analyze_text_sentiment(row['post'])
            for i in range(len(hashtag_list)):
                for key in hashtag_list[i]:
                    if(row['post_id'] in hashtag_list[i][key]['post_ids']):
                        if('documentSentiment' in sentiment_result):
                            score = sentiment_result['documentSentiment']['score']
                            hashtag_list[i][key]['sentiments'].append(score)
                        else:
                            score = sentiment_result['score']
                            hashtag_list[i][key]['sentiments'].append(score)
        
        
        for index, row in missing_file.iterrows():
            #sentiment_result = analyze_text_sentiment(row['post'])
            entity_sentiment_result = analyze_entity_sentiment(row['post'])
            for i in range(len(entity_sentiment_result['entities'])):
                name = entity_sentiment_result['entities'][i]['name']
                if(check_not_exist(name, entity_sentiments_list)):
                    entity_sentiments_list.append({name:{'sentiments':[], 'salience':[], 'post_ids':[], 'number':[], 'type':[]}})
        
        for index, row in missing_file.iterrows():
            account_name_key = 'account_name'
            if 'account_name' in row.keys():
                account_name_key = 'account_name'
            elif '\ufeffaccount_name' in row.keys():
                account_name_key = '\ufeffaccount_name'
            else:
                for key in list(row.keys()):
                    if 'account_name' in key:
                        account_name_key = key
            if row[account_name_key].lower() == 'roycommdvsrc':
                    continue
            sentiment_result = analyze_text_sentiment(row['post'])
            entity_sentiment_result = analyze_entity_sentiment(row['post'])
            print(entity_sentiment_result['entities'])
            num_return += 1
            num_filter += 1
            if(row['source']=='twitter'):
                num_twitter+=1
            elif(row['source']=='instagram'):
                num_ins += 1
            elif(row['source']=='facebook'):
                num_facebook +=1
            for i in range(len(entity_sentiment_result['entities'])):
                for j in range(len(entity_sentiments_list)):
                    name = entity_sentiment_result['entities'][i]['name']
                    if(name in entity_sentiments_list[j]):
                        entity_sentiments_list[j][name]['sentiments'].append(entity_sentiment_result['entities'][i]['sentiment']['score'])
                        entity_sentiments_list[j][name]['salience'].append(entity_sentiment_result['entities'][i]['salience'])
                        entity_sentiments_list[j][name]['type'].append(entity_sentiment_result['entities'][i]['type'])
                        entity_sentiments_list[j][name]['post_ids'].append(row['post_id'])
                        entity_sentiments_list[j][name]['number'].append(len(entity_sentiment_result['entities'][i]['mentions']))
            if('documentSentiment' in sentiment_result):
                score = sentiment_result['documentSentiment']['score']
                
                if(score >= 0.2):
                    num_positive+=1
                elif score<= -0.2:
                    num_negative += 1
                elif -0.2 < score<0.2:
                    num_neutral +=1 
                
                str_postdate = row['post_date']
                str_postdate = str_postdate.split('/')
                if int(str_postdate[0]) < 10:
                    postdate = str_postdate[2] + '-' + str_postdate[1] + '-0' + str_postdate[0]
                else:
                    postdate = str_postdate[2] + '-' + str_postdate[1] + '-' + str_postdate[0]
                magnitude = sentiment_result['documentSentiment']['magnitude']
                df_sentiments['post_id'].append(row['post_id']+"'")
                df_sentiments['sentence'].append(row['post'])
                df_sentiments['score'].append(score)
                df_sentiments['magnitude'].append(magnitude)
                df_sentiments['account_name'].append(row[account_name_key])
                df_sentiments['post_date'].append(postdate)
                df_sentiments['source'].append(row['source'])
                #print(json_object['url'])
                if(row['post_url'] == ''):  
                    df_sentiments['post_url'].append(' ')
                
                if row['likes'] == '':
                    num_likes = 0
                else:
                    num_likes = row['likes']
                if row['comments'] == '':
                    num_comments = 0
                else:
                    num_comments = row['comments']
                if row['retweets'] == '':
                    num_retweets = 0
                else:
                    num_retweets = row['retweets']
                df_sentiments['likes'].append(row['likes'])
                df_sentiments['comments'].append(row['comments'])
                df_sentiments['retweets/shares'].append(row['retweets'])
                df_sentiments['engagement'].append(num_likes+num_comments+num_retweets)
            else:
                score = sentiment_result['score']
                
                if(score >= 0.2):
                    num_positive+=1
                elif score<= -0.2:
                    num_negative += 1
                elif -0.2 < score<0.2:
                    num_neutral +=1
                str_postdate = row['post_date']
                str_postdate = str_postdate.split('/')
                #if int(str_postdate[0]) < 10:
                #postdate = str_postdate[2] + '-' + str_postdate[1] + '-0' + str_postdate[0]
                #else:
                    #postdate = str_postdate[2] + '-' + str_postdate[1] + '-' + str_postdate[0]
                postdate = row['post_date']
                magnitude = sentiment_result['magnitude']
                df_sentiments['post_id'].append(row['post_id'])
                df_sentiments['sentence'].append(row['post'])
                df_sentiments['score'].append(score)
                df_sentiments['magnitude'].append(magnitude)
                df_sentiments['account_name'].append(row[account_name_key])
                df_sentiments['post_date'].append(postdate)
                df_sentiments['source'].append(row['source'])
                #print(json_object['url'])    
                df_sentiments['post_url'].append(row['post_url'])
                
                if row['likes'] == '':
                    num_likes = 0
                else:
                    num_likes = row['likes']
                if row['comments'] == '':
                    num_comments = 0
                else:
                    num_comments = row['comments']
                if row['retweets'] == '':
                    num_retweets = 0
                else:
                    num_retweets = row['retweets']
                df_sentiments['likes'].append(row['likes'])
                df_sentiments['comments'].append(row['comments'])
                df_sentiments['retweets/shares'].append(row['retweets'])
                df_sentiments['engagement'].append(num_likes+num_comments+num_retweets)
    
    
    
    for index, row in df.iterrows():
        json_object = json.loads(row['data_json'])
        if(row['data_type']=='API-response'):
            if('userid' in json_object['user']):
                if(check_not_exist(json_object['user']['userid'], id_list)):
                    id_list.append({json_object['user']['userid']:[]})
    #username
    for index, row in df.iterrows():
        json_object = json.loads(row['data_json'])
        if(row['data_type']=='API-response'):
            if('name' in json_object['user']):
                if(check_not_exist(json_object['user']['name'], name_list)):
                    name_list.append({json_object['user']['name']:[]})
    for index, row in df.iterrows():
        json_object = json.loads(row['data_json'])
        if(row['data_type']=='API-response'):
            if('userid' in json_object['user']):
                for i in range(len(id_list)):
                    if(json_object['user']['userid'] in id_list[i]):
                        id_list[i][json_object['user']['userid']].append(row['post_id'])
                        date_list.append({row['post_id']: json_object['posted']})
    for index, row in df.iterrows():
        json_object = json.loads(row['data_json'])
        if(row['data_type']=='API-response'):
            if('name' in json_object['user']):
                for i in range(len(name_list)):
                    if(json_object['user']['name'] in name_list[i]):
                        name_list[i][json_object['user']['name']].append(row['post_id'])
                        #date_list.append({row['post_id']: json_object['posted']})
    for index, row in df.iterrows():
        json_object = json.loads(row['data_json'])
        if(row['data_type']=='API-response'):
            if(check_not_exist(json_object['user']['name'], source_list)):
                source_list.append({json_object['user']['name']: row['source']})
    
    for index, row in df.iterrows():
        json_object = json.loads(row['data_json'])
        if(row['data_type'] == 'NLP-analyze-entity-sentiment'):
            for j in range(len(json_object['entities'])):
                entity_name = json_object['entities'][j]['name'].lower()
                if(check_not_exist(entity_name, entity_sentiments_list)):
                    entity_sentiments_list.append({entity_name:{'sentiments':[], 'salience':[], 'post_ids':[], 'number':[], 'type':[]}})
    for index, row in df.iterrows():
        if(row['data_type'] == 'NLP-analyze-entity-sentiment'):
            json_object = json.loads(row['data_json'])
            for j in range(len(json_object['entities'])):
                for k in range(len(entity_sentiments_list)):
                    name = json_object['entities'][j]['name'].lower()
                    if(name in entity_sentiments_list[k]):
                        entity_sentiments_list[k][name]['sentiments'].append(json_object['entities'][j]['sentiment']['score'])
                        entity_sentiments_list[k][name]['salience'].append(json_object['entities'][j]['salience'])
                        entity_sentiments_list[k][name]['type'].append(json_object['entities'][j]['type'])
                        entity_sentiments_list[k][name]['post_ids'].append(row['post_id'])
                        entity_sentiments_list[k][name]['number'].append(len(json_object['entities'][j]['mentions']))
    for index, row in df.iterrows():
        if(row['data_type']=='API-response'):
            json_object = json.loads(row['data_json'])
            postid = json_object['postid']
            if(check_not_exist(postid, engage_list)):
                engage_list.append({postid:{'likes':0, 'comments':0, 'retweets':0, 'engagement':0, 'url': ' '}})
                
    for index, row in df.iterrows():
        if(row['data_type']=='API-response'):
            json_object = json.loads(row['data_json'])
            like_num = 0
            com_num = 0
            re_num = 0
            url = json_object['url']
            if(json_object['popularity']==None):
                eng_num = 0
            else:
                for m in range(len(json_object['popularity'])):
                    name = json_object['popularity'][m]['name']
                    if(name == 'likes'):
                        like_num = json_object['popularity'][m]['count']
                    elif(name == 'comments'):
                        com_num = json_object['popularity'][m]['count']
                    elif(name == 'retweets'):
                        re_num = json_object['popularity'][m]['count']
                eng_num = like_num + com_num + re_num
            for i in range(len(engage_list)):
                if(json_object['postid'] in engage_list[i]):
                    engage_list[i][json_object['postid']]['likes'] = like_num
                    engage_list[i][json_object['postid']]['comments'] = com_num
                    engage_list[i][json_object['postid']]['retweets'] = re_num
                    engage_list[i][json_object['postid']]['engagement'] = eng_num
                    engage_list[i][json_object['postid']]['url'] = url
    #print(engage_list)
    
    #print('eeeeeeeeeeeeeeeeeeeeeee')
    total_engage_list = []
    for index, row in df.iterrows():
        if(row['data_type']=='API-response'):
            json_object = json.loads(row['data_json'])
            if('name' in json_object['user']):
                if(check_not_exist(json_object['user']['name'], total_engage_list)):
                    total_engage_list.append({json_object['user']['name']:[]})
                
    for i in range(len(entity_sentiments_list)):
        for key in entity_sentiments_list[i]:
            typeof = ''
            #mode_type = mode(entity_sentiments_list[i][key]['type'])
            if(len(entity_sentiments_list[i][key]['type'])>0):
                mode_type = max(set(entity_sentiments_list[i][key]['type']), key=entity_sentiments_list[i][key]['type'].count)
            else:
                mode_type = 7
            if(mode_type==1):
                typeof = 'PERSON'
            elif(mode_type==2):
                typeof = 'LOCATION'
            elif(mode_type==3):
                typeof = 'ORGANIZATION'
            elif(mode_type==4):
                typeof = 'EVENT'
            elif(mode_type==5):
                typeof = 'WORK_OF_ART'
            elif(mode_type==6):
                typeof = 'CONSUMER_GOOD'
            elif(mode_type==7):
                typeof = 'OTHER'
            df_entity_sentiments_occ['entity'].append(key)
            df_entity_sentiments_occ['type'].append(typeof)
            if(len(entity_sentiments_list[i][key]['salience'])>0):
                df_entity_sentiments_occ['salience'].append(mean(entity_sentiments_list[i][key]['salience']))
            else:
                df_entity_sentiments_occ['salience'].append(0)
            if(len(entity_sentiments_list[i][key]['sentiments'])>0):
                df_entity_sentiments_occ['sentiment'].append(mean(entity_sentiments_list[i][key]['sentiments']))
            else:
                df_entity_sentiments_occ['sentiment'].append(0)
            if(len(entity_sentiments_list[i][key]['sentiments'])>0):
                df_entity_sentiments_occ['min_sentiment'].append(min(entity_sentiments_list[i][key]['sentiments']))
            else:
                df_entity_sentiments_occ['min_sentiment'].append(0)
            if(len(entity_sentiments_list[i][key]['sentiments'])>0):
                df_entity_sentiments_occ['max_sentiment'].append(max(entity_sentiments_list[i][key]['sentiments']))
            else:
                df_entity_sentiments_occ['max_sentiment'].append(0)
            if(len(entity_sentiments_list[i][key]['number'])>0):
                df_entity_sentiments_occ['occurence'].append(sum(entity_sentiments_list[i][key]['number']))
            else:
                df_entity_sentiments_occ['occurence'].append(0)
            #df_entity_sentiments_occ['sentiment'].append(mean(entity_sentiments_list[i][key]['sentiments']))
            #df_entity_sentiments_occ['min_sentiment'].append(min(entity_sentiments_list[i][key]['sentiments']))
            #df_entity_sentiments_occ['max_sentiment'].append(max(entity_sentiments_list[i][key]['sentiments']))
            #df_entity_sentiments_occ['occurence'].append(sum(entity_sentiments_list[i][key]['number']))
            df_entity_sentiments_occ['post_ids'].append(entity_sentiments_list[i][key]['post_ids'])
            
    for index, row in df.iterrows():
        if(row['data_type']=='API-response'):
            text_list = json_object['text'].split(' ')
            for j in range(len(text_list)):
                if ('#' in text_list[j] and text_list[j][0] == '#'):
                    hashtag = text_list[j]
                    hashtag = hashtag.replace(',', '')
                    hashtag = hashtag.replace('.', '')
                    hashtag = hashtag.replace('?', '')
                    hashtag = hashtag.replace('"', '')
                    hashtag = hashtag.lower()
                    if(check_not_exist(hashtag, hashtag_list)):
                        hashtag_list.append({hashtag: {'sentiments':[], 'occurences':0, 'post_ids':[]}})
    for index, row in df.iterrows():
        if(row['data_type']=='API-response'):
            for i in range(len(hashtag_list)):
                for key in hashtag_list[i]:
                    if(key in json_object['text'].lower()):
                        hashtag_list[i][key]['post_ids'].append(row['post_id'])
                        num = json_object['text'].lower().count(key)
                        hashtag_list[i][key]['occurences'] += num
    #print(hashtag_list)
    #return
    #hashtag processing
    
    for index, row in df.iterrows():
        if(row['data_type'] == 'NLP-analyze-sentiment'):
            json_object = json.loads(row['data_json'])
            for i in range(len(hashtag_list)):
                for key in hashtag_list[i]:
                    if(row['post_id'] in hashtag_list[i][key]['post_ids']):
                        if('documentSentiment' in json_object):
                            score = json_object['documentSentiment']['score']
                            hashtag_list[i][key]['sentiments'].append(score)
                        else:
                            score = float(json_object['score'].replace("%", ""))/100
                            hashtag_list[i][key]['sentiments'].append(score)

    for i in range(len(hashtag_list)):
        for key in hashtag_list[i]:   
            df_hash_sentiments['hashtags'].append(key)
            df_hash_sentiments['occurences'].append(hashtag_list[i][key]['occurences'])
            if len(hashtag_list[i][key]['sentiments']) == 0:
                df_hash_sentiments['average_sentiments'].append('')
            else:
                df_hash_sentiments['average_sentiments'].append(mean(hashtag_list[i][key]['sentiments']))
            df_hash_sentiments['post_ids'].append(hashtag_list[i][key]['post_ids'])
    df_hash_sentiments = pd.DataFrame(data = df_hash_sentiments)
    if checkrange == True:
        df_hash_sentiments.to_csv('/home/jupyter/influencer_outputs/4_influencer_hashtags_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=local_time, timefrom = time_from_day, timeto= time_to_day), index = False)
    else:
        df_hash_sentiments.to_csv('/home/jupyter/influencer_outputs/4_influencer_hashtags_{localtime}_all.csv'.format(localtime=local_time), index = False)

    for index, row in df.iterrows():
        #print(row['data_type'])
        json_object = json.loads(row['data_json'])        
        post_id = row['post_id']
        post_date = ''
        for i in range(len(date_list)):
            if(post_id in date_list[i]):
                post_date = date_list[i][post_id]
        print(date_list)
        if(post_date!=''):
            post_day = post_date.split(' ')[0]
            #print(post_day)
            post_time = post_date.split(' ')[1]
            post_day = post_day.split('-')
            post_day = post_day[1] + '-' + post_day[2] + '-' + post_day[0]
            post_date = post_day + ' ' + post_time
            post_date_object = datetime.datetime.strptime(post_date, '%m-%d-%Y %H:%M:%S')
            local = aslocaltimestr(post_date_object)
            local_str = str(local)
            local_date = local_str.split(' ')[0]
            if(checkrange == False):
                if(weekday==0):
                    if(local_date in before_list):
                        post_date = local_str
                    else:
                        post_date = local_str
                else:                
                    if(local_date == yesterday):
                        post_date = local_str
                    else:
                        post_date = local_str
            else:
                #current_date = datetime.date(int(local_date.split('-')[0]), int(local_date.split('-')[1]), int(local_date.split('-')[2]))
                time_from_day = time_from.split(' ')[0]
                time_from_date = time_from_day.split('-')
                time_from_date = time_from_date[1] + '-' + time_from_date[2]+ '-' + time_from_date[0]
                str_time_from = time_from_date + ' ' + time_from.split(' ')[1]
                time_from_object = datetime.datetime.strptime(str_time_from, '%m-%d-%Y %H:%M:%S')
                
                time_to_day = time_to.split(' ')[0]
                time_to_date = time_to_day.split('-')
                time_to_date = time_to_date[1] + '-' + time_to_date[2]+ '-' + time_to_date[0]
                str_time_to = time_to_date + ' ' + time_to.split(' ')[1]
                time_to_object = datetime.datetime.strptime(str_time_to, '%m-%d-%Y %H:%M:%S')
                #print(current_date)
                local_day = local.split(' ')[0]
                local_day = local_day.split('-')
                local_date = local_day[1] + '-' + local_day[2]+ '-' + local_day[0]
                str_local = local_date + ' ' + local.split(' ')[1]
                local_object = datetime.datetime.strptime(str_local, '%m-%d-%Y %H:%M:%S')
                if(local_object <= time_to_object and local_object >= time_from_object):
                    post_date = str_local
                else:
                    continue
        
        user_id = ''
        user_name = ''
        source = ''
        if(row['data_type'] == 'NLP-analyze-sentiment'):
            #print(type(row['post_id']))
            #print(type(list(engage_list[-1].keys())[0]))
            #print(row['post_id'] == list(engage_list[-1].keys())[0])
            #print('hello')
            for i in range(len(id_list)):
                for key in id_list[i]:
                    uid = key
                for j in range(len(id_list[i][uid])):
                    if(row['post_id']==id_list[i][uid][j]):
                        user_id = uid
            for i in range(len(name_list)):
                for key in name_list[i]:
                    name = key
                    for j in range(len(name_list[i][name])):
                        if(row['post_id']==name_list[i][name][j]):
                            user_name = name
            sentence = ''
            like_num = 0
            com_num = 0
            re_num = 0
            url = " " 
            if('documentSentiment' in json_object):
                
                for j in range(len(json_object['sentences'])):
                    sentence+= json_object['sentences'][j]['text']['content']
                    
                score = json_object['documentSentiment']['score']
                magnitude = json_object['documentSentiment']['magnitude']
                df_sentiments['post_id'].append(post_id)
                df_sentiments['sentence'].append(sentence)
                df_sentiments['score'].append(score)
                df_sentiments['magnitude'].append(magnitude)
                
                #df_sentiments['account_id'].append(user_id)
                df_sentiments['account_name'].append(user_name)
                df_sentiments['post_date'].append(post_date)
                #df_sentiments['account_name'].append(user_name)
                for j in range(len(source_list)):
                    if(user_name in source_list[j]):
                        source = source_list[j][user_name]
                        break
                df_sentiments['source'].append(source)
                #print(json_object['url'])
                n = 0
                
                for j in range(len(engage_list)):
                    print(engage_list[j])
                    if(str(row['post_id']) == list(engage_list[j].keys())[0]):
                        n = n + 1
                        df_sentiments['post_url'].append(engage_list[j][str(row['post_id'])]['url'])
                        df_sentiments['likes'].append(engage_list[j][str(row['post_id'])]['likes'])
                        df_sentiments['comments'].append(engage_list[j][str(row['post_id'])]['comments'])
                        df_sentiments['retweets/shares'].append(engage_list[j][str(row['post_id'])]['retweets'])
                        df_sentiments['engagement'].append(engage_list[j][str(row['post_id'])]['engagement'])
                print(n)
            #print(json_object)
            elif('documentSentiment' not in json_object):
                sentence = json_object['text']
                score = float(json_object['score'].replace("%", ""))/100
                magnitude = float(json_object['magnitude'].replace("%", ""))/100
                df_sentiments['post_id'].append(post_id)
                df_sentiments['sentence'].append(sentence)
                df_sentiments['score'].append(score)
                df_sentiments['magnitude'].append(magnitude)
                #df_sentiments['account_id'].append(user_id)
                df_sentiments['account_name'].append(user_name)
                df_sentiments['post_date'].append(post_date)
                #df_sentiments['account_name'].append(user_name)
                for j in range(len(source_list)):
                    if(user_name in source_list[j]):
                        source = source_list[j][user_name]
                        break
                df_sentiments['source'].append(source)
                for j in range(len(engage_list)):
                    if(str(row['post_id']) == list(engage_list[j].keys())[0]):     
                        df_sentiments['post_url'].append(engage_list[j][str(row['post_id'])]['url'])
                        df_sentiments['likes'].append(engage_list[j][str(row['post_id'])]['likes'])
                        df_sentiments['comments'].append(engage_list[j][str(row['post_id'])]['comments'])
                        df_sentiments['retweets/shares'].append(engage_list[j][str(row['post_id'])]['retweets'])
                        df_sentiments['engagement'].append(engage_list[j][str(row['post_id'])]['engagement'])
        elif(row['data_type'] == 'NLP-analyze-entities'):
            print('entity')
            for i in range(len(id_list)):
                for key in id_list[i]:
                    uid = key
                for j in range(len(id_list[i][uid])):
                    if(row['post_id']==id_list[i][uid][j]):
                        user_id = uid
            
            for i in range(len(name_list)):
                for key in name_list[i]:
                    name = key
                for j in range(len(name_list[i][name])):
                    if(row['post_id']==name_list[i][name][j]):
                        user_name = name
            #print(json_object)
            if(len(json_object)>0):
                for i in range(len(json_object['entities'])):
                    typeof = ""
                    df_entities['post_id'].append(post_id)
                    if(json_object['entities'][i]['type']==1):
                        typeof = 'PERSON'
                    elif(json_object['entities'][i]['type']==2):
                        typeof = 'LOCATION'
                    elif(json_object['entities'][i]['type']==3):
                        typeof = 'ORGANIZATION'
                    elif(json_object['entities'][i]['type']==4):
                        typeof = 'EVENT'
                    elif(json_object['entities'][i]['type']==5):
                        typeof = 'WORK_OF_ART'
                    elif(json_object['entities'][i]['type']==6):
                        typeof = 'CONSUMER_GOOD'
                    elif(json_object['entities'][i]['type']==7):
                        typeof = 'OTHER'
                    df_entities['type'].append(typeof)
                    df_entities['salience'].append(json_object['entities'][i]['salience'])
                    df_entities['entity'].append(json_object['entities'][i]['name'])
                    #df_entities['account_id'].append(user_id)
                    df_entities['account_name'].append(user_name)
                    df_entities['post_date'].append(post_date)
                    #df_entities['account_name'].append(user_name)
                    
                    for j in range(len(source_list)):
                        if(user_name in source_list[j]):
                            source = source_list[j][user_name]
                            break
                    df_entities['source'].append(source)
    new_df_es = pd.DataFrame(data=df_entity_sentiments_occ)
    if checkrange == True:
        new_df_es.to_csv('/home/jupyter/influencer_outputs/2_influencer_entities_sentiments_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=local_time, timefrom=time_from_day, timeto=time_to_day), index=False)
    else:
        new_df_es.to_csv('/home/jupyter/influencer_outputs/2_influencer_entities_sentiments_{localtime}_all.csv'.format(localtime=local_time), index=False)
    print(df_sentiments)
    new_df_sent =pd.DataFrame(data=df_sentiments)
    grouped_sent = new_df_sent.groupby(['account_name', 'source'])['engagement'].sum().reset_index()
    if checkrange == True:
        grouped_sent.to_csv('/home/jupyter/influencer_outputs/3_influencer_account_engagement_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=local_time, timefrom=time_from_day, timeto=time_to_day), index=False)
    else:
        grouped_sent.to_csv('/home/jupyter/influencer_outputs/3_influencer_account_engagement_{localtime}_all.csv'.format(localtime=local_time), index=False)
    post_with_labels = get_class_df(new_df_sent, local_time, checkrange, time_from_day, time_to_day)
    new_df_e = pd.DataFrame(data=df_entities)
    person = new_df_es.loc[new_df_es['type'] == 'PERSON']
    entity = new_df_es.loc[new_df_es['type'] != 'PERSON']
    if checkrange == True:
        entity.sort_values(by='sentiment', ascending=False).head(10).to_csv('/home/jupyter/influencer_outputs/influencer_top_entity_positive_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=local_time, timefrom=time_from_day, timeto=time_to_day), index=False)
        entity.sort_values(by='sentiment', ascending=True).head(10).to_csv('/home/jupyter/influencer_outputs/influencer_top_entity_negative_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=local_time, timefrom=time_from_day, timeto=time_to_day), index=False)        
        person.sort_values(by='sentiment', ascending=False).head(10).to_csv('/home/jupyter/influencer_outputs/influencer_top_person_positive_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=local_time, timefrom=time_from_day, timeto=time_to_day), index=False)
        person.sort_values(by='sentiment', ascending=True).head(10).to_csv('/home/jupyter/influencer_outputs/influencer_top_person_negative_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=local_time, timefrom=time_from_day, timeto=time_to_day), index=False)
    else:
        entity.sort_values(by='sentiment', ascending=False).head(10).to_csv('/home/jupyter/influencer_outputs/influencer_top_entity_positive_{localtime}_all.csv'.format(localtime=local_time), index=False)
        entity.sort_values(by='sentiment', ascending=True).head(10).to_csv('/home/jupyter/influencer_outputs/influencer_top_entity_negative_{localtime}_all.csv'.format(localtime=local_time), index=False)        
        person.sort_values(by='sentiment', ascending=False).head(10).to_csv('/home/jupyter/influencer_outputs/influencer_top_person_positive_{localtime}_all.csv'.format(localtime=local_time), index=False)
        person.sort_values(by='sentiment', ascending=True).head(10).to_csv('/home/jupyter/influencer_outputs/influencer_top_person_negative_{localtime}_all.csv'.format(localtime=local_time), index=False)
    average_sentiment_df = {"account_name":[], "source":[], "average_sentiments":[], "number_of_posts":[]}
    #acc_id = new_df_sent['account_id'].iloc[0]
    senti_list = []
    new_df_sent_grouped = new_df_sent.groupby("account_name")
    picked_name_list = []
    for index, row in new_df_sent.iterrows():
        if(check_not_exist(row['account_name'], picked_name_list)):
            picked_name_list.append({row['account_name']:[]})
    for index, row in new_df_sent.iterrows():
        for i in range(len(picked_name_list)):
            if(row['account_name'] in picked_name_list[i]):
                picked_name_list[i][row['account_name']].append(row['post_id'])
        
    mean_df = new_df_sent_grouped.mean()
    mean_df = mean_df.reset_index()
    for index, row in mean_df.iterrows():
        source = ''
        number_post = 0
        average_sentiment_df['account_name'].append(row['account_name'])
        for i in range(len(source_list)):
            if(row['account_name'] in source_list[i]):
                source = source_list[i][row['account_name']]
                break
        average_sentiment_df['source'].append(source)
        for i in range(len(picked_name_list)):
            if(row['account_name'] in picked_name_list[i]):
                number_post = len(picked_name_list[i][row['account_name']])
                break
        average_sentiment_df['number_of_posts'].append(number_post)
        #average_sentiment_df['account_name'].append(row['account_name'])
        #average_sentiment_df['source'].append(row['source'])
        average_sentiment_df['average_sentiments'].append(row['score'])
    average_sentiment_df = pd.DataFrame(data=average_sentiment_df)
    if(checkrange == True):
        average_sentiment_df.to_csv('/home/jupyter/influencer_outputs/influencer_average_sentiment_account_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=local_time, timefrom = time_from_day, timeto = time_to_day), index=False)
    else:
        average_sentiment_df.to_csv('/home/jupyter/influencer_outputs/influencer_average_sentiment_account_{localtime}_all.csv'.format(localtime=local_time), index=False)
    overall_sentiment_df = {'post_date':[],'overall_sentiment':[]}
    overall_score = new_df_sent.mean()['score']
    if(checkrange == False):
        overall_sentiment_df['post_date'].append('all_dates')
    else:
        overall_sentiment_df['post_date'].append(time_from_day + '  -  ' + time_to_day)
    overall_sentiment_df['overall_sentiment'].append(overall_score)
    overall_sentiment_df=pd.DataFrame(data=overall_sentiment_df)
    if(checkrange == True):
        overall_sentiment_df.to_csv('/home/jupyter/influencer_outputs/influencer_overall_sentiment_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=local_time, timefrom = time_from_day, timeto = time_to_day), index=False)
    else:
        overall_sentiment_df.to_csv('/home/jupyter/influencer_outputs/influencer_overall_sentiment_{localtime}_all.csv'.format(localtime=local_time), index=False)
    
    
    df_stat = {'Sources':[], 'Count':[]}
    df_stat['Sources'].append('Posts founded')
    df_stat['Sources'].append('Posts after filtering (processed)')
    df_stat['Sources'].append('Facebook')
    df_stat['Sources'].append('Twitter')
    df_stat['Sources'].append('Instagram')
    df_stat['Count'].append(num_return)
    df_stat['Count'].append(num_filter)
    df_stat['Count'].append(num_facebook)
    df_stat['Count'].append(num_twitter)
    df_stat['Count'].append(num_ins)
    #print(yesterday)
    for i in range(len(post_sentiments_list)):
        for key in post_sentiments_list[i]:
            
            if(post_sentiments_list[i][key] >= 0.2):
                num_positive+=1
            elif post_sentiments_list[i][key]<= -0.2:
                num_negative += 1
            elif -0.2 < post_sentiments_list[i][key]<0.2:
                num_neutral +=1
    df_sentiment_stat = {'Sentiment':[], 'Count':[], 'Percentage':[]}
    df_sentiment_stat['Sentiment'].append('Negative')
    df_sentiment_stat['Sentiment'].append('Positive')
    df_sentiment_stat['Sentiment'].append('Neutral')
    df_sentiment_stat['Count'].append(num_negative)
    df_sentiment_stat['Count'].append(num_positive)
    df_sentiment_stat['Count'].append(num_neutral)
    if(num_filter != 0):
        df_sentiment_stat['Percentage'].append(num_negative/num_filter)
        df_sentiment_stat['Percentage'].append(num_positive/num_filter)
        df_sentiment_stat['Percentage'].append(num_neutral/num_filter)
    else:
        df_sentiment_stat['Percentage'].append(0)
        df_sentiment_stat['Percentage'].append(0)
        df_sentiment_stat['Percentage'].append(0)
    
    df_stat = pd.DataFrame(data=df_stat)
    df_sentiment_stat = pd.DataFrame(data=df_sentiment_stat)
    if(checkrange==True):
        df_stat.to_csv('/home/jupyter/influencer_outputs/0_influencer_stats_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=local_str, timefrom=time_from, timeto=time_to), index=False)
        df_sentiment_stat.to_csv('/home/jupyter/influencer_outputs/1_influencer_sentiment_breakdown_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=local_str, timefrom=time_from, timeto=time_to),index=False)
    else:
        df_stat.to_csv('/home/jupyter/influencer_outputs/0_influencer_stats_{localtime}_all.csv'.format(localtime=local_str), index=False)
        df_sentiment_stat.to_csv('/home/jupyter/influencer_outputs/1_influencer_sentiment_breakdown_{localtime}_all.csv'.format(localtime=local_str), index=False)
    return df_stat, df_sentiment_stat, new_df_es, grouped_sent, df_hash_sentiments, post_with_labels
    

    
    
## generate_csvs    
def translate_text(target,text):
    """Translates text into the target language (English).
        Target must be an ISO 639-1 language code (e.g."en").
    """
    #logger  = gcp_logger()
    translate_client = translate_v2.Client()
    if isinstance(text, six.binary_type):
        text = text.decode("utf-8")
    # Text can also be a sequence of strings, in which case this method
    # will return a sequence of results for each text.
    result = translate_client.translate(text, target_language=target)
    detectedSourceLanguage = result["detectedSourceLanguage"]
    #logger.debug( f"The source language is: {detectedSourceLanguage}" )
    #logger.debug( f"The translated text is: {result}" )
    return result

def initialize_df(posts):
    
    account_names = []
    sources = []
    post_dates = []
    post_ids = []
    sentences = []
    scores = []
    magnitudes = []
    likes = []
    comments =[]
    retweets = []
    engagement = []
    url = []
    all_entities = {}
    for post in posts:
        post_id = post.get("postid","")
        if post_id+"'" not in post_ids:
            post_ids.append(post_id+"'")
            account_names.append(post.get("user",{}).get("name",""))
            date = post.get("posted","")
            if date:
                posted = date.replace('+', "").strip()
                posted_local = dateutil.parser.parse(posted).astimezone(pytz.timezone('Australia/Sydney'))
                posted_str = str(posted_local).split(" ")[0]
                post_dates.append(posted_str)
            else:
                post_dates.append(date)
        
            sources.append(post.get("network",""))
            sentences.append(post.get("text",""))
            scores.append(post.get("analysis",{}).get("text_analysis",{}).get("sentiment_analysis",{}).get("score",0))
            magnitudes.append(post.get("analysis",{}).get("text_analysis",{}).get("sentiment_analysis",{}).get("magnitude",0))
            entities = post.get("analysis",{}).get("text_analysis",{}).get("entity_sentiment_analysis",{}).get("entities",[])
            #print(entities)
            if entities:
                for entity in entities:
                    entity_name = entity.get("name","").lower()
                    #print(entity_name)
                    if entity_name not in all_entities.keys():
                        all_entities[entity_name] = {}
                        all_entities[entity_name]["name"] = [entity.get("name","").lower()]
                        all_entities[entity_name]["type"] = [entity.get("type","")]
                        all_entities[entity_name]["salience"] = [entity.get("salience",0)]
                        all_entities[entity_name]["sentiment"] = [entity.get("sentiment",{}).get("score",0)]
                        all_entities[entity_name]["post_ids"] = [post.get("postid","")+"'"]
                    else:
                        all_entities[entity_name]["name"].append(entity.get("name","").lower())
                        all_entities[entity_name]["type"].append(entity.get("type",""))
                        all_entities[entity_name]["salience"].append(entity.get("salience",0))
                        all_entities[entity_name]["sentiment"].append(entity.get("sentiment",{}).get("score",0))
                        all_entities[entity_name]["post_ids"].append(post.get("postid","")+"'")
                        
           
            popularity = post.get("popularity",{})
            if popularity:

                a =False
                b = False
                c = False
                for element in popularity:
                    name = element.get("name","")
                    count = element.get("count",0)
                    if not count:
                        count = 0
                    else:
                        count = int(float(count))
                    if name =="comments":
                        comments.append(count)
                        a = True
                    if name=="likes":
                        likes.append(count)
                        b = True
                    if name =="retweets":
                        retweets.append(count)
                        c= True
                if not a:
                    comments.append(0)
                if not b:
                    likes.append(0)
                if not c:
                    retweets.append(0)
            else:
                likes.append(0) 
                retweets.append(0)
                comments.append(0)
                
            post_url = post.get("url","")
            #print(post_url)
            url.append(post_url)
                
    for i in range(len(likes)):
        a= likes[i]+retweets[i]+comments[i]
        engagement.append(a)

            
    df = {
    "account_name": account_names,
    "source": sources,
    "post_date": post_dates,
    "post_id": post_ids,
    "sentence": sentences,
    "score": scores,
    "magnitude": magnitudes,
    "likes":likes,
    "comments":comments,
    "retweets/shares":retweets,
    "engagement":engagement,
    "post_url": url
    }

    df_post = pd.DataFrame(df)
    return df_post, all_entities

# 1.overall sentiment csv
# 2.average sentiment account csv
def generate_csvs(df_post, checkrange, time_from, time_to):
    # overall sentiment csv
    overall_sentiment = {'post_date':[], 'overall_sentiment':[]}
    if(checkrange==False):
        overall_sentiment['post_date'].append('all_dates')
    else:
        overall_sentiment['post_date'].append(time_from + ' - ' + time_to)
    df_post_1 = df_post.loc[df_post['account_name'] != 'roycommDVSRC']
    overall_sentiment['overall_sentiment'].append(df_post_1['score'].mean())
    overall_sentiment = pd.DataFrame(overall_sentiment)
    #overall_sentiment = df_post.groupby('post_date').agg({'score':'mean'}).rename(columns={'score':'overall_sentiment'})
    # average sentiment account csv
    #average_sentiment_account = df_post.groupby(['account_name','source']).agg({'score':'mean','post_id':'count'}).rename(columns={'score':'average_sentiments','post_id':'number_of_posts'})
    
    new_df_sent_grouped = df_post_1.groupby("account_name")
    picked_name_list = []
    for index, row in df_post_1.iterrows():
        if(check_not_exist(row['account_name'], picked_name_list)):
            picked_name_list.append({row['account_name']:[]})
    for index, row in df_post_1.iterrows():
        for i in range(len(picked_name_list)):
            if(row['account_name'] in picked_name_list[i]):
                picked_name_list[i][row['account_name']].append(row['post_id'])
                
    average_sentiment_df = {"account_name":[], "source":[], "average_sentiments":[], "number_of_posts":[]}
    post_source_list=[]
    for index, row in df_post.iterrows():
        if(check_not_exist(row['account_name'], post_source_list)):
            post_source_list.append({row['account_name']: row['source']})
    
    mean_df = new_df_sent_grouped.mean()
    mean_df = mean_df.reset_index()
    for index, row in mean_df.iterrows():
        source = ''
        number_post = 0
        average_sentiment_df['account_name'].append(row['account_name'])
        for i in range(len(post_source_list)):
            if(row['account_name'] in post_source_list[i]):
                source = post_source_list[i][row['account_name']]
                break
        average_sentiment_df['source'].append(source)
        for i in range(len(picked_name_list)):
            if(row['account_name'] in picked_name_list[i]):
                number_post = len(picked_name_list[i][row['account_name']])
                break
        average_sentiment_df['number_of_posts'].append(number_post)
        #average_sentiment_df['account_name'].append(row['account_name'])
        #average_sentiment_df['source'].append(row['source'])
        average_sentiment_df['average_sentiments'].append(row['score'])
    average_sentiment_df = pd.DataFrame(average_sentiment_df)
    
    return overall_sentiment, average_sentiment_df

# 3. entities sentiments csv
def entities_sentiments_csv(all_entities):
    entity_rows = []
    for key in all_entities.keys():
        #print(all_entities)
        if key:
            entity = all_entities[key]
            name = key
            entity_number = entity["type"][0]
            type_list = ["UNKNOWN","PERSON","LOCATION","ORGANIZATION","EVENT","WORK_OF_ART","CONSUMER_GOOD","OTHER"]
            entity_type = type_list[entity_number]
            posts_ids = list(set(entity["post_ids"]))
            max_sentiment = max(entity["sentiment"])
            min_sentiment = min(entity["sentiment"])
            df_entity = pd.DataFrame(entity)
            entity_group = df_entity.groupby('name').agg({'sentiment':'mean','salience':'mean','name':'count'})
            sentiment = entity_group['sentiment'][0]
            entity_count = entity_group['name'][0]
            salience = entity_group['salience'][0]
            entity_rows.append([name,entity_type,posts_ids,sentiment,max_sentiment,min_sentiment,entity_count,salience])   
    
    entities_sentiments = pd.DataFrame(entity_rows, columns = ["entity","type","posts_ids","sentiment","max_sentiment","min_sentiment","occurence","salience"])
    
    return entities_sentiments
    
if __name__ == "__main__":
    if path.exists("/home/jupyter/influencer_outputs"):
        num = 0
    else:
        os.mkdir('/home/jupyter/influencer_outputs')
    if path.exists("/home/jupyter/public_outputs"):
        num = 0
    else:
        os.mkdir('/home/jupyter/public_outputs')
    australia = pytz.timezone('Australia/Sydney')
    au_time = datetime.datetime.now(australia)
    au_time = str(au_time)
    au_time = au_time.split('+')[0]
    au_date = au_time.split(" ")[0]
    au_date = au_date.split('-')
    au_date = au_date[1] + '-' + au_date[2] + '-' + au_date[0]
    #print(au_date)
    au_date = datetime.datetime.strptime(au_date, '%m-%d-%Y').date()
    weekday = au_date.weekday()
    #print(type(weekday))
    yesterday = au_date - datetime.timedelta(days=1)
    beforeyesterday = au_date - datetime.timedelta(days=2)
    yesterday = str(yesterday)
    beforeyesterday = str(beforeyesterday)
    influencer_df_stat, influencer_df_sentiment_stat, influencer_new_df_es, influencer_grouped_sent, influencer_df_hash_sentiments, influencer_post_with_labels = process_df(beforeyesterday,yesterday, weekday, au_time)
    #sleep(5)
    #get_class()
    #print(weekday)
    
    ## public part
    
    f = open('public_filters.json')
    public_filters = json.load(f)
    time_from = ''
    time_to = ''
    #if filter_keys in influencer_filter:
    if 'time_from' in public_filters:
        time_from = public_filters['time_from']
    if 'time_to' in public_filters:
        time_to = public_filters['time_to']
    if 'filter_keys' in public_filters:
        filter_keys = public_filters['filter_keys']
        
    time_from_day = time_from.split(' ')[0]
    time_to_day = time_to.split(' ')[0]
    checkrange = False
    if(time_from=='' or time_to == ''):
        checkrange = False
    else:
        checkrange = True
    with open("deduplicated_social_searcher_response.json","r") as f:
        raw_posts = json.load(f)

    #missing part for public
    if (path.exists("deduplicated_public_missing_posts.csv")):
        Missing_posts = True
    else:
        Missing_posts = True
    
    if Missing_posts == False:
                # initialize and translate text
        for i in range(len(raw_posts)):
            raw_posts[i]["analysis"] =  {"text_analysis": {"sentiment_analysis":'',
                                             "entity_sentiment_analysis":''}
                                                                            }
            raw_posts[i]["translated_text"] = ''
            #language = raw_posts[i].get("lang", "unknown")
            target = "en"
            translate = translate_text(target, raw_posts[i]["text"])
            raw_posts[i]["translated_text"] = translate["translatedText"]
            #trans_text = translate["translatedText"]
        
        for i in range(len(raw_posts)):
            # add nlp analysis
            text = raw_posts[i]["translated_text"]
            sentiment_result = analyze_text_sentiment(text)# nlp sentiment analysis
            entity_sentiment_result = analyze_entity_sentiment(text)#nlp entity sentiment analysis
            #print(sentiment_result)
            raw_posts[i]["analysis"]["text_analysis"]["sentiment_analysis"]=sentiment_result
            #print(i)
            raw_posts[i]["analysis"]["text_analysis"]["entity_sentiment_analysis"]=entity_sentiment_result
        
        if path.exists("/home/jupyter/input_files/remove.csv"):
            remove = pd.read_csv("/home/jupyter/input_files/remove.csv")
            remove_ids = []
            posts = []
            for index, row in remove.iterrows():
                if("'" in row['post_id'] and row['post_id'][-1] == "'"):
                    remove_id = row['post_id'].replace("'", "")
                else:
                    remove_id = row['post_id']
                remove_ids.append(remove_id)
            for i in range(len(raw_posts)):
                if(raw_posts[i]['postid'] in remove_ids):
                    continue
                posts.append(raw_posts[i])
        else:
            posts = raw_posts
        hashtag_list = []
        for i in range(len(posts)):
            text_list = posts[i]['text'].split(' ')
            for j in range(len(text_list)):
                if('#' in text_list[j] and text_list[j][0]=='#'):
                    hashtag = text_list[j]
                    hashtag = hashtag.replace(',', '')
                    hashtag = hashtag.lower()
                    if(check_not_exist(hashtag, hashtag_list)):
                        hashtag_list.append({hashtag: {'sentiments':[], 'occurences':0, 'post_ids':[]}})
        for i in range(len(posts)):
            for j in range(len(hashtag_list)):
                for key in hashtag_list[j]:
                    if(key in posts[i]['text'].lower()):
                        hashtag_list[j][key]['post_ids'].append(posts[i]['post_id'])
                        hashtag_list[j][key]['occurences'] += posts[i]['text'].lower().count(key)
                        if('documentSentiment' in posts[i]['analysis']['text_analysis']['sentiment_analysis']):
                            score = posts[i]['analysis']['text_analysis']['sentiment_analysis']['documentSentiment']['score']
                            hashtag_list[j][key]['sentiments'].append(score)
                        else:
                            score = posts[i]['analysis']['text_analysis']['sentiment_analysis']['score']
                            hashtag_list[j][key]['sentiments'].append(score)
        df_hash_sentiments = {'hashtags':[], 'occurences':[], 'average_sentiments':[], 'post_ids':[]}
        for i in range(len(hashtag_list)):
            for key in hashtag_list[i]:
                df_hash_sentiments['hashtags'].append(key)
                df_hash_sentiments['occurences'].append(hashtag_list[i][key]['occurences'])
                df_hash_sentiments['average_sentiments'].append(mean(hashtag_list[i][key]['sentiments']))
                df_hash_sentiments['post_ids'].append(hashtag_list[i][key]['post_ids'])
        df_hash_sentiments = pd.DataFrame(data = df_hash_sentiments)
        df_post, all_entities = initialize_df(posts)
        overall_sentiment, average_sentiment_account = generate_csvs(df_post, checkrange, time_from_day, time_to_day)
        entities_sentiments = entities_sentiments_csv(all_entities)
        top_person_negative = entities_sentiments[(entities_sentiments['type']=="PERSON") & (entities_sentiments['sentiment']<=0)]    
        top_person_positive = entities_sentiments[(entities_sentiments['type']=="PERSON") & (entities_sentiments['sentiment']>0)]    
        top_entity_positive = entities_sentiments.sort_values(by=['sentiment'],ascending=False)
        top_entity_negative = entities_sentiments.sort_values(by=['sentiment'],ascending=True)
        df_post.to_csv('public_sentiments.csv',index=False)
    
        
        num_return = len(posts)
        num_filter = len(df_post)
        num_facebook = 0
        num_twitter = 0
        num_ins = 0
    
        for index, row in df_post.iterrows():
            if(row['source'] == 'facebook'):
                num_facebook+=1
            elif(row['source'] == 'twitter'):
                num_twitter +=1
            elif(row['source'] == 'instagram'):
                num_ins +=1
        df_stat = {'Sources':[], 'Count':[]}
        df_stat['Sources'].append('Posts founded')
        df_stat['Sources'].append('Posts after filtering (processed)')
        df_stat['Sources'].append('Facebook')
        df_stat['Sources'].append('Twitter')
        df_stat['Sources'].append('Instagram')
        df_stat['Count'].append(num_return)
        df_stat['Count'].append(num_filter)
        df_stat['Count'].append(num_facebook)
        df_stat['Count'].append(num_twitter)
        df_stat['Count'].append(num_ins)
 
        num_neutral = 0
        num_positive = 0
        num_negative = 0
        for index, row in df_post.iterrows():
            if row['account_name'].lower() == 'roycommdvsrc':
                continue
            score = row['score']
            if(score >= 0.2):
                num_positive+=1
            elif score<= -0.2:
                num_negative += 1
            elif -0.2 < score<0.2:
                num_neutral +=1
        df_sentiment_stat = {'Sentiment':[], 'Count':[], 'Percentage':[]}
        df_sentiment_stat['Sentiment'].append('Negative')
        df_sentiment_stat['Sentiment'].append('Positive')
        df_sentiment_stat['Sentiment'].append('Neutral')
        df_sentiment_stat['Count'].append(num_negative)
        df_sentiment_stat['Count'].append(num_positive)
        df_sentiment_stat['Count'].append(num_neutral)
        df_sentiment_stat['Percentage'].append(num_negative/num_filter)
        df_sentiment_stat['Percentage'].append(num_positive/num_filter)
        df_sentiment_stat['Percentage'].append(num_neutral/num_filter)
    
        df_stat = pd.DataFrame(df_stat)
        df_sentiment_stat = pd.DataFrame(df_sentiment_stat)
        public_account_engagement = df_post.groupby(["account_name","source"])['engagement'].sum().reset_index()
    
        if checkrange == True:
            overall_sentiment.to_csv('/home/jupyter/public_outputs/public_overall_sentiment_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom = time_from_day, timeto = time_to_day), index=False)
            average_sentiment_account.to_csv('/home/jupyter/public_outputs/public_average_sentiment_account_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom = time_from_day, timeto = time_to_day), index=False)
            entities_sentiments.to_csv('/home/jupyter/public_outputs/2_public_entities_sentiments_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom = time_from_day, timeto = time_to_day), index=False)
            top_person_negative.sort_values(by='sentiment', ascending=True).head(10).to_csv('/home/jupyter/public_outputs/public_top_person_negative_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom = time_from_day, timeto = time_to_day), index=False)
            top_person_positive.sort_values(by='sentiment', ascending=False).head(10).to_csv('/home/jupyter/public_outputs/public_top_person_positive_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom = time_from_day, timeto = time_to_day), index=False)
            top_entity_positive.sort_values(by='sentiment', ascending=False).head(10).to_csv('/home/jupyter/public_outputs/public_top_entity_positive_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom = time_from_day, timeto = time_to_day), index=False)
            top_entity_negative.sort_values(by='sentiment', ascending=True).head(10).to_csv('/home/jupyter/public_outputs/public_top_entity_negative_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom = time_from_day, timeto = time_to_day), index=False)
            df_hash_sentiments.to_csv('/home/jupyter/public_outputs/4_public_hashtags_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom = time_from_day, timeto = time_to_day), index=False)
            df_stat.to_csv('/home/jupyter/public_outputs/0_public_stats_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom=time_from, timeto=time_to), index=False)
            df_sentiment_stat.to_csv('/home/jupyter/public_outputs/1_public_sentiment_breakdown_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom=time_from, timeto=time_to),index=False)
            public_account_engagement.to_csv('/home/jupyter/public_outputs/3_public_account_engagement_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom=time_from, timeto=time_to), index=False)
        else:
            overall_sentiment.to_csv('/home/jupyter/public_outputs/public_overall_sentiment_{localtime}_all.csv'.format(localtime=au_time), index=False)
            average_sentiment_account.to_csv('/home/jupyter/public_outputs/public_average_sentiment_account_{localtime}_all.csv'.format(localtime=au_time), index=False)
            entities_sentiments.to_csv('/home/jupyter/public_outputs/2_public_entities_sentiments_{localtime}_all.csv'.format(localtime=au_time), index=False)
            top_person_negative.sort_values(by='sentiment', ascending=True).head(10).to_csv('/home/jupyter/public_outputs/public_top_person_negative_{localtime}_all.csv'.format(localtime=au_time), index=False)
            top_person_positive.sort_values(by='sentiment', ascending=False).head(10).to_csv('/home/jupyter/public_outputs/public_top_person_positive_{localtime}_all.csv'.format(localtime=au_time), index=False)
            top_entity_positive.sort_values(by='sentiment', ascending=False).head(10).to_csv('/home/jupyter/public_outputs/public_top_entity_positive_{localtime}_all.csv'.format(localtime=au_time), index=False)
            top_entity_negative.sort_values(by='sentiment', ascending=True).head(10).to_csv('/home/jupyter/public_outputs/public_top_entity_negative_{localtime}_all.csv'.format(localtime=au_time), index=False)
            df_hash_sentiments.to_csv('/home/jupyter/public_outputs/4_public_hashtags_{localtime}_all.csv'.format(localtime=au_time), index=False)
            df_stat.to_csv('/home/jupyter/public_outputs/0_public_stats_{localtime}_all.csv'.format(localtime=au_time), index=False)
            df_sentiment_stat.to_csv('/home/jupyter/public_outputs/1_public_sentiment_breakdown_{localtime}_all.csv'.format(localtime=au_time), index=False)
            public_account_engagement.to_csv('/home/jupyter/public_outputs/3_public_account_engagement_{localtime}_all.csv'.format(localtime=au_time), index=False)
        public_post_with_labels = get_class_csv('public_sentiments.csv', au_time, checkrange, time_from_day, time_to_day)

    #missing post is true
    else:
        if(path.exists('deduplicated_public_missing_posts.csv')):
            missing_posts = [json.loads(json.dumps(d)) for d in csv.DictReader(open('deduplicated_public_missing_posts.csv'))]
            for idx in range(len(missing_posts)):
                print(missing_posts[0].keys())
                missing_posts[idx]['user'] = {}
                if 'account_name' in missing_posts[idx]:
                    missing_posts[idx]['user']['name'] = missing_posts[idx]['account_name']
                elif '\ufeffaccount_name' in missing_posts[idx]:
                    missing_posts[idx]['user']['name'] = missing_posts[idx]['\ufeffaccount_name']
                else:
                    for key in list(missing_posts[idx].keys()):
                        if 'account_name' in key:
                            missing_posts[idx]['user']['name'] = missing_posts[idx][key]
                missing_posts[idx]['posted'] = missing_posts[idx]['post_date']+" +"
                missing_posts[idx]['text'] = missing_posts[idx]['post']
                missing_posts[idx]['postid'] = missing_posts[idx]['post_id']
                missing_posts[idx]['url'] = missing_posts[idx]['post_url']
                missing_posts[idx]['network'] = missing_posts[idx]['source']
                missing_posts[idx]['popularity'] = []
                missing_posts[idx]['popularity']=[{'name':'likes','count':missing_posts[idx]['likes']},{'name':'comments','count':missing_posts[idx]['comments']},{'name':'retweets','count':missing_posts[idx]['retweets']}]
            for data in missing_posts:
                raw_posts.append(data)
            
        # initialize and translate text
        for i in range(len(raw_posts)):
            raw_posts[i]["analysis"] =  {"text_analysis": {"sentiment_analysis":'',
                                             "entity_sentiment_analysis":''}
                                                                            }
            raw_posts[i]["translated_text"] = ''
            #language = raw_posts[i].get("lang", "unknown")
            target = "en"
            translate = translate_text(target, raw_posts[i]["text"])
            raw_posts[i]["translated_text"] = translate["translatedText"]
            #trans_text = translate["translatedText"]
        
        for i in range(len(raw_posts)):
            # add nlp analysis
            text = raw_posts[i]["translated_text"]
            sentiment_result = analyze_text_sentiment(text)# nlp sentiment analysis
            entity_sentiment_result = analyze_entity_sentiment(text)#nlp entity sentiment analysis
            #print(sentiment_result)
            raw_posts[i]["analysis"]["text_analysis"]["sentiment_analysis"]=sentiment_result
            #print(i)
            raw_posts[i]["analysis"]["text_analysis"]["entity_sentiment_analysis"]=entity_sentiment_result
        if path.exists("/home/jupyter/input_files/remove.csv"):
            remove = pd.read_csv("/home/jupyter/input_files/remove.csv")
            remove_ids = []
            posts = []
            for index, row in remove.iterrows():
                if("'" in row['post_id'] and row['post_id'][-1] == "'"):
                    remove_id = row['post_id'].replace("'", "")
                else:
                    remove_id = row['post_id']
                remove_ids.append(remove_id)
            for i in range(len(raw_posts)):
                if(raw_posts[i]['postid'] in remove_ids):
                    continue
                posts.append(raw_posts[i])
        else:
            posts = raw_posts
        hashtag_list = []
        for i in range(len(posts)):
            text_list = posts[i]['text'].split(' ')
            for j in range(len(text_list)):
                if('#' in text_list[j] and text_list[j][0]=='#'):
                    hashtag = text_list[j]
                    hashtag = hashtag.replace(',', '')
                    hashtag = hashtag.lower()
                    if(check_not_exist(hashtag, hashtag_list)):
                        hashtag_list.append({hashtag: {'sentiments':[], 'occurences':0, 'post_ids':[]}})
        for i in range(len(posts)):
            for j in range(len(hashtag_list)):
                for key in hashtag_list[j]:
                    if(key in posts[i]['text'].lower()):
                        print(posts[i])
                        hashtag_list[j][key]['post_ids'].append(posts[i]['postid'])
                        hashtag_list[j][key]['occurences'] += posts[i]['text'].lower().count(key)
                        if('documentSentiment' in posts[i]['analysis']['text_analysis']['sentiment_analysis']):
                            score = posts[i]['analysis']['text_analysis']['sentiment_analysis']['documentSentiment']['score']
                            hashtag_list[j][key]['sentiments'].append(score)
                        else:
                            score = posts[i]['analysis']['text_analysis']['sentiment_analysis']['score']
                            hashtag_list[j][key]['sentiments'].append(score)
        df_hash_sentiments = {'hashtags':[], 'occurences':[], 'average_sentiments':[], 'post_ids':[]}
        for i in range(len(hashtag_list)):
            for key in hashtag_list[i]:
                df_hash_sentiments['hashtags'].append(key)
                df_hash_sentiments['occurences'].append(hashtag_list[i][key]['occurences'])
                df_hash_sentiments['average_sentiments'].append(mean(hashtag_list[i][key]['sentiments']))
                df_hash_sentiments['post_ids'].append(hashtag_list[i][key]['post_ids'])
        df_hash_sentiments = pd.DataFrame(data = df_hash_sentiments)
        df_post, all_entities = initialize_df(posts)
        overall_sentiment, average_sentiment_account = generate_csvs(df_post, checkrange, time_from_day, time_to_day)
        entities_sentiments = entities_sentiments_csv(all_entities)
        top_person_negative = entities_sentiments[(entities_sentiments['type']=="PERSON") & (entities_sentiments['sentiment']<=0)]    
        top_person_positive = entities_sentiments[(entities_sentiments['type']=="PERSON") & (entities_sentiments['sentiment']>0)]    
        top_entity_positive = entities_sentiments.sort_values(by=['sentiment'],ascending=False)
        top_entity_negative = entities_sentiments.sort_values(by=['sentiment'],ascending=True)
        df_post.to_csv('public_sentiments.csv',index=False)
    
        #with open("social_searcher_response.json","r") as f:
            #filter_posts = json.load(f)
        #with open("raw_social_searcher_response.json","r") as f:
            #raw_posts = json.load(f)
        num_return = len(posts)
        num_filter = len(df_post)
        num_facebook = 0
        num_twitter = 0
        num_ins = 0
    
        for index, row in df_post.iterrows():
            if(row['source'] == 'facebook'):
                num_facebook+=1
            elif(row['source'] == 'twitter'):
                num_twitter +=1
            elif(row['source'] == 'instagram'):
                num_ins +=1
        df_stat = {'Sources':[], 'Count':[]}
        df_stat['Sources'].append('Posts founded')
        df_stat['Sources'].append('Posts after filtering (processed)')
        df_stat['Sources'].append('Facebook')
        df_stat['Sources'].append('Twitter')
        df_stat['Sources'].append('Instagram')
        df_stat['Count'].append(num_return)
        df_stat['Count'].append(num_filter)
        df_stat['Count'].append(num_facebook)
        df_stat['Count'].append(num_twitter)
        df_stat['Count'].append(num_ins)
 
        num_neutral = 0
        num_positive = 0
        num_negative = 0
        for index, row in df_post.iterrows():
            if row['account_name'] == 'roycommDVSRC':
                continue
            score = row['score']
            if(score >= 0.2):
                num_positive+=1
            elif score<= -0.2:
                num_negative += 1
            elif -0.2 < score<0.2:
                num_neutral +=1
        df_sentiment_stat = {'Sentiment':[], 'Count':[], 'Percentage':[]}
        df_sentiment_stat['Sentiment'].append('Negative')
        df_sentiment_stat['Sentiment'].append('Positive')
        df_sentiment_stat['Sentiment'].append('Neutral')
        df_sentiment_stat['Count'].append(num_negative)
        df_sentiment_stat['Count'].append(num_positive)
        df_sentiment_stat['Count'].append(num_neutral)
        df_sentiment_stat['Percentage'].append(num_negative/num_filter)
        df_sentiment_stat['Percentage'].append(num_positive/num_filter)
        df_sentiment_stat['Percentage'].append(num_neutral/num_filter)
    
        df_stat = pd.DataFrame(df_stat)
        df_sentiment_stat = pd.DataFrame(df_sentiment_stat)
        public_account_engagement = df_post.groupby(["account_name","source"])['engagement'].sum().reset_index()
    
        if checkrange == True:
            overall_sentiment.to_csv('/home/jupyter/public_outputs/public_overall_sentiment_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom = time_from_day, timeto = time_to_day), index=False)
            average_sentiment_account.to_csv('/home/jupyter/public_outputs/public_average_sentiment_account_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom = time_from_day, timeto = time_to_day), index=False)
            entities_sentiments.to_csv('/home/jupyter/public_outputs/2_public_entities_sentiments_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom = time_from_day, timeto = time_to_day), index=False)
            top_person_negative.sort_values(by='sentiment', ascending=True).head(10).to_csv('/home/jupyter/public_outputs/public_top_person_negative_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom = time_from_day, timeto = time_to_day), index=False)
            top_person_positive.sort_values(by='sentiment', ascending=False).head(10).to_csv('/home/jupyter/public_outputs/public_top_person_positive_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom = time_from_day, timeto = time_to_day), index=False)
            top_entity_positive.sort_values(by='sentiment', ascending=False).head(10).to_csv('/home/jupyter/public_outputs/public_top_entity_positive_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom = time_from_day, timeto = time_to_day), index=False)
            top_entity_negative.sort_values(by='sentiment', ascending=True).head(10).to_csv('/home/jupyter/public_outputs/public_top_entity_negative_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom = time_from_day, timeto = time_to_day), index=False)
            df_hash_sentiments.to_csv('/home/jupyter/public_outputs/4_public_hashtags_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom = time_from_day, timeto = time_to_day), index=False)
            df_stat.to_csv('/home/jupyter/public_outputs/0_public_stats_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom=time_from, timeto=time_to), index=False)
            df_sentiment_stat.to_csv('/home/jupyter/public_outputs/1_public_sentiment_breakdown_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom=time_from, timeto=time_to),index=False)
            public_account_engagement.to_csv('/home/jupyter/public_outputs/3_public_account_engagement_{localtime}_{timefrom}_{timeto}.csv'.format(localtime=au_time, timefrom=time_from, timeto=time_to), index=False)
        else:
            overall_sentiment.to_csv('/home/jupyter/public_outputs/public_overall_sentiment_{localtime}_all.csv'.format(localtime=au_time), index=False)
            average_sentiment_account.to_csv('/home/jupyter/public_outputs/public_average_sentiment_account_{localtime}_all.csv'.format(localtime=au_time), index=False)
            entities_sentiments.to_csv('/home/jupyter/public_outputs/2_public_entities_sentiments_{localtime}_all.csv'.format(localtime=au_time), index=False)
            top_person_negative.sort_values(by='sentiment', ascending=True).head(10).to_csv('/home/jupyter/public_outputs/public_top_person_negative_{localtime}_all.csv'.format(localtime=au_time), index=False)
            top_person_positive.sort_values(by='sentiment', ascending=False).head(10).to_csv('/home/jupyter/public_outputs/public_top_person_positive_{localtime}_all.csv'.format(localtime=au_time), index=False)
            top_entity_positive.sort_values(by='sentiment', ascending=True).head(10).to_csv('/home/jupyter/public_outputs/public_top_entity_positive_{localtime}_all.csv'.format(localtime=au_time), index=False)
            top_entity_negative.sort_values(by='sentiment', ascending=False).head(10).to_csv('/home/jupyter/public_outputs/public_top_entity_negative_{localtime}_all.csv'.format(localtime=au_time), index=False)
            df_hash_sentiments.to_csv('/home/jupyter/public_outputs/4_public_hashtags_{localtime}_all.csv'.format(localtime=au_time), index=False)
            df_stat.to_csv('/home/jupyter/public_outputs/0_public_stats_{localtime}_all.csv'.format(localtime=au_time), index=False)
            df_sentiment_stat.to_csv('/home/jupyter/public_outputs/1_public_sentiment_breakdown_{localtime}_all.csv'.format(localtime=au_time), index=False)
            public_account_engagement.to_csv('/home/jupyter/public_outputs/3_public_account_engagement_{localtime}_all.csv'.format(localtime=au_time), index=False)
        public_post_with_labels = get_class_csv('public_sentiments.csv', au_time, checkrange, time_from_day, time_to_day)
    #ALL_posts
    
    
    print(type(df_stat))
    influencer_df_stat, influencer_df_sentiment_stat, influencer_new_df_es, influencer_grouped_sent, influencer_df_hash_sentiments, influencer_post_with_labels
    # 0
    for index, row in df_stat.iterrows():
        for index_2, row_2 in influencer_df_stat.iterrows():
            if(row_2['Sources'] == row['Sources']):
                df_stat.at[index, 'Count'] = row['Count'] + row_2['Count']
    # 1
    count = df_sentiment_stat['Count'].sum() + influencer_df_sentiment_stat['Count'].sum()
    negative = df_sentiment_stat.loc[df_sentiment_stat['Sentiment']=='Negative']['Count'] + influencer_df_sentiment_stat.loc[influencer_df_sentiment_stat['Sentiment']=='Negative']['Count']
    positive = df_sentiment_stat.loc[df_sentiment_stat['Sentiment']=='Positive']['Count'] + influencer_df_sentiment_stat.loc[influencer_df_sentiment_stat['Sentiment']=='Positive']['Count']
    neutral = df_sentiment_stat.loc[df_sentiment_stat['Sentiment']=='Neutral']['Count'] + influencer_df_sentiment_stat.loc[influencer_df_sentiment_stat['Sentiment']=='Neutral']['Count']
    
    for index, row in df_sentiment_stat.iterrows():
        if row['Sentiment'] == 'Negative':
            df_sentiment_stat.at[index, 'Count']= negative
            df_sentiment_stat.at[index, 'Percentage']= negative/count
        elif row['Sentiment'] == 'Positive':
            df_sentiment_stat.at[index, 'Count']= positive
            df_sentiment_stat.at[index, 'Percentage']= positive/count
        elif row['Sentiment'] == 'Neutral':
            df_sentiment_stat.at[index, 'Count']= neutral
            df_sentiment_stat.at[index, 'Percentage']= neutral/count\
    # 2
    for index, row in entities_sentiments.iterrows():
        for index_2, row_2 in influencer_new_df_es.iterrows():
            if(row['entity']==row_2['entity'] and row['type'] == row_2['type']):
                salience_1 = row['salience'] * row['occurence']
                salience_2 = row_2['salience'] * row_2['occurence']
                salience = (salience_1+salience_2)/(row['occurence']+row_2['occurence'])
                sentiment_1 = row['sentiment'] * row['occurence']
                sentiment_2 = row_2['sentiment'] * row_2['occurence']
                sentiment = (sentiment_1+sentiment_2)/(row['occurence']+row_2['occurence'])
                if(row_2['min_sentiment']<row['min_sentiment']):
                    entities_sentiments.at[index, 'min_sentiment'] = row_2['min_sentiment']
                if(row_2['max_sentiment']>row['max_sentiment']):
                    entities_sentiments.at[index, 'max_sentiment'] = row_2['max_sentiment']
                entities_sentiments.at[index, 'salience'] = salience
                entities_sentiments.at[index, 'sentiment'] = sentiment
                entities_sentiments.at[index, 'occurence'] = row['occurence']+row_2['occurence']
                ids_list = row['posts_ids'] + row_2['post_ids']
                #remove duplicates
                ids_list = list(dict.fromkeys(ids_list))
                entities_sentiments.at[index, 'posts_ids'] = ids_list
    for index_2, row_2 in influencer_new_df_es.iterrows():
        num = 0
        for index, row in entities_sentiments.iterrows():
            if(row['entity']==row_2['entity'] and row['type'] == row_2['type']):
                num = num + 1
        if num == 0:
            entities_sentiments = entities_sentiments.append({'entity':row_2['entity'], 'type':row_2['type'], 'salience':row_2['salience'], 'sentiment':row_2['sentiment'], 'min_sentiment':row_2['min_sentiment'], 'max_sentiment':row_2['max_sentiment'], 'occurence':row_2['occurence'], 'posts_ids':row_2['post_ids']},ignore_index=True)
    
    #3
    for index, row in public_account_engagement.iterrows():
        for index_2, row_2 in influencer_grouped_sent.iterrows():
            if(row['account_name']==row_2['account_name'] and row['source'] == row_2['source']):
                entities_sentiments.at[index, 'engagement'] = row['engagement'] + row_2['engagement']
    for index_2, row_2 in influencer_grouped_sent.iterrows():
        num = 0
        for index, row in public_account_engagement.iterrows():
            if(row['account_name']==row_2['account_name'] and row['source'] == row_2['source']):
                num = num + 1
        if num == 0:
            public_account_engagement = public_account_engagement.append({'account_name':row_2['account_name'],'source':row_2['source'],'engagement':row_2['engagement']},ignore_index=True)
    
    #4
    for index, row in df_hash_sentiments.iterrows():
        for index_2, row_2 in influencer_df_hash_sentiments.iterrows():
            if(row['hashtags']==row_2['hashtags']):
                df_hash_sentiments.at[index, 'occurences'] = row['occurences'] + row_2['occurences']
                sentiment_1 = row['average_sentiments'] * row['occurences']
                sentiment_2 = row_2['average_sentiments'] * row_2['occurences']
                sentiment = (sentiment_1+sentiment_2)/(row['occurences'] + row_2['occurences'])
                ids_list = row['post_ids'] + row_2['post_ids']
                #remove duplicates
                ids_list = list(dict.fromkeys(ids_list))
                df_hash_sentiments.at[index, 'post_ids'] = ids_list
                df_hash_sentiments.at[index, 'average_sentiments'] = sentiment
    for index_2, row_2 in influencer_df_hash_sentiments.iterrows():
        num = 0
        for index, row in df_hash_sentiments.iterrows():
            if(row['hashtags']==row_2['hashtags']):
                num = num + 1
        if num == 0:
            df_hash_sentiments = df_hash_sentiments.append({'hashtags':row_2['hashtags'],'occurences':row_2['occurences'],'average_sentiments':row_2['average_sentiments'],'post_ids':row_2['post_ids']},ignore_index=True)
    #5
    all_post = pd.concat([public_post_with_labels, influencer_post_with_labels], ignore_index=True)
    df_stat.to_csv('/home/jupyter/All/0_ALL_stats_{timefrom}-{timeto}.csv'.format(timefrom=time_from_day, timeto=time_to_day), index=False) #public stat
    df_sentiment_stat.to_csv('/home/jupyter/All/1_ALL_sentiment_breakdown_{timefrom}-{timeto}.csv'.format(timefrom=time_from_day, timeto=time_to_day), index=False) # public_sent
    entities_sentiments.to_csv('/home/jupyter/All/2_ALL_entities_sentiments_{timefrom}-{timeto}.csv'.format(timefrom=time_from_day, timeto=time_to_day), index=False) #public_es
    public_account_engagement.to_csv('/home/jupyter/All/3_ALL_account_engagement_{timefrom}-{timeto}.csv'.format(timefrom=time_from_day, timeto=time_to_day), index=False) # public_enga
    df_hash_sentiments.to_csv('/home/jupyter/All/4_ALL_hashtags_{timefrom}-{timeto}.csv'.format(timefrom=time_from_day, timeto=time_to_day), index=False) # public_hash
    all_post.to_csv('/home/jupyter/All/5_ALL_posts_with_labels_{timefrom}-{timeto}.csv'.format(timefrom=time_from_day, timeto=time_to_day), index=False) 
    