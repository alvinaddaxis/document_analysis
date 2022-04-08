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
import pytz
import six
import sys
from statistics import mode
from statistics import mean
from time import sleep

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
    for prediction in predictions:
        print(" prediction:", dict(prediction))
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
    

    
def get_class():
    new_df = {'account_name':[], 'source':[],'post_date':[],'post_id':[], 'post':[], 'score':[], 'magnitude':[], 'likes':[], 'comments':[], 'retweets/shares':[],'engagement':[], 'post_url':[], "non_temporal_label":[], "non_temporal_label_confidence":[], "temporal_label":[], "temporal_label_confidence":[]}
    df_nontemp = {"account_name":[], "source":[], "post_date":[], "post_id":[], "post":[], "Culture":[], "Access_to_information":[], "Hypermasculinity":[], "Lived_experience":[], "Training":[], "Bullying":[], "Tribalism":[]}
    df_temp = {"account_name":[], "source":[], "post_date":[], "post_id":[], "post":[], "Transition":[], "Service_deployment":[], "Separation":[], "Post_service_issues":[], "Service_training":[], "Pre_service":[]}
    df = pd.read_csv('sentiments.csv')
    for index, row in df.iterrows():
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
    new_df.to_csv("posts_with_labels.csv", index=False)
    df_nontemp.to_csv("non_temporal_labels.csv", index=False)
    df_temp.to_csv("temporal_labels.csv", index=False)
    
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

def process_df(str_lower_date,str_higher_date, checkrange, beforeyesterday,yesterday, weekday):
    #x = predict_text_classification_single_label(project="test-stuff-292200",endpoint_id="5404926483263127552",location="us-central1",content="hello, my friends")
    #print(x)
    f = open('influencer_filter.json')
    influencer_filter = json.load(f)
    #if filter_keys in influencer_filter:
    if 'time_from' in influencer_filter:
        time_from = influencer_filter['time_from']
    if 'time_to' in influencer_filter:
        time_to = influencer_filter['time_to']
    if 'filter_keys' in influencer_filter:
        filter_keys = influencer_filter['filter_keys']
    
    before_list = [beforeyesterday, yesterday]
    id_list = [] # no id
    name_list = [] # one user has multiple post_ids
    source_list = [] # postid对应的来源
    entity_sentiments_list = [] # entity对应的多个salience和多个sentiments
    engage_list = [] #
    #name_date_list = []
    date_list = [] # need
    file = get_raw_data()
    df = file.drop_duplicates(subset=['post_id', 'data_type'])
    df.to_csv('raw_data.csv', index=False)
    #df = pd.read_csv('input.csv')
    dict_data = {"list":[]}
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
    
    # 初始化以上list
    for index, row in df.iterrows():
        json_object = json.loads(row['data_json'])
        if(row['data_type']=='API-response'):
            if('userid' in json_object['user']):
                if(check_not_exist(json_object['user']['userid'], id_list)):
                    id_list.append({json_object['user']['userid']:[]})
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
            if(check_not_exist(json_object['postid'], source_list)):
                source_list.append({json_object['postid']: row['source']})
    for index, row in df.iterrows():
        json_object = json.loads(row['data_json'])
        if(row['data_type'] == 'NLP-analyze-entity-sentiment'):
            for j in range(len(json_object['entities'])):
                entity_name = json_object['entities'][j]['name']
                if(check_not_exist(entity_name, entity_sentiments_list)):
                    entity_sentiments_list.append({entity_name:{'sentiments':[], 'salience':[], 'post_ids':[], 'number':[], 'type':[]}})
    for index, row in df.iterrows():
        json_object = json.loads(row['data_json'])
        if(row['data_type'] == 'NLP-analyze-entity-sentiment'):
            for j in range(len(json_object['entities'])):
                for k in range(len(entity_sentiments_list)):
                    name = json_object['entities'][j]['name']
                    if(json_object['entities'][j]['name'] in entity_sentiments_list[k]):
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
    for i in range(len(entity_sentiments_list)):
        
        for key in entity_sentiments_list[i]:
            typeof = ''
            #mode_type = mode(entity_sentiments_list[i][key]['type'])
            mode_type = max(set(entity_sentiments_list[i][key]['type']), key=entity_sentiments_list[i][key]['type'].count)
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
            df_entity_sentiments_occ['salience'].append(mean(entity_sentiments_list[i][key]['salience']))
            df_entity_sentiments_occ['sentiment'].append(mean(entity_sentiments_list[i][key]['sentiments']))
            df_entity_sentiments_occ['min_sentiment'].append(min(entity_sentiments_list[i][key]['sentiments']))
            df_entity_sentiments_occ['max_sentiment'].append(max(entity_sentiments_list[i][key]['sentiments']))
            df_entity_sentiments_occ['occurence'].append(sum(entity_sentiments_list[i][key]['number']))
            df_entity_sentiments_occ['post_ids'].append(entity_sentiments_list[i][key]['post_ids'])
            
    #初始化结束
    
    for index, row in df.iterrows():
        #print(row['data_type'])
        json_object = json.loads(row['data_json'])
#        if(row['post_id']=='https://www.facebook.com/TheProjectTV/videos/royal-commission-into-veteran-suicide-announced/463610018255843/'):
            #print(json_object)
#        json_object = json.loads(row['data_json'])
#            if(json_object['user']['name'] == 'www.facebook.com'):
#            uid = json_object['user']['userid']
#                print(json_object)
#            if(uid not in id_list):
#                id_list.append({uid:[]})
        
        #name_list = []
        
        #转换post time 成澳洲当地时间
        post_id = row['post_id']
        post_date = ''
        for i in range(len(date_list)):
            if(post_id in date_list[i]):
                post_date = date_list[i][post_id]
       # print(date_list)
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
                        continue
                else:                
                    if(local_date == yesterday):
                        post_date = local_str
                    else:
                        continue
            else:
                current_date = datetime.date(int(local_date.split('-')[0]), int(local_date.split('-')[1]), int(local_date.split('-')[2]))
                #print(current_date)
                #print(type(current_date))
                lower_date = datetime.date(int(str_lower_date.split('-')[0]), int(str_lower_date.split('-')[1]), int(str_lower_date.split('-')[2]))
                higher_date = datetime.date(int(str_higher_date.split('-')[0]), int(str_higher_date.split('-')[1]), int(str_higher_date.split('-')[2]))
                if(current_date <= higher_date and current_date >= lower_date):
                    post_date = local_date
                else:
                    continue
        
        user_id = ''
        user_name = ''
        source = ''
        
        if(row['data_type'] == 'NLP-analyze-sentiment'):
            print('hello')
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
                    if(row['post_id'] in source_list[j]):
                        source = source_list[j][row['post_id']]
                        break
                df_sentiments['source'].append(source)
                #print(json_object['url'])
                for j in range(len(engage_list)):
                    if(row['post_id'] in engage_list[j]):     
                        df_sentiments['post_url'].append(engage_list[j][row['post_id']]['url'])
                        df_sentiments['likes'].append(engage_list[j][row['post_id']]['likes'])
                        df_sentiments['comments'].append(engage_list[j][row['post_id']]['comments'])
                        df_sentiments['retweets/shares'].append(engage_list[j][row['post_id']]['retweets'])
                        df_sentiments['engagement'].append(engage_list[j][row['post_id']]['engagement'])
                
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
                    if(row['post_id'] in engage_list[j]):     
                        df_sentiments['post_url'].append(engage_list[j][row['post_id']]['url'])
                        df_sentiments['likes'].append(engage_list[j][row['post_id']]['likes'])
                        df_sentiments['comments'].append(engage_list[j][row['post_id']]['comments'])
                        df_sentiments['retweets/shares'].append(engage_list[j][row['post_id']]['retweets'])
                        df_sentiments['engagement'].append(engage_list[j][row['post_id']]['engagement'])
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
    new_df_es.to_csv('entities_sentiments.csv', index=False)
    new_df_sent =pd.DataFrame(data=df_sentiments)
    #print(new_df_sent['account_name'])
    new_df_sent.to_csv('sentiments.csv', index=False)
    new_df_e = pd.DataFrame(data=df_entities)
    #new_df_e.to_csv('entities.csv', index=False)
    person = new_df_es.loc[new_df_es['type'] == 'PERSON']
    entity = new_df_es.loc[new_df_es['type'] != 'PERSON']
    entity.sort_values(by='sentiment', ascending=False).head(10).to_csv('top_entity_positive.csv', index=False)
    entity.sort_values(by='sentiment', ascending=True).head(10).to_csv('top_entity_negative.csv', index=False)        
    person.sort_values(by='sentiment', ascending=False).head(10).to_csv('top_person_positive.csv', index=False)
    person.sort_values(by='sentiment', ascending=True).head(10).to_csv('top_person_negative.csv', index=False)
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
    average_sentiment_df.to_csv('average_sentiment_account.csv', index=False)
    overall_sentiment_df = {'post_date':[],'overall_sentiment':[]}
    overall_score = new_df_sent.mean()['score']
    if(checkrange == False):
        if(weekday == 0):
            overall_sentiment_df['post_date'].append(beforeyesterday+ '-' +yesterday)
        else:
            overall_sentiment_df['post_date'].append(yesterday)
    else:
        overall_sentiment_df['post_date'].append(str_lower_date + '-' + str_higher_date)
    overall_sentiment_df['overall_sentiment'].append(overall_score)
    overall_sentiment_df=pd.DataFrame(data=overall_sentiment_df)
    overall_sentiment_df.to_csv('overall_sentiment.csv', index=False)

    #print(yesterday)
    
if __name__ == "__main__":
    australia = pytz.timezone('Australia/Sydney')
    au_time = datetime.datetime.now(australia)
    au_time = str(au_time)
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
    checkrange = False
    str_lower_date = ''
    str_higher_date = ''
    if(len(sys.argv)>1):
        str_lower_date = sys.argv[1]
        str_higher_date = sys.argv[2]
        checkrange = True
    process_df(str_lower_date,str_higher_date, checkrange, beforeyesterday,yesterday, weekday)
    sleep(1)
    get_class()
    #print(weekday)