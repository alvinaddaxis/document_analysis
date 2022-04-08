from google.cloud import bigquery_storage
from google.cloud.bigquery_storage import types
from google.cloud import bigquery
import pandas as pd
import json
import os.path
from os import path
import numpy as np
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

def preprocess():
    df_influencer_bq = get_raw_data()
    df_influencer_bq = df_influencer_bq.drop_duplicates(subset=['post_id', 'data_type'])
    #df_influencer_bq = df_influencer_bq.drop_duplicates(subset=[''])
    df_influencer_bq_ids = []
    if path.exists('/home/jupyter/input_files/influencer_missing_posts.csv'):
        df_influencer_miss = pd.read_csv("/home/jupyter/input_files/influencer_missing_posts.csv", encoding= 'unicode_escape')
        df_influencer_miss = df_influencer_miss.drop_duplicates(subset=['post_id'])
        df_influencer_miss = df_influencer_miss.dropna(axis = 0, how = 'all')
        df_influencer_miss_ids = []
        for index, row in df_influencer_miss.iterrows():
            print(row['post_id'])
            if "'" in str(row['post_id']):
                row['post_id'] = row['post_id'].replace("'", '')
        for index, row in df_influencer_miss.iterrows():
            df_influencer_miss_ids.append(str(row['post_id']).replace("'", ""))
        for index, row in df_influencer_bq.iterrows():
            if(row['data_type']=="API-response"):
                df_influencer_bq_ids.append(row['post_id'])
        for i in range(len(df_influencer_miss_ids)):
            for j in range(len(df_influencer_bq_ids)):
                if(df_influencer_miss_ids[i] == df_influencer_bq_ids[j]):
                    x = df_influencer_miss[(df_influencer_miss['post_id'] == df_influencer_miss_ids[i]+"'")].index
                    df_influencer_miss = df_influencer_miss.drop(x)
        
        df_influencer_miss.to_csv("deduplicated_influencer_missing_posts.csv", index=False)

    df_influencer_bq.to_csv("deduplicated_influencer_posts.csv", index=False)
    #------------------public----------------------------------
    with open("social_searcher_response.json","r") as f:
        posts = json.load(f)
    result = []
    seen = set()
    for items in posts:
        key = items['postid']
        if key in seen:
            continue
        result.append(items)
        seen.add(key)
    print(len(result))
    final_result = []
    final_seen = set()
    for items in result:
        key = items['text']
        if key in final_seen:
            continue
        final_result.append(items)
        final_seen.add(key)
    print(len(final_result))
    final_final_result = []
    for items in final_result:
        text = items['text'].strip()
        stay_flag = True
        if 'RT' in text and text.find('RT') < 5:
            original_text = ':'.join(text.split(':')[1:])
            print(original_text)
            for other_items in final_result:
                if other_items['postid'] == items['postid']:
                    continue
                if other_items['text'].strip() == original_text.strip():
                    stay_flag = False
            if stay_flag:
                final_final_result.append(items)
        else:
            final_final_result.append(items)
    if path.exists('/home/jupyter/input_files/public_missing_posts.csv'):
        df_public_missing = pd.read_csv("/home/jupyter/input_files/public_missing_posts.csv", encoding= 'unicode_escape')
        df_public_missing = df_public_missing.drop_duplicates(subset=['post_id'])
        df_public_missing = df_public_missing.drop_duplicates(subset=['post'])
        #df_public_missing['post_id'][7] = '1499335561324396548'
        df_public_miss_ids = []
        df_public_post_ids = []
        for index, row in df_public_missing.iterrows():
            print(type(row['post_id']))
            if isinstance(row['post_id'], float):
                df_public_missing = df_public_missing.drop(index)
            else:
                if "'" in row['post_id']:
                    row['post_id'] = row['post_id'].replace("'", '')
        for index, row in df_public_missing.iterrows():
            print(type(row['post_id']))
            postid = str(row['post_id'])
            df_public_miss_ids.append(postid.replace("'", ""))
        for i in range(len(result)):
            df_public_post_ids.append(result[i]['postid'])
        for i in range(len(df_public_miss_ids)):
            for j in range(len(df_public_post_ids)):
                if(df_public_miss_ids[i] == df_public_post_ids[j]):
                    x = df_public_missing[(df_public_missing['post_id'] == df_public_miss_ids[i]+"'")].index
                    df_public_missing = df_public_missing.drop(x)
    #print(df_public_post_ids)
    #print(df_public_miss_ids)
        df_public_missing.to_csv("deduplicated_public_missing_posts.csv", index=False)
    with open("deduplicated_social_searcher_response.json", "w") as f:
        json.dump(final_final_result, f, indent=4)
        
    
if __name__ == "__main__":
    preprocess()
    