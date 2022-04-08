import requests
from datetime import datetime
import dateutil.parser
import pytz
import json
import re
from tweepy_function import *
import tweepy

def get_twitter_response(query):
    response = {}
    params = get_query_params(query)
    url = query.get("url", "")
    #print(params)
    response = requests.get(url, auth=bearer_oauth, params=params)
    errors = response.json().get("errors",[])
    if errors:
        print (f"Errors occurred, error message: {response.text}")
        return []
    print(len(response.json().get("data", [])))
    return response.json().get("data", [])

def get_query_params(query):
    params = {
		"tweet.fields": "attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld",
		"expansions":   "attachments.poll_ids,attachments.media_keys,author_id,geo.place_id,in_reply_to_user_id,referenced_tweets.id,entities.mentions.username,referenced_tweets.id.author_id",
		"media.fields": "duration_ms,height,media_key,preview_image_url,public_metrics,type,url,width",
		"place.fields": "contained_within,country,country_code,full_name,geo,id,name,place_type",
		"poll.fields":  "duration_minutes,end_datetime,id,options,voting_status",
		"user.fields":  "created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld",
	}
  
    for key in query.keys():
        if not query[key]:
            continue
        elif key == "url":
            continue
        params[key] = query[key]
    return params


def bearer_oauth(r):
    """ 
    Method required by bearer token authentication.
    """
    Bearer_token = "AAAAAAAAAAAAAAAAAAAAAAIrNQEAAAAAWn2DbTwOMcpacU1bDcQkSF7pnms%3DUvTFWyHofVMiVq2blnRyWlefRAvQf0d5jJTTkb0wcYr7cDq1ru"
    r.headers["Authorization"] = f"Bearer {Bearer_token}"
    r.headers["User-Agent"] = "v2UserTweetsPython"
    return r

def bearer_oauth_11(r):
    """
    Method required by bearer token authentication.
    """
    bearer_token = "AAAAAAAAAAAAAAAAAAAAAAIrNQEAAAAAWn2DbTwOMcpacU1bDcQkSF7pnms%3DUvTFWyHofVMiVq2blnRyWlefRAvQf0d5jJTTkb0wcYr7cDq1ru"
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v1UserTweetsPython"
    return r
def get_tweet_11(tweet_id):
    url = "https://api.twitter.com/1.1/statuses/show.json"
    params = {
        "id": tweet_id
    }
    response = requests.get(url, auth=bearer_oauth_11, params=params)
    if response.status_code != 200:
        print(f"Invalid Search Response Code: {response.status_code}")
        return []
    response_json = response.json()
    return response_json    


def get_filter_key():
    with open('/home/jupyter/public_filters.json', 'r') as f:
        filters = json.load(f)
    return filters

def filter_key_posts(posts, filter_keys):
    num = 0
    if(len(filter_keys['filter_keys'])==0):
        num+=1
    else:
        for idx in range(len(posts)-1,-1,-1):
            for key in filter_keys["filter_keys"]:
                if key.lower() in posts[idx].get("text","").lower():
                    posts.pop(idx)
    if posts:
        print('return posts')
        return posts
    else:
        print("All posts are filtered out.")
        return []


def filter_time_posts(posts, filter_keys):
    if(filter_keys["time_from"]=="" and filter_keys["time_to"]!=""):
        end_time = datetime.strptime(filter_keys["time_to"], "%Y-%m-%d %H:%M:%S")
        for idx in range(len(posts)-1,-1,-1):
            posted = posts[idx]["created_at"].split("T")[0] + " " + posts[idx]["created_at"].split("T")[1].split(".")[0]
            posted_local = dateutil.parser.parse(posted).astimezone(pytz.timezone('Australia/Sydney'))
            posted_local = datetime.strftime(posted_local, "%Y-%m-%d %H:%M:%S")
            posted_datetime = datetime.strptime(posted_local, "%Y-%m-%d %H:%M:%S")
            if posted_datetime <= end_time:
                posts[idx]["posted"] = posted
                posts[idx]["network"] = "twitter"
                tweet_id = posts[idx]["id"]
                posts[idx]["postid"] = tweet_id
            
                posts[idx]["popularity"] = [
                    {"name":"retweets", "count": posts[idx].get("public_metrics", {}).get("retweet_count", 0)},
                    {"name":"likes", "count": posts[idx].get("public_metrics", {}).get("like_count", 0)}
                ]
                posts[idx]["user"] = {}
                posts[idx]["user"]["userid"] = posts[idx]["author_id"]
                username = get_user_username(posts[idx]["user"]["userid"])
                posts[idx]["user"]["name"]  = username
                url="https://twitter.com/{}/status/{}".format(username, tweet_id )
                posts[idx]["url"] = url
                continue
            else:
                posts.pop(idx)
    
        if posts:
            print('return posts')
            return posts
        else:
            print("All posts are out of date.")
            return []
    elif(filter_keys["time_from"]!="" and filter_keys["time_to"]==""):
        start_time = datetime.strptime(filter_keys["time_from"], "%Y-%m-%d %H:%M:%S")
        for idx in range(len(posts)-1,-1,-1):
            posted = posts[idx]["created_at"].split("T")[0] + " " + posts[idx]["created_at"].split("T")[1].split(".")[0]
            posted_local = dateutil.parser.parse(posted).astimezone(pytz.timezone('Australia/Sydney'))
            posted_local = datetime.strftime(posted_local, "%Y-%m-%d %H:%M:%S")
            posted_datetime = datetime.strptime(posted_local, "%Y-%m-%d %H:%M:%S")
            if start_time <= posted_datetime:
                posts[idx]["posted"] = posted
                posts[idx]["network"] = "twitter"
                tweet_id = posts[idx]["id"]
                posts[idx]["postid"] = tweet_id
            
                posts[idx]["popularity"] = [
                    {"name":"retweets", "count": posts[idx].get("public_metrics", {}).get("retweet_count", 0)},
                    {"name":"likes", "count": posts[idx].get("public_metrics", {}).get("like_count", 0)}
                ]
                posts[idx]["user"] = {}
                posts[idx]["user"]["userid"] = posts[idx]["author_id"]
                username = get_user_username(posts[idx]["user"]["userid"])
                posts[idx]["user"]["name"]  = username
                url="https://twitter.com/{}/status/{}".format(username, tweet_id )
                posts[idx]["url"] = url
                continue
            else:
                posts.pop(idx)
    
        if posts:
            print('return posts')
            return posts
        else:
            print("All posts are out of date.")
            return []
    elif(filter_keys["time_from"]!="" and filter_keys["time_to"]!=""):
        start_time = datetime.strptime(filter_keys["time_from"], "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(filter_keys["time_to"], "%Y-%m-%d %H:%M:%S")
        for idx in range(len(posts)-1,-1,-1):
            posted = posts[idx]["created_at"].split("T")[0] + " " + posts[idx]["created_at"].split("T")[1].split(".")[0]
            posted_local = dateutil.parser.parse(posted).astimezone(pytz.timezone('Australia/Sydney'))
            posted_local = datetime.strftime(posted_local, "%Y-%m-%d %H:%M:%S")
            posted_datetime = datetime.strptime(posted_local, "%Y-%m-%d %H:%M:%S")
            if start_time <= posted_datetime <= end_time:
                posts[idx]["posted"] = posted
                posts[idx]["network"] = "twitter"
                tweet_id = posts[idx]["id"]
                posts[idx]["postid"] = tweet_id
            
                posts[idx]["popularity"] = [
                    {"name":"retweets", "count": posts[idx].get("public_metrics", {}).get("retweet_count", 0)},
                    {"name":"likes", "count": posts[idx].get("public_metrics", {}).get("like_count", 0)}
                ]
                posts[idx]["user"] = {}
                posts[idx]["user"]["userid"] = posts[idx]["author_id"]
                username = get_user_username(posts[idx]["user"]["userid"])
                posts[idx]["user"]["name"]  = username
                url="https://twitter.com/{}/status/{}".format(username, tweet_id )
                posts[idx]["url"] = url
                continue
            else:
                posts.pop(idx)
    
        if posts:
            print('return posts')
            return posts
        else:
            print("All posts are out of date.")
            return []
    elif(filter_keys["time_from"]=="" and filter_keys["time_to"]==""):
        if posts:
            print('return posts')
            return posts
        else:
            print("All posts are out of date.")
            return []

        
        
if __name__ == "__main__":
    queries0 = [
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("DVSRC")',
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("RCDVS")',
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("Royal Commission" "veteran" "suicide")',
            "max_results": 100
        },
    ]
    queries = [
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("DVSRC")',
            "start_time":"2022-04-01T00:00:00Z",
            "end_time":"2022-04-01T23:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("RCDVS")',
            "start_time":"2022-04-01T00:00:00Z",
            "end_time":"2022-04-01T23:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("Royal Commission" "veteran" "suicide")',
            "start_time":"2022-04-01T00:00:00Z",
            "end_time":"2022-04-01T23:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("DVSRC")',
            "start_time":"2022-04-02T00:00:00Z",
            "end_time":"2022-04-02T23:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("RCDVS")',
            "start_time":"2022-04-02T00:00:00Z",
            "end_time":"2022-04-02T23:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("Royal Commission" "veteran" "suicide")',
            "start_time":"2022-04-02T00:00:00Z",
            "end_time":"2022-04-02T23:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("DVSRC")',
            "start_time":"2022-04-03T00:00:00Z",
            "end_time":"2022-04-03T23:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("RCDVS")',
            "start_time":"2022-04-03T00:00:00Z",
            "end_time":"2022-04-03T23:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("Royal Commission" "veteran" "suicide")',
            "start_time":"2022-04-03T00:00:00Z",
            "end_time":"2022-04-03T23:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("DVSRC")',
            "start_time":"2022-04-04T00:00:00Z",
            "end_time":"2022-04-04T23:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("RCDVS")',
            "start_time":"2022-04-04T00:00:00Z",
            "end_time":"2022-04-04T23:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("Royal Commission" "veteran" "suicide")',
            "start_time":"2022-04-04T00:00:00Z",
            "end_time":"2022-04-04T23:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("DVSRC")',
            "start_time":"2022-04-05T00:00:00Z",
            "end_time":"2022-04-05T23:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("RCDVS")',
            "start_time":"2022-04-05T00:00:00Z",
            "end_time":"2022-04-05T23:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("Royal Commission" "veteran" "suicide")',
            "start_time":"2022-04-05T00:00:00Z",
            "end_time":"2022-04-05T23:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("DVSRC")',
            "start_time":"2022-04-06T00:00:00Z",
            "end_time":"2022-04-06T23:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("RCDVS")',
            "start_time":"2022-04-06T00:00:00Z",
            "end_time":"2022-04-06T23:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("Royal Commission" "veteran" "suicide")',
            "start_time":"2022-04-06T00:00:00Z",
            "end_time":"2022-04-06T23:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("DVSRC")',
            "start_time":"2022-04-07T00:00:00Z",
            "end_time":"2022-04-07T06:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("RCDVS")',
            "start_time":"2022-04-07T00:00:00Z",
            "end_time":"2022-04-07T06:59:59Z",
            "max_results": 100
        },
        {
            "url": "https://api.twitter.com/2/tweets/search/recent",
            "query": '("Royal Commission" "veteran" "suicide")',
            "start_time":"2022-04-07T00:00:00Z",
            "end_time":"2022-04-07T06:59:59Z",
            "max_results": 100
        }
    ]
    posts = []
    for query in queries:
        posts += get_twitter_response(query)
    file_name = "twitter_response.json"
    with open('/home/jupyter/{filename}'.format(filename=file_name),"w") as f:
        json.dump(posts,f,indent = 4)
        

    if not posts:
        print ("No results returned")
        
    filter_keys = get_filter_key()
    posts = filter_key_posts(posts, filter_keys)
    posts = filter_time_posts(posts, filter_keys)

    file_name = "social_searcher_response.json"
    with open('/home/jupyter/{filename}'.format(filename=file_name),"w") as f:
        json.dump(posts,f,indent = 4)
