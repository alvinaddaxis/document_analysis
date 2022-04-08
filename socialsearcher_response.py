import requests
from datetime import datetime
import dateutil.parser
import pytz
import json

def get_api_response(url_list):
    
    posts = []

    for url in url_list:

        response = requests.get( url )
        url_posts = response.json().get("posts", [])
        if not url_posts:
            print(url)
        else:
            print(len(url_posts))
            posts += url_posts
    #print(len(posts))
    return posts

def get_filter_key():
    with open('/home/jupyter/public_filters.json', 'r') as f:
        filters = json.load(f)
    return filters

def filter_time_posts(posts, filter_keys):
    if(filter_keys["time_from"]=="" and filter_keys["time_to"]!=""):
        end_time = datetime.strptime(filter_keys["time_to"], "%Y-%m-%d %H:%M:%S")
        for idx in range(len(posts)-1,-1,-1):
            posted = posts[idx]["posted"].split(" ")[0] + " " + posts[idx]["posted"].split(" ")[1]
            posted_local = dateutil.parser.parse(posted).astimezone(pytz.timezone('Australia/Sydney'))
            posted_local = datetime.strftime(posted_local, "%Y-%m-%d %H:%M:%S")
            posted_datetime = datetime.strptime(posted_local, "%Y-%m-%d %H:%M:%S")
            if posted_datetime <= end_time:
                posts[idx]["posted"] = posted
                continue
            else:
                posts.pop(idx)
    
        if posts:
            print('return posts')
            return posts
        else:
            print("All posts are filtered out.")
            return []
    elif(filter_keys["time_from"]!="" and filter_keys["time_to"]==""):
        start_time = datetime.strptime(filter_keys["time_from"], "%Y-%m-%d %H:%M:%S")
        for idx in range(len(posts)-1,-1,-1):
            posted = posted = posts[idx]["posted"].split(" ")[0] + " " + posts[idx]["posted"].split(" ")[1]
            posted_local = dateutil.parser.parse(posted).astimezone(pytz.timezone('Australia/Sydney'))
            posted_local = datetime.strftime(posted_local, "%Y-%m-%d %H:%M:%S")
            posted_datetime = datetime.strptime(posted_local, "%Y-%m-%d %H:%M:%S")
            if start_time <= posted_datetime:
                posts[idx]["posted"] = posted
                continue
            else:
                posts.pop(idx)
    
        if posts:
            print('return posts')
            return posts
        else:
            print("All posts are filtered out.")
            return []
    elif(filter_keys["time_from"]!="" and filter_keys["time_to"]!=""):
        start_time = datetime.strptime(filter_keys["time_from"], "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(filter_keys["time_to"], "%Y-%m-%d %H:%M:%S")
        for idx in range(len(posts)-1,-1,-1):
            posted = posted = posts[idx]["posted"].split(" ")[0] + " " + posts[idx]["posted"].split(" ")[1]
            posted_local = dateutil.parser.parse(posted).astimezone(pytz.timezone('Australia/Sydney'))
            posted_local = datetime.strftime(posted_local, "%Y-%m-%d %H:%M:%S")
            posted_datetime = datetime.strptime(posted_local, "%Y-%m-%d %H:%M:%S")
            if start_time <= posted_datetime <= end_time:
                posts[idx]["posted"] = posted
                continue
            else:
                posts.pop(idx)
    
        if posts:
            print('return posts')
            return posts
        else:
            print("All posts are filtered out.")
            return []
    elif(filter_keys["time_from"]=="" and filter_keys["time_to"]==""):
        if posts:
            print('return posts')
            return posts
        else:
            print("All posts are filtered out.")
            return []

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
    
if __name__ == "__main__":
    
    url_list = [
        "https://api.social-searcher.com/v2/search?network=twitter&key=346bc4b91203a15abba7e49a29483393&lang=en&q=DVSRC&limit=1000",
        "https://api.social-searcher.com/v2/search?network=instagram&key=346bc4b91203a15abba7e49a29483393&lang=en&q=DVSRC&limit=1000",
        "https://api.social-searcher.com/v2/search?network=instagram&key=346bc4b91203a15abba7e49a29483393&lang=en&q=RoyalCommission&limit=1000",
        "https://api.social-searcher.com/v2/search?network=twitter&key=346bc4b91203a15abba7e49a29483393&lang=en&q=Royal%20Commission%20veteran&limit=1000",
        "https://api.social-searcher.com/v2/search?network=twitter&key=346bc4b91203a15abba7e49a29483393&lang=en&q=Royal%20Commission%20suicide&limit=1000"
    ]
    
    #with open('/home/jupyter/total_social_searcher_response.json',"r") as f:
        #posts = json.load(f)
    posts = get_api_response(url_list)
    file_name = "total_social_searcher_response.json"
    with open('/home/jupyter/{filename}'.format(filename=file_name),"w") as f:
        json.dump(posts,f,indent = 4)
    filter_keys = get_filter_key()
    # filter the posts by keyword and time period
    file_name = "raw_social_searcher_response.json"
    posts = filter_time_posts(posts, filter_keys)
    with open('/home/jupyter/{filename}'.format(filename=file_name),"w") as f:
        json.dump(posts,f,indent = 4)

    posts = filter_key_posts(posts, filter_keys)
    if not posts:
        print("Some post urls are invalid")
    file_name = "social_searcher_response.json"
    with open('/home/jupyter/{filename}'.format(filename=file_name),"w") as f:
        json.dump(posts,f,indent = 4)