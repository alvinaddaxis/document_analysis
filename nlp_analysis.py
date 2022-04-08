from google.cloud import language
from google.cloud import language_v1
from google.protobuf import json_format
from google.protobuf.json_format import MessageToDict
import os, glob
import json

# sentiment analysis using google nlp api
def analyze_text_sentiment(text):
    client = language.LanguageServiceClient()
    document = language.Document(content=text, type_=language.Document.Type.PLAIN_TEXT)

    response = client.analyze_sentiment(document=document)

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
    client = language.LanguageServiceClient()
    document = language.Document(content=text, type_=language.Document.Type.PLAIN_TEXT)

    response = client.analyze_entities(document=document)

    # convert google protobuf format to dic
    entity_result = MessageToDict(response.__class__.pb(response))
    
    return entity_result

def analyze_entity_sentiment(text):
    client = language.LanguageServiceClient()
    document = language.Document(content=text, type_=language.Document.Type.PLAIN_TEXT)
    # get the entities sentiment analysis results
    response = client.analyze_entity_sentiment(document=document)
    result_json = response.__class__.to_json(response)
    results = json.loads(result_json)
    return results
