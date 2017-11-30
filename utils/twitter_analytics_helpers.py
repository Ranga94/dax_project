import string
from google.cloud import translate
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
from nltk.tag import StanfordNERTagger
import os
from nltk import TweetTokenizer
import time
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from . import TaggingUtils
import spacy
from collections import defaultdict

def get_constituent_id_name(old_constituent_name):
    mapping = {}
    mapping["BMW"] = ("BMWDE8170003036" , "BAYERISCHE MOTOREN WERKE AG")
    mapping["Allianz"] = ("ALVDEFEI1007380" , "ALLIANZ SE")
    mapping["Commerzbank"] = ("CBKDEFEB13190" , "COMMERZBANK AKTIENGESELLSCHAFT")
    mapping["adidas"] = ("ADSDE8190216927", "ADIDAS AG")
    mapping["Deutsche Bank"] = ("DBKDEFEB13216" , "DEUTSCHE BANK AG")
    mapping["EON"] = ("EOANDE5050056484" , "E.ON SE")
    mapping["Lufthansa"] = ("LHADE5190000974" ,"DEUTSCHE LUFTHANSA AG")
    mapping["Continental"] = ("CONDE2190001578" , "CONTINENTAL AG")
    mapping["Daimler"] = ("DAIDE7330530056" , "DAIMLER AG")
    mapping["Siemens"] = ("SIEDE2010000581" , "SIEMENS AG")
    mapping["BASF"] = ("BASDE7150000030" , "BASF SE")
    mapping["Bayer"] = ("BAYNDE5330000056" , "BAYER AG")
    mapping["Beiersdorf"] = ("BEIDE2150000164" , "BEIERSDORF AG")
    mapping["Deutsche Börse"] = ("DB1DEFEB54555" , "DEUTSCHE BOERSE AG")
    mapping["Deutsche Post"] = ("DPWDE5030147191" , "DEUTSCHE POST AG")
    mapping["Deutsche Telekom"] = ("DTEDE5030147137" , "DEUTSCHE TELEKOM AG")
    mapping["Fresenius"] = ("FREDE6290014544" , "FRESENIUS SE & CO.KGAA")
    mapping["HeidelbergCement"] = ("HEIDE7050000100" , "HEIDELBERGCEMENT AG")
    mapping["Henkel vz"] = ("HEN3DE5050001329" , "HENKEL AG & CO.KGAA")
    mapping["Infineon"] = ("IFXDE8330359160" , "INFINEON TECHNOLOGIES AG")
    mapping["Linde"] = ("LINDE8170014684" , "LINDE AG")
    mapping["Merck"] = ("MRKDE6050108507" , "MERCK KGAA")
    mapping["ProSiebenSat1 Media"] = ("PSMDE8330261794" , "PROSIEBENSAT.1 MEDIA SE")
    mapping["RWE"] = ("RWEDE5110206610" , "RWE AG")
    mapping["SAP"] = ("SAPDE7050001788" , "SAP SE")
    mapping["thyssenkrupp"] = ("TKADE5110216866" , "THYSSENKRUPP AG")
    mapping["Vonovia"] = ("VNADE5050438829" , "VONOVIA SE")
    mapping["DAX"] = ("DAX", "DAX")
    mapping["Fresenius Medical Care"] = ("FMEDE8110066557" , "FRESENIUS MEDICAL CARE AG & CO.KGAA")
    mapping["Volkswagen"] = ("VOW3DE2070000543" , "VOLKSWAGEN AG")
    mapping["Münchener Rückversicherungs-Gesellschaft"] = ("MUV2DEFEI1007130" , "MUNCHENER RUCKVERSICHERUNGS - GESELLSCHAFT AKTIENGESELLSCHAFT IN MUNCHEN")

    if old_constituent_name in mapping:
        return mapping[old_constituent_name]
    else:
        return old_constituent_name

def get_old_constituent_name(constituent_id):
    mapping = {}
    mapping["BMWDE8170003036"] = "BMW"
    mapping["ALVDEFEI1007380"] = "Allianz"
    mapping["CBKDEFEB13190"] = "Commerzbank"
    mapping["ADSDE8190216927"] = "adidas"
    mapping["DBKDEFEB13216"] = "Deutsche Bank"
    mapping["EOANDE5050056484"] = "EON"
    mapping["LHADE5190000974"] = "Lufthansa"
    mapping["CONDE2190001578"] = "Continental"
    mapping["DAIDE7330530056"] = "Daimler"
    mapping["SIEDE2010000581"] = "Siemens"
    mapping["BASDE7150000030"] = "BASF"
    mapping["BAYNDE5330000056"] = "Bayer"
    mapping["BEIDE2150000164"] = "Beiersdorf"
    mapping["DB1DEFEB54555"] = "Deutsche Börse"
    mapping["DPWDE5030147191"] = "Deutsche Post"
    mapping["DTEDE5030147137"] = "Deutsche Telekom"
    mapping["FREDE6290014544"] = "Fresenius"
    mapping["HEIDE7050000100"] = "HeidelbergCement"
    mapping["HEN3DE5050001329"] = "Henkel vz"
    mapping["IFXDE8330359160"] = "Infineon"
    mapping["LINDE8170014684"] = "Linde"
    mapping["MRKDE6050108507"] = "Merck"
    mapping["PSMDE8330261794"] = "ProSiebenSat1 Media"
    mapping["RWEDE5110206610"] = "RWE"
    mapping["SAPDE7050001788"] = "SAP"
    mapping["TKADE5110216866"] = "thyssenkrupp"
    mapping["VNADE5050438829"] = "Vonovia"
    mapping["DAX"] = "DAX"
    mapping["FMEDE8110066557"] = "Fresenius Medical Care"
    mapping["VOW3DE2070000543"] = "Volkswagen"
    mapping["MUV2DEFEI1007130"] = "Münchener Rückversicherungs-Gesellschaft"

    if constituent_id in mapping:
        return mapping[constituent_id]
    else:
        return constituent_id

def get_nltk_sentiment(text):
    sia = SIA()
    sent = None
    try:
        sent = sia.polarity_scores(text)['compound']
    except Exception as e:
        sent = None

    return sent

def do_translation(to_translate):
    translate_client = None
    try:
        translate_client = translate.Client()
    except Exception as e:
        #print("Error translating. Skipping...")
        print(e)
        return False


    texts = []
    tokenizer = TweetTokenizer(preserve_case=False, reduce_len=True, strip_handles=False)

    for tweet in to_translate:
        tokens = tokenizer.tokenize(tweet._json['text'])
        # remove links
        no_url_tokens = [word for word in tokens if 'http' not in word]
        texts.append(" ".join(no_url_tokens))


    texts = [tweet._json['text'] for tweet in to_translate]

    try:
        translations = translate_client.translate(texts, target_language='en')
    except Exception as e:
        #print("Error translating. Skipping...")
        print(e)
        return False

    if len(translations) == len(to_translate):
        for i in range(0, len(translations)):
            to_translate[i]._json['text_en'] = translations[i]['translatedText']

        time.sleep(1)
        return True
    else:
        return False

def preprocess_tweet(text):
    # Tokenize the tweet text
    tokenizer = TweetTokenizer(preserve_case=False, reduce_len=True, strip_handles=False)
    tokens = tokenizer.tokenize(text)

    # remove links
    no_url_tokens = [word for word in tokens if 'http' not in word]

    no_url_joined = " ".join(no_url_tokens)

    # remove stop words and punctuation
    stop_words = set(stopwords.words('english'))
    punct = string.punctuation
    punct_1 = punct.replace('#', '')
    punct_2 = punct_1.replace('@', '')
    stop_words.update(punct_2)
    stop_words.add('...')

    filtered_tokens = [word for word in no_url_tokens if not word in stop_words]

    stemmer = PorterStemmer()
    stemmed_tokens = [stemmer.stem(word) if (word[0] != '#' and word[0] != '@') else word for word in filtered_tokens]

    return {'semi_processed_text': no_url_joined, 'processed_text': stemmed_tokens}

def get_tags(tagger, text):
    '''
    new_text = text.replace('€','$')
    tokenized_text = tokenizer.tokenize(new_text)
    classified_text = st.tag(tokenized_text)
    return classified_text
    '''
    return tagger(text.decode('utf-8'))

def get_tagger():
    return spacy.load('en')

def update_tags(dict_object, tagged_text):
    tags = {}
    tags["PERSON_TAGS"] = []
    tags["NORP_TAGS"] = []
    tags["FACILITY_TAGS"] = []
    tags["ORG_TAGS"] = []
    tags["GPE_TAGS"] = []
    tags["LOC_TAGS"] = []
    tags["PRODUCT_TAGS"] = []
    tags["EVENT_TAGS"] = []
    tags["WORK_OF_ART_TAGS"] = []
    tags["LANGUAGE_TAGS"] = []
    tags["DATE_TAGS"] = []
    tags["TIME_TAGS"] = []
    tags["PERCENT_TAGS"] = []
    tags["MONEY_TAGS"] = []
    tags["QUANTITY_TAGS"] = []
    tags["ORDINAL_TAGS"] = []
    tags["CARDINAL_TAGS"] = []

    for ent in tagged_text.ents:
        tags["{}_TAGS".format(ent.label_)].append(ent.text)

    dict_object.update(tags)

    return dict_object

def get_spacey_tags(tagged_text):
    data = {}
    data["PERSON"] = []
    data["NORP"] = []
    data["FACILITY"] = []
    data["ORG"] = []
    data["GPE"] = []
    data["LOC"] = []
    data["PRODUCT"] = []
    data["EVENT"] = []
    data["WORK_OF_ART"] = []
    data["LAW"] = []
    data["LANGUAGE"] = []
    data["DATE"] = []
    data["TIME"] = []
    data["PERCENT"] = []
    data["MONEY"] = []
    data["QUANTITY"] = []
    data["ORDINAL"] = []
    data["CARDINAL"] = []
    data["FAC"] = []
    for ent in tagged_text.ents:
        try:
            data[ent.label_].append(ent.text)
        except KeyError as e:
            pass

    return data

def convert_timestamp(str):
    ts = time.strptime(str, '%a %b %d %H:%M:%S +0000 %Y')
    ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)

    return ts

def scrub(d):
    tweet = create_tweet_skelleton()
    record_types = ["contributors", "coordinates", "entities", "user", "geo","place"]
    contributor_fields = ["screen_name", "id", "id_str"]
    coordinates_fields = ["type","coordinates"]
    coordinates_coordinates_fields = ["lat", "long"]
    entities_fields = ["symbols", "hashtags", "user_mentions", "trends",
                       "urls"]
    entities_symbols_fields = ["indices", "text"]
    entities_hashtags_fields = ["indices", "text"]
    entities_user_mentions_fields = ["id", "indices", "id_str", "screen_name", "name"]
    entities_trends_fields = ["woeid", "name", "countryCode", "url", "country"]
    entities_urls_fields = ["url","indices","expanded_url", "display_url"]
    user_fields = ["follow_request_sent", "profile_use_background_image","default_profile_image",
                   "id","verified","profile_image_url_https","profile_sidebar_fill_color",
                   "profile_text_color","followers_count","profile_sidebar_border_color",
                   "id_str","profile_background_color","listed_count","profile_background_image_url_https",
                   "utc_offset","statuses_count","description","friends_count","location","profile_link_color",
                   "profile_image_url","following","geo_enabled","profile_banner_url","profile_background_image_url",
                   "name","lang","profile_background_tile","favourites_count","screen_name","notifications",
                   "url","created_at","contributors_enabled","time_zone","protected","default_profile",
                   "is_translator"]
    geo_fields = ["type", "coordinates"]
    geo_coordinates_fields = ["lat", "long"]
    place_fields = ["full_name","url","country","place_type","country_code",
                    "id","name"]

    for key, value in list(d.items()):
        if key in d:
            if key in record_types:
                if key == "contributors":
                    if d[key]:
                        for item in d[key]:
                            temp = {"screen_name":None, "id":None, "id_str":None}
                            for key2, value2 in list(item.items()):
                                if key2 in contributor_fields:
                                    temp[key2] = value2
                            tweet[key].append(temp)
                elif key == "coordinates":
                    for key2, value2 in list(d["coordinates"].items()):
                        if key2 in coordinates_fields:
                            if key2 == "coordinates":
                                for key3, value3 in list(d["coordinates"]["coordinates"]):
                                    if key3 in coordinates_coordinates_fields:
                                        tweet[key][key2][key3] = value
                            else:
                                tweet[key][key2] = value2
                elif key == "entities":
                    for key2, value2 in list(d["entities"].items()):
                        if key2 in entities_fields:
                            if key2 == "symbols":
                                if tweet[key][key2]:
                                    for item in d[key][key2]:
                                        temp = {"indices": None, "text": None}
                                        for key3, value3 in list(item.items()):
                                            if key3 in entities_symbols_fields:
                                                temp[key3] = value3
                                        tweet[key][key2].append(temp)
                            elif key2 == "hashtags":
                                if d[key][key2]:
                                    for item in d[key][key2]:
                                        temp = {"indices":None, "text":None}
                                        for key3, value3 in list(item.items()):
                                            if key3 in entities_hashtags_fields:
                                                temp[key3] = value3
                                        tweet[key][key2].append(temp)
                            elif key2 == "user_mentions":
                                if d[key][key2]:
                                    for item in d[key][key2]:
                                        temp = {"id":None, "indices":None, "id_str":None,
                                                "screen_name":None, "name":None}
                                        for key3, value3 in list(item.items()):
                                            if key3 in entities_user_mentions_fields:
                                                temp[key3] = value3
                                        tweet[key][key2].append(temp)
                            elif key2 == "trends":
                                if d[key][key2]:
                                    for item in d[key][key2]:
                                        temp = {"woeid":None, "name":None, "countryCode":None,
                                                "url":None, "country":None}
                                        for key3, value3 in list(item.items()):
                                            if key3 in entities_trends_fields:
                                                temp[key3] = value3
                                        tweet[key][key2].append(temp)
                            elif key2 == "urls":
                                if d[key][key2]:
                                    for item in d[key][key2]:
                                        temp = {"url":None,"indices":None,"expanded_url":None,
                                                "display_url":None}
                                        for key3, value3 in list(item.items()):
                                            if key3 in entities_urls_fields:
                                                temp[key3] = value3
                                        tweet[key][key2].append(temp)
                elif key == "user":
                    for key2, value2 in list(d[key].items()):
                        if key2 in user_fields:
                            tweet[key][key2] = value2
                elif key == "geo":
                    for key2, value2 in list(d[key].items()):
                        if key2 in geo_fields:
                            if key2 == "coordinates":
                                for key3, value3 in list(d[key][key2].items()):
                                    if key3 in geo_coordinates_fields:
                                        tweet[key][key2][key3] = value3
                            else:
                                tweet[key][key2] = value2
                elif key == "place":
                    for key2, value2 in list(d[key].items()):
                        if key2 in place_fields:
                            tweet[key][key2] = value2
            else:
                tweet[key] = value
    return tweet  # For convenience

def create_tweet_skelleton():
    tweet = {}
    tweet["contributors"] = []
    tweet["truncated"] = None
    tweet["text"] = None
    tweet["in_reply_to_status_id"] = None
    tweet["id"] = None
    tweet["favorite_count"] = None
    tweet["source"] = None
    tweet["retweeted"] = None
    tweet["coordinates"] = {}
    tweet["coordinates"]["type"] = None
    tweet["coordinates"]["coordinates"] = {}
    tweet["coordinates"]["coordinates"]["lat"] = None
    tweet["coordinates"]["coordinates"]["long"] = None
    tweet["timestamp_ms"] = None
    tweet["entities"] = {}
    tweet["entities"]["symbols"] = []
    tweet["entities"]["hashtags"] = []
    tweet["entities"]["user_mentions"] = []
    tweet["entities"]["trends"] = []
    tweet["entities"]["urls"] = []
    tweet["in_reply_to_screen_name"] = None
    tweet["id_str"] = None
    tweet["retweet_count"] = None
    tweet["in_reply_to_user_id"] = None
    tweet["favorited"] = None
    tweet["user"] = {}
    tweet["user"]["follow_request_sent"] = None
    tweet["user"]["profile_use_background_image"] = None
    tweet["user"]["default_profile_image"] = None
    tweet["user"]["id"] = None
    tweet["user"]["verified"] = None
    tweet["user"]["profile_image_url_https"] = None
    tweet["user"]["profile_sidebar_fill_color"] = None
    tweet["user"]["profile_text_color"] = None
    tweet["user"]["followers_count"] = None
    tweet["user"]["profile_sidebar_border_color"] = None
    tweet["user"]["id_str"] = None
    tweet["user"]["profile_background_color"] = None
    tweet["user"]["listed_count"] = None
    tweet["user"]["profile_background_image_url_https"] = None
    tweet["user"]["utc_offset"] = None
    tweet["user"]["statuses_count"] = None
    tweet["user"]["description"] = None
    tweet["user"]["friends_count"] = None
    tweet["user"]["location"] = None
    tweet["user"]["profile_link_color"] = None
    tweet["user"]["profile_image_url"] = None
    tweet["user"]["following"] = None
    tweet["user"]["geo_enabled"] = None
    tweet["user"]["profile_banner_url"] = None
    tweet["user"]["profile_background_image_url"] = None
    tweet["user"]["name"] = None
    tweet["user"]["lang"] = None
    tweet["user"]["profile_background_tile"] = None
    tweet["user"]["favourites_count"] = None
    tweet["user"]["screen_name"] = None
    tweet["user"]["notifications"] = None
    tweet["user"]["url"] = None
    tweet["user"]["created_at"] = None
    tweet["user"]["contributors_enabled"] = None
    tweet["user"]["time_zone"] = None
    tweet["user"]["protected"] = None
    tweet["user"]["default_profile"] = None
    tweet["user"]["is_translator"] = None
    tweet["geo"] = {}
    tweet["geo"]["type"] = None
    tweet["geo"]["coordinates"] = {}
    tweet["geo"]["coordinates"]["lat"] = None
    tweet["geo"]["coordinates"]["long"] = None
    tweet["in_reply_to_user_id_str"] = None
    tweet["possibly_sensitive"] = None
    tweet["lang"] = None
    tweet["created_at"] = None
    tweet["filter_level"] = None
    tweet["in_reply_to_status_id_str"] = None
    tweet["place"] = {}
    tweet["place"]["full_name"] = None
    tweet["place"]["url"] = None
    tweet["place"]["country"] = None
    tweet["place"]["place_type"] = None
    tweet["place"]["country_code"] = None
    tweet["place"]["id"] = None
    tweet["place"]["name"] = None
    tweet["constituent_id"] = None
    tweet["constituent_name"] = None
    tweet["constituent_id"] = None
    tweet["search_term"] = None

    return tweet



if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The connection string')
    parser.add_argument('tagger_dir_1', help='Tagging server directory')
    parser.add_argument('tagger_dir_2', help='Tagging server directory')
    args = parser.parse_args()