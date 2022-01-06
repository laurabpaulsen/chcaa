"""
usage:
    python src/json2csv.py --filepath data/example/
"""

# imports 
import ndjson
import datetime
import pandas as pd
import numpy as np
import re
from tqdm import tqdm
import os
import argparse 
from pathlib import Path
import io


media_list = ["ouzhounews", "shen_shiwei", "CGTNOfficial", "XHNews", "ChinaDaily", "chenweihua", "CNS1952", "PDChina", "PDChinese", "globaltimesnews", "HuXijin_GT", "XinWen_Ch", "QiushiJournal"]
diplomat_list = ['AmbassadeChine', 'Amb_ChenXu', 'ambcina', 'AmbCuiTiankai', 'AmbLiuXiaoMing','CCGBelfast','CGTNOfficial','chenweihua','ChinaAmbUN','chinacgedi', 'ChinaConsulate','ChinaDaily', 'ChinaEmbassyUSA','ChinaEmbGermany','ChinaEUMission','ChinaInDenmark','China_Lyon','Chinamission2un','ChinaMissionGva','ChinaMissionVie','chinascio', 'ChineseEmbinUK', 'ChineseEmbinUS', 'ChnMission','CHN_UN_NY', 'CNS1952', 'consulat_de', 'EUMissionChina','GeneralkonsulDu','globaltimesnews','HuXijin_GT','MFA_China','ouzhounews','PDChina','PDChinese','QiushiJournal','shen_shiwei', 'SpokespersonCHN', 'spokespersonHZM','XHNews', 'XinWen_Ch','zlj517', 'AmbCina']

def get_mentionee(mention_info):
    ''' Extracts mention info from tweets
    Args: 
        Mention info from tweet object (list of dicts or 0)
    Returns:
        mentions if they are included in the diplomat list
    '''
    if not mention_info:
        return ''
    return ",".join(info['username'] for info in mention_info if info['username'] in diplomat_list)

def get_mentionee1(tweet_data):
    ''' Extracts urls from tweets
    Args: 
        Media info from tweet object (list of dicts or 0)
    Returns:
        Urls if there is some
    '''
    list_of_mentions = [tweet_data.get('entities').get('mentions') for tweet in tweet_data.get('includes')['tweets']][0]
    mention_info = [tweet['username'] for tweet in list_of_mentions if list_of_mentions is not None]

    if not mention_info:
        return ''
    return ",".join(info for info in mention_info if info in diplomat_list)

def get_category(string, media_list, diplomat_list):
    if string in media_list:
        return "Media"
    elif string in diplomat_list:
        return "Diplomat"
    else:
        return "Neither"

def convert_to_df(data):
    """Converts a ndjson-file to a pd.DataFrame

    Args:
        data (.ndjson): .ndjson-file containing the necessary information

    Returns:
        pd.DataFrame: Dataframe containing the necessary information.
    """    
    
    dataframe = {
        "mentioner": [row["includes"]["users"][0]["username"] for row in data],
        "mentionee": [get_mentionee1(row) if row["text"].encode("utf-8").startswith("RT @") else get_mentionee(row['entities'].get('mentions')) for row in data],
        "text": [[row['text'] for row in row.get('includes')['tweets']][0] if row["text"].encode("utf-8").startswith("RT @") else row["text"].encode("utf-8") for row in data],
        "retweet": [row["referenced_tweets"][0]["type"] if row.get("referenced_tweets") else "original" for row in data]
        }
    return pd.DataFrame(dataframe)


def load_data(data_path):
    """Loads all specified ndjson-files and converts into a pandas dataframe. 

    Args:
        data_path (str): Path to the ndjson-file.

    Returns:
        pd.DataFrame: Dataframe of the files.
    """    

    with open(data_path, 'r') as f:
        data = ndjson.load(f)
    
    return convert_to_df(data)



if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('-f','--filepath', required=True, help='path to dir with json')
    args = vars(ap.parse_args())

    df = load_data(args['filepath'])
    df["category"] = df["mentioner"].apply(lambda x:get_category(x, media_list, diplomat_list))
    df.to_csv('mentiondata/%s.csv' % args['filepath'].split("/")[-1], encoding =  "utf-8")
