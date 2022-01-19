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

diplomat_list = ['AmbassadeChine', 'Amb_ChenXu', 'ambcina', 'AmbCuiTiankai', 'AmbLiuXiaoMing','CCGBelfast','ChinaAmbUN','chinacgedi', 'ChinaConsulate', 'ChinaEmbassyUSA','ChinaEmbGermany','ChinaEUMission','ChinaInDenmark','China_Lyon','Chinamission2un','ChinaMissionGva','ChinaMissionVie','chinascio', 'ChineseEmbinUK', 'ChineseEmbinUS', 'ChnMission','CHN_UN_NY', 'consulat_de', 'EUMissionChina','GeneralkonsulDu','MFA_China','SpokespersonCHN', 'spokespersonHZM','zlj517', 'AmbCina']

handle = list(args['filepath'].split('mention_')[-1].split('_2007-01')[0])

def get_mentionee(mention_info):
    ''' Extracts mention info from tweets
    Args: 
        Mention info from tweet object (list of dicts or 0)
    Returns:
        mentions if they are included in the diplomat list
    '''
    if not mention_info:
        return ''
    return ",".join(info['username'] for info in mention_info if info['username'] in handle)


def get_category(string, media_list, diplomat_list):
    if string in media_list:
        return "Media"
    elif string in diplomat_list:
        return "Diplomat"
    else:
        return "Neither"
    
    
def check(tweet_data):
    if tweet_data["text"].encode("utf-8").startswith("RT @"):
        if tweet_data.get('includes'):
            tweetinfo = tweet_data.get('includes')
            if tweetinfo.get('tweets'):
                return True
    else:
        return False

def convert_to_df(data):
    """Converts a ndjson-file to a pd.DataFrame

    Args:
        data (.ndjson): .ndjson-file containing the necessary information

    Returns:
        pd.DataFrame: Dataframe containing the necessary information.
    """    
    
    dataframe = {
        "tweetID": [row.get('id').encode("utf-8") for row in data],
        "mentioner": [row["includes"]["users"][0]["username"] for row in data],
        "mentionee": [get_mentionee(row) for row in data],
        "text": [[row['text'] for row in row.get('includes')['tweets']][0] if check(row) else row["text"].encode("utf-8") for row in data],
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
    df["category_mentionee"] = df["mentionee"].apply(lambda x:get_category(x, media_list, diplomat_list))
    df['mentionee'] = df['mentionee'].replace('', handle)
    df['tweetID']= df['tweetID'].astype('int')
    df.to_csv('mentiondata/%s.csv' % handle, index = False, encoding =  "utf-8")
