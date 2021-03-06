"""
usage:
    python src/json2csv.py --filepath data/example.ndjson
"""

# imports 
import ndjson
import datetime, time
import pandas as pd
import numpy as np
import re
from tqdm import tqdm
import os
import argparse 
from pathlib import Path
import io



media_list = ['Echinanews', "shen_shiwei", "CGTNOfficial", "XHNews", "ChinaDaily", "chenweihua", "CNS1952", "PDChina", "PDChinese", "globaltimesnews", "HuXijin_GT", "XinWen_Ch", "QiushiJournal"]

diplomat_list = ['AmbassadeChine', 'Amb_ChenXu', 'ambcina', 'AmbCuiTiankai', 'AmbLiuXiaoMing','CCGBelfast','ChinaAmbUN','chinacgedi', 'ChinaConsulate', 'ChinaEmbassyUSA','ChinaEmbGermany','ChinaEUMission','ChinaInDenmark','China_Lyon','Chinamission2un','ChinaMissionGva','ChinaMissionVie','chinascio', 'ChineseEmbinUK', 'ChineseEmbinUS', 'ChnMission','CHN_UN_NY', 'consulat_de', 'EUMissionChina','GeneralkonsulDu','MFA_China','SpokespersonCHN', 'SpokespersonHZM','zlj517', 'AmbCina', 'ChinaConSydney', 'ChinaEmbOttawa', 'ChinaCGCalgary', 'ChinaCGMTL', 'ChinainVan', 'ChnEmbassy_jp', 'ChnConsul_osaka', 
                 'AmbKongXuanyou', 'AmbLiJunhua','AmbQinGang', 'AmbZhengZeguang', 'Cao_Li_CHN', 'CGHuangPingNY', 'CGMeifangZhang', 'CGZhangPingLA', 'ChinaCG_HH', 'ChinaCG_Muc', 'ChinaCG_NYC', 'ChinaEmbEsp', 'China_Ukraine_', 'China_Ukraine', 'ChineseCon_Mel', 'ChineseEmbinRus', 'ChnConsulateFuk', 'ChnConsulateNag', 'ChnConsulateNgo', 'ChnConsulateNgt', 'ConsulChinaBcn', 'ConsulateSan', 'DIOC_MFA_China', 
                 'FuCong17', 'FukLyuGuijun', 'Li_Yang_China', 'LongDingbin', 'SpoxCHNinUS', 'WangLutongMFA', 'WuPeng_MFAChina', 'XIEYongjun_CHN', 'xuejianosaka', 'zhaobaogang2011', 'Zhou_Li_CHN', 'zhu_jingyang', 'ChinaCG_Ffm']




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
        "mentionee": [handle for row in data],
        "text": [[row['text'] for row in row.get('includes')['tweets']][0].replace('\r','') if check(row) else row["text"].encode("utf-8").replace('\r','') for row in data],
        "retweet": [row["referenced_tweets"][0]["type"] if row.get("referenced_tweets") else "original" for row in data],
        "created_at": [row["created_at"] for row in data],
        "lang": [row["lang"] for row in data],
        "followers_mentioner": [row["includes"]["users"][0]["public_metrics"]["followers_count"] for row in data],
        "following_mentioner": [row["includes"]["users"][0]["public_metrics"]["following_count"] for row in data],
        "verified_mentioner": [row["includes"]["users"][0]["verified"] for row in data],
        "profileimg_mentioner": [row["includes"]["users"][0]["profile_image_url"] for row in data],
        "created_mentioner": [row["includes"]["users"][0]["created_at"] for row in data]
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
    handle = args['filepath'].split('mention_')[-1].split('_20')[0]
    filename = args['filepath'].split('mention_')[-1].split('.nd')[0]

    df = load_data(args['filepath'])
    df["category"] = df["mentioner"].apply(lambda x:get_category(x, media_list, diplomat_list))
    df["category_mentionee"] = df["mentionee"].apply(lambda x:get_category(x, media_list, diplomat_list))
    df['mentionee'] = df['mentionee'].replace('', handle)
    df['tweetID']= df['tweetID'].astype('str')
    df.to_csv('mentiondata/%s.csv' % filename, index = False, encoding =  "utf-8")
