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
diplomat_list = ['AmbassadeChine', 'Amb_ChenXu', 'ambcina', 'AmbCuiTiankai', 'AmbLiuXiaoMing','CCGBelfast','CGTNOfficial','chenweihua','ChinaAmbUN','chinacgedi','ChinaConsulate','ChinaDaily', 'ChinaEmbassyUSA''ChinaEmbGermany','ChinaEUMission','ChinaInDenmark','China_Lyon','Chinamission2un','ChinaMissionGva','ChinaMissionVie','chinascio', 'ChineseEmbinUK', 'ChineseEmbinUS', 'ChnMission','CHN_UN_NY', 'CNS1952', 'consulat_de', 'EUMissionChina','GeneralkonsulDu','globaltimesnews','HuXijin_GT','MFA_China','ouzhounews','PDChina','PDChinese','QiushiJournal','shen_shiwei', 'SpokespersonCHN', 'spokespersonHZM','XHNews', 'XinWen_Ch','zlj517']

def get_mentionee(mention_info):
    ''' Extracts urls from tweets
    Args: 
        Media info from tweet object (list of dicts or 0)
    Returns:
        Urls if there is some
    '''
    if not mention_info:
        return ''
    return ",".join(info['username'] for info in mention_info if info['username'] in diplomat_list)



def get_category(string, media_list):
    if string in media_list:
        return "Media"
    else:
        return "Not media"

def convert_to_df(data):
    """Converts a ndjson-file to a pd.DataFrame

    Args:
        data (.ndjson): .ndjson-file containing the necessary information

    Returns:
        pd.DataFrame: Dataframe containing the necessary information.
    """    

    text = [row["text"] for row in data]

    dataframe = {
        "mentioner": [row["includes"]["users"][0]["username"] for row in data],
        "mentionee": [get_mentionee(row['entities'].get('mentions')) for row in data]
        }
    return pd.DataFrame(dataframe)


def load_data(data_path):
    """Loads all specified ndjson-files and converts into a pandas dataframe. 

    Args:
        data_path (str): Path to the ndjson-file.

    Returns:
        pd.DataFrame: Dataframe of the files.
    """    

    with open('data.ndjson', 'r') as f:
        data = ndjson.load(f)
    
    return convert_to_df(data)

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('-f','--filepath', required=True, help='path to dir with json')
    args = vars(ap.parse_args())

    df = load_data('filepath')
    df["category"] = df["mentioner"].apply(lambda x:get_category(x, media_list))
    df.to_csv('Amention_data.csv')