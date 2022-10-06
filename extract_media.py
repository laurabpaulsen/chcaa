'''
python extract_media.py --filepath data/
'''
import ndjson
import pandas as pd
import numpy as np
import re
import os
import argparse 

def convert_to_df(data):
    """Converts a ndjson-file to a pd.DataFrame
    Args:
        data (.ndjson): .ndjson-file containing the necessary information
    Returns:
        pd.DataFrame: Dataframe containing the necessary information.
    """    
    dataframe = {
        "username": [row["includes"]["users"][0]["username"] for row in data],
        "tweetID": [row.get('id').encode("utf-8") for row in data],
        "number_of_photos": [countphoto(row['includes'].get("media", 0)) for row in data],
        "url_photos": [geturl(row['includes'].get("media", 0)) for row in data]

    }

    return pd.DataFrame(dataframe)

def load_data(data_path):
    """Loads all the ndjson-files in the specified data_path, converts them to a df and concatenates them.
    Args:
        data_path (str): Path to the folder of the ndjson-files.
    Returns:
        pd.DataFrame: Concatenated dataframe of all the files.
    """    
    file_list = os.listdir(data_path)

    file_list = [file for file in file_list if re.match("from", file)]

    dataframes = []

    for i in file_list:
        with open(data_path + i, "r") as file:
            data = ndjson.load(file)
        dataframes.append(convert_to_df(data))
    
    return pd.concat(dataframes, axis = 0).reset_index(drop = True)

def geturl(photo_info):
    ''' Extracts urls from tweets
    Args: 
        Media info from tweet object (list of dicts or 0)
    Returns:
        Urls if there is some
    '''
    if not photo_info:
        return ''
    return ",".join(info['url'] for info in photo_info if info['type'] == 'photo')

def countphoto(photo_info):
  ''' Counts the number of photos in tweets
  Args: 
    Media info from tweet object (list of dicts or 0)
  Returns:
    The number of photos
    '''
  if not photo_info:
    return 0
  return sum(1 for info in photo_info if info['type'] == 'photo')

def geturl_video(video_info, tweet):
    ''' Extracts urls from tweets
    Args: 
        Media info from tweet object (list of dicts or 0)
    Returns:
        Urls if there is some
    '''
    if not video_info:
        return ''
    elif video_info[0]['type'] == 'video': 
        return tweet['entities']['urls'][0].get('expanded_url', 0)
    else:
        return ''



if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('-f','--filepath', required=True, help='path to dir with json')
    args = vars(ap.parse_args())

    df = load_data(args['filepath'])
    df['tweetID']= df['tweetID'].astype('str')
    df.to_csv('testing.csv', index = False, encoding =  "utf-8")