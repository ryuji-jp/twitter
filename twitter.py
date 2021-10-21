# -*- coding: utf-8 -*-
import json
import pickle
import datetime
from dateutil.parser import parse as dateutil_parser
from requests_oauthlib import OAuth1Session
from influxdb import InfluxDBClient
#from datetime import datetime

# アクセストークン等を設定
config = {
    "CONSUMER_KEY": "XXX",
    "CONSUMER_SECRET": "XXX",
    "ACCESS_TOKEN": "XXX",
    "ACCESS_TOKEN_SECRET": "XXX"
}

client = InfluxDBClient(host='localhost',port=8086,database='twitter')

# ツイート保存用関数
def pickle_dump(obj, path):
    with open(path, mode='wb') as f:
        pickle.dump(obj,f)
# 保存したツイートをロードする関数
def pickle_load(path):
    with open(path, mode='rb') as f:
        data = pickle.load(f)
        return data

count = 0

class TwitterApi:
    def __init__(self, config):
        self.start_session(**config)
        # search api の url
        self.url_search = "https://api.twitter.com/1.1/search/tweets.json?tweet_mode=extended"

    def start_session(self, CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET):
        # OAuth認証
        self.api = OAuth1Session(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    def search(self, 
        search_word,# 検索ワード
        since=None, # ツイート期間の絞りこみ（いつからか、日本時間）
        until=None, # ツイート期間の絞りこみ（いつまでか、日本時間）
        max_tweets = 1000 #最大検索数
    ):
        def print_tweet(tweet):
            print("====================================")
            print( tweet["user"].get("name"), "@", tweet["user"].get("screen_name") )
            print( tweet.get("full_text").replace("\n", " ") )
            print( "Date:", tweet["created_at"])
            print( "FV:", tweet.get("favorite_count"))
            print( "RT:", tweet.get("retweet_count"))
            print()
            global count
            count +=1


        def perser(index, tweet, hours=9): #hours=9 で日本時間への変換となっている
            dt = dateutil_parser(tweet.get("created_at")) + datetime.timedelta(hours=hours)
            tweet["created_at"] = dt
            return tweet, dt

        # 日付関連の変数
        since_dt = dateutil_parser(since+"+00:00")
        until_params = dateutil_parser(until+"+00:00").strftime('%Y-%m-%d_%H:%M:%S')+"_JST"
        
        max_api_call = max_tweets // 100 + 1 # APIを叩く回数


        # 検索実行
        tweets = []
        max_id = None
        index = 0
        params = {"q": search_word, "count": 100, "result_type": "recent", 
                  "exclude": "retweets", "until": until_params}
        for n in range(max_api_call):
            # APIを叩く（最大100件）
            req = self.api.get(self.url_search, params=params)
            if req.status_code == 200: #検索結果を得られた場合
                #検索を取得
                timeline = json.loads(req.text)["statuses"]
                if len(timeline) == 0:
                    break
                for k, tweet in enumerate(timeline):
                    max_id = int(tweet["id"]) - 1
                    # ツイートの日時を取得
                    tweet_mod, dt = perser(index, tweet)
                    # ツイートが期間内のものか確認
                    if dt >= since_dt:
                        tweets.append(tweet_mod)
                        print_tweet(tweet_mod)
                        index += 1
                        params["max_id"] = max_id 
                    else:
                        max_id = None
                print()
                if max_id is None:
                    break
            else:
                print("Error")
                
        return tweets


if __name__=='__main__':

    # 1. インスタンス作成
    twitter_api = TwitterApi(config)
    
    # 2. 検索を実行し、結果（各ツイート：辞書型）が格納されたリストを得る
    tweets = twitter_api.search(
        "#codeblue_jp",
        since="2021-10-18 00:00:00",
        until="2021-10-21 00:00:00"
    )
    
    # 3. リストを保存
#    pickle_dump(tweets, "twitter_search_api.pickle")
    
    import code
    console = code.InteractiveConsole(locals=locals())
    print(count)

    json_body = [
    {
        "measurement": "twitter",
        "time": datetime.datetime.utcnow(),
        "fields": {
            "#codeblue_jp": count
            }
        }
    ]

    client.write_points(json_body)


#    console.interact()    
