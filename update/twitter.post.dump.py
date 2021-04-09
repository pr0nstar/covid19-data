import tweepy
import json
import io
import os
import sys

from datetime import datetime as dt

def get_auth_data():
    with io.open('../../../../../gente/twitter.data') as f:
        data = [_.strip().split(':') for _ in f if _]

    return dict(data)


def load_tweets(api, PAGES):
    cur = tweepy.Cursor(api.home_timeline)

    for username in PAGES:
        print(username)

        file_name = './twitter/posts/{}.json'.format(username)
        query = {
            'method': api.user_timeline,
            'screen_name': username
        }

        if ' ' in username:
            query = {
                'method': api.search,
                'q': username
            }

        with io.open(file_name, encoding='utf-8') as f:
            data = []

            for tweet in f:
                tweet = json.loads(tweet)
                if type(tweet['created_at']) == str:
                    tweet['created_at'] = int(dt.strptime(
                        tweet['created_at'], '%a %b %d %H:%M:%S %z %Y'
                    ).timestamp()) * 1000

                data.append(tweet)

            data = sorted(data, key=lambda _: _['created_at'])[-1]

        cur = tweepy.Cursor(
            since_id=data['id'] + 1,
            tweet_mode='extended',
            **query
        )
        try:
            with io.open(file_name, mode='a', encoding='utf-8') as f:
                for tweet in cur.items():
                    tweet = json.dumps(tweet._json, ensure_ascii=False)
                    f.write(u'{}\n'.format(tweet))
        except:
            continue

if __name__ == '__main__':
    if len(sys.argv) == 1:
        exit('arg is file or accounts')

    args = sys.argv
    if args[1].endswith('.lst'):
        PAGES = [line.strip() for line in open(args[1]) if line.strip()]
    else:
        PAGES = args[1:]

    auth_data = get_auth_data()

    auth = tweepy.OAuthHandler(auth_data['apiK'], auth_data['apiSK'])
    auth.set_access_token(auth_data['accessT'], auth_data['accessST'])

    api = tweepy.API(auth,wait_on_rate_limit=True)

    load_tweets(api, PAGES)
