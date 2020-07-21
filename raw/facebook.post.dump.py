import io
import os
import sys
import json
import time
import datetime
import facebook_scraper

LIMIT = time.mktime(datetime.datetime(2020, 3, 10).timetuple())

def load_data(file_name):
    if not os.path.exists(file_name):
        data = [{'time': 0}]

    else:
        with io.open(file_name, encoding='utf-8') as f:
            data = sorted(
                [json.loads(line) for line in f],
                key = lambda _: _['time']
            )

    return data

def write_data(file_name, data):
    with io.open(file_name, mode='a', encoding='utf-8') as f:
        f.write(u'{}\n'.format(
            json.dumps(data, ensure_ascii=False)
        ))

if __name__ == '__main__':
    if len(sys.argv) == 1:
        exit('arg is file or accounts')

    args = sys.argv
    if args[1].endswith('.lst'):
        PAGES = [line.strip() for line in open(args[1]) if line.strip()]
    else:
        PAGES = args[1:]

    for PAGE in PAGES:
        file_name = './facebook/posts/{}.json'.format(PAGE)
        data = load_data(file_name)

        try:
            for post in facebook_scraper.get_posts(
                PAGE, timeout=10, sleep=5, pages=10000
            ):
                if 'time' not in post or post['time'] is None:
                    continue

                post['time'] = time.mktime(post['time'].timetuple())
                if post['time'] <= data[-1]['time'] or post['time'] <= LIMIT:
                    break

                write_data(file_name, post)
        except Exception as e:
            pass
