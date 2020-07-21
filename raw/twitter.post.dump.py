import io
import os
import json
import twint

from datetime import datetime

def load_data(file_name):
    if not os.path.exists(file_name):
        data = None

    else:
        with io.open(file_name, encoding='utf-8') as f:
            data = sorted(
                [json.loads(line) for line in f],
                key = lambda _: _['created_at'],
                reverse = True
            )

    return data


if __name__ == '__main__':
    if len(sys.argv) == 1:
        exit('arg is file or accounts')

    args = sys.argv
    if args[1].endswith('.lst'):
        PAGES = [line.strip() for line in open(args[1]) if line.strip()]
    else:
        PAGES = args[1:]

    for PAGE in PAGES:
        file_name = './twitter/posts/{}.json'.format(PAGE)
        data = load_data(file_name)

        try:
            config = twint.Config()
            config.Username = PAGE

            if data:
                timestamp = data[0]['created_at']
                timestamp = datetime.fromtimestamp((timestamp / 1000) + 1)
                config.Since = timestamp.strftime('%Y-%m-%d %H:%M:%S')

            else:
                config.Since = '2020-03-10'


            config.Store_json = True
            config.Output = file_name

            twint.run.Search(config)

        except Exception as e:
            raise(e)
            pass
