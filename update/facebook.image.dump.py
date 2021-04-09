import io
import os
import sys
import json
import glob
import urllib.request

import gevent
from gevent import monkey
from gevent.pool import Pool
monkey.patch_socket()


def load_data(file_name):
    with io.open(file_name, encoding='utf-8') as f:
        data = sorted(
            [json.loads(line) for line in f],
            key = lambda _: _['time'],
            reverse = True
        )

        return data

def do_download(url, image_name):
    _try = 0

    while _try < 3:
        try:
            urllib.request.urlretrieve(url, image_name)
            break
        except Exception as e:
            print('Cant download `{}` to `{}` ({}/3):\n{}'.format(
                url, image_name, _try + 1, e
            ))
            _try = _try + 1


if __name__ == '__main__':
    gpool = Pool(3)

    if len(sys.argv) > 1:
        files = [sys.argv[1]]
    else:
        files = glob.glob('./facebook/posts/*.json')

    for file_name in files:
        data = load_data(file_name)
        data = filter(lambda _: 'image' in _ and _['image'], data)

        base_path = os.path.splitext(os.path.basename(file_name))[0]
        base_path = './facebook/images/{}'.format(base_path)

        if not os.path.exists(base_path):
            os.mkdir(base_path)

        for post in data:
            if os.path.exists('{}/{}_0.png'.format(base_path, post['post_id'])):
                break

            for idx, image in enumerate(post['images']):
                image_name = '{}/{}_{}.png'.format(base_path, post['post_id'], idx)
                gpool.spawn(do_download, image, image_name)

        gpool.join()
