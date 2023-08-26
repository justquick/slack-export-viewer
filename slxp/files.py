import json
import sys
from pathlib import Path
import asyncio
from time import perf_counter
import re

import httpx
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from aiofile import async_open
from rich import print


FN_PATTERN = re.compile('[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9].json')

limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
client = httpx.AsyncClient(verify=False, limits=limits)


async def read_json(path):
    async with async_open(path) as f:
        return json.loads(await f.read())


def find_files(data):
    files = {}
    for msg in data:
        for fn in msg.get('files', []):
            if fn.get('mode') == 'tombstone':
                continue
            if 'url_private_download' not in fn:
                print(msg)
                continue
            files[f'{fn["id"]}-{fn["name"]}'] = fn['url_private_download']
    return files


async def download(dest, url):
    bcount = 0
    async with async_open(dest, 'wb') as f:
        async with client.stream('GET', url) as resp:
            async for chunk in resp.aiter_bytes():
                bcount += len(chunk)
                await f.write(chunk)
    return bcount


async def main():
    files = {}
    paths = []
    for path in sys.argv[1:]:
        path = Path(path).relative_to('.')
        if path.suffix == '.json':
            paths.append(path)
        elif path.is_dir():
            paths.extend(path.rglob('*.json'))
        else:
            print(f"No JSON file or folder at: {path}")
            continue
    count = 0
    dltasks = []
    print(f'Scanning {len(paths)} paths for convo files')
    for path in tqdm(paths):
        if FN_PATTERN.match(path.name):
            files[path] = fs = find_files(await read_json(path))
            parent = path.parent / 'files'
            parent.mkdir(parents=True, exist_ok=True)
            dltasks.extend(download(parent / name, url) for name, url in fs.items())
            count += len(fs)
    print(f'Downloading {count} found files...')
    ti = perf_counter()
    mbtotal = sum(await tqdm_asyncio.gather(*dltasks)) / 1024**2
    tf = perf_counter() - ti
    print(f'Downloaded {mbtotal:.2f} MiB in {tf:.2f} seconds, {mbtotal / tf:.2f} MiB/s')


def extract(src, dest):
    'zip -j {src} {glob} -d {dest}'


if __name__ == '__main__':
    asyncio.run(main())
