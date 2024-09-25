import requests
from pprint import pprint
import time

import psycopg2
from typing import Optional
from sqlalchemy import MetaData, Integer, String, TIMESTAMP, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from datetime import datetime

import os

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_NAME = os.getenv("DB_NAME")
DB_PASS = os.getenv("DB_PASS")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)
engine.connect()


metadata = MetaData()

class Base(DeclarativeBase):
    metadata = MetaData()

class Posted(Base):
    __tablename__ = 'posted'
    id: Mapped[str] = mapped_column(String, primary_key=True)
    author: Mapped[str] = mapped_column(String)
    title: Mapped[str] = mapped_column(String)
    utc: Mapped[datetime] = mapped_column(TIMESTAMP)
    comments: Mapped[int] = mapped_column(Integer)
    score: Mapped[int] = mapped_column(Integer)
    url: Mapped[Optional[str]] = mapped_column(String, nullable=True)



def parse(subreddit, after=''):
    url_template = 'https://www.reddit.com/r/{}/top.json?t=all{}'

    headers = {
    'Uset-Agent': 'VirtboxBot'
    }

    params = f'&afler={after}' if after else ''
    while True:
        url = url_template.format(subreddit, params)
        response = requests.get(url, headers=headers)

        if response.ok:
            data = response.json()['data']
            for post in data['children']:
                pdata = post['data']
                post_id =  pdata['id']
                post_author = pdata['author']
                post_title = pdata['title']
                post_utc = pdata['created_utc']
                post_comm = pdata['num_comments']
                post_score = pdata['score']
                post_url = pdata.get('url_overriden_by_dest')
                pprint({'id': post_id,
                    'author': post_author,
                    'title': post_title,
                    'created_utc': post_utc,
                    'num_comments': post_comm,
                    'score': post_score,
                    'url': post_url})
            return data['after']
        else:
            print(f'Error {response.status_code}')
            return None

def main():
    subreddit = 'golang'
    after = ''
    while True:
        after = parse(subreddit, after)
        time.sleep(1)
        if not after:
            break
    
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('exiting...')