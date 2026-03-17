
import requests
import time
import random
import logging
import threading
import queue
import csv
import os
import json
import re
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s', datefmt='%H:%M:%S')

DATASET_FILE = "data/raw/goodreads_dataset.csv"
MAX_WORKERS = 4
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Edge/121.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

FIELDNAMES = [
    'book_id', 'title', 'author', 'rating', 'ratings_count', 'reviews_count',
    'description', 'genres', 'pages', 'format', 'publication_date',
    'literary_awards', 'original_title', 'series', 'setting', 'characters',
    'isbn', 'language'
]

data_queue = queue.Queue()


def clean_html(raw_html):
    if not raw_html: return ""
    clean = re.sub(r'<.*?>', '', raw_html)
    return " ".join(clean.split())


def csv_writer_worker():
    file_exists = os.path.exists(DATASET_FILE)
    with open(DATASET_FILE, 'a', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        while True:
            item = data_queue.get()
            if item is None: break
            writer.writerow(item)
            f.flush()
            data_queue.task_done()



def fetch_book(book_id):
    url = f"https://www.goodreads.com/book/show/{book_id}"
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    try:
        time.sleep(random.uniform(2, 4))
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200: return None

        match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', response.text,
                          re.DOTALL)
        if not match: return None

        apollo = json.loads(match.group(1))['props']['pageProps']['apolloState']

      
        book_key = next((k for k, v in apollo.items() if k.startswith('Book:') and v.get('title')), None)
        if not book_key: return None
        book = apollo[book_key]

       
        details = book.get('details', {})
        if isinstance(details, dict) and details.get('__ref'):
            details = apollo.get(details['__ref'], {})

       
        work_ref = book.get('work', {}).get('__ref')
        work = apollo.get(work_ref, {}) if work_ref else {}

        work_details = work.get('details', {})
        if isinstance(work_details, dict) and work_details.get('__ref'):
            work_details = apollo.get(work_details['__ref'], {})

        stats_data = work.get('stats', {})
        if isinstance(stats_data, dict) and stats_data.get('__ref'):
            stats_data = apollo.get(stats_data['__ref'], {})

        pub_time = details.get('publicationTime') or work_details.get('publicationTime')
        pub_date_str = ""
        if pub_time:
          
            pub_date_str = time.strftime('%B %d, %Y', time.gmtime(pub_time / 1000))

    
        res = {
            'book_id': book_id,
            'title': book.get('title', ''),
            'author': "Unknown",
            'rating': stats_data.get('averageRating', 0),
            'ratings_count': stats_data.get('ratingsCount', 0),
            'reviews_count': stats_data.get('textReviewsCount', 0),
            'description': clean_html(book.get('description', '')),
            'genres': ", ".join(
                filter(None, [g.get('genre', {}).get('name') for g in book.get('bookGenres', []) if g.get('genre')])),
            'pages': details.get('numPages', ''),
            'format': details.get('format', ''),
            'publication_date': pub_date_str,
            'literary_awards': ", ".join([a.get('name') for a in work_details.get('awardsWon', [])]) if isinstance(
                work_details, dict) and work_details.get('awardsWon') else '',
            'original_title': work_details.get('originalTitle', '') if isinstance(work_details, dict) else '',
            'series': "",
            'setting': ", ".join([p.get('name') for p in work_details.get('places', [])]) if isinstance(work_details,
                                                                                                        dict) and work_details.get(
                'places') else '',
            'characters': ", ".join([c.get('name') for c in work_details.get('characters', [])]) if isinstance(
                work_details, dict) and work_details.get('characters') else '',
            'isbn': details.get('isbn13') or details.get('isbn', ''),
            'language': details.get('language', {}).get('name', '') if isinstance(details.get('language'), dict) else ''
        }

    
        auth_node = book.get('primaryContributorEdge', {}).get('node', {})
        auth_ref = auth_node.get('__ref') if auth_node else None
        if auth_ref and auth_ref in apollo:
            res['author'] = apollo[auth_ref].get('name', 'Unknown')

        
        if book.get('bookSeries'):
            s_ref = book['bookSeries'][0].get('series', {}).get('__ref')
            if s_ref and s_ref in apollo:
                res['series'] = apollo[s_ref].get('title', '')

        return res

    except Exception:
        return None


def gr_scraper_main():
    threading.Thread(target=csv_writer_worker, daemon=True).start()
    curr_id = 103517

    logging.info(f" (ID: {curr_id})...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while True:
            batch = range(curr_id, curr_id + 10)
            futures = {executor.submit(fetch_book, i): i for i in batch}
            for f in futures:
                r = f.result()
                if r:
                    data_queue.put(r)
                    logging.info(f" {r['book_id']}: {r['title'][:30]} | {r['pages']} p. | {r['format']}")
            curr_id += 10


if __name__ == "__main__":
    gr_scraper_main()