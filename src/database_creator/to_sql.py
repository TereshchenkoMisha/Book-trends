import pandas as pd
import sqlite3
import json
import os

all_genres = [
    "Fiction",
    "Science Fiction",
    "Fantasy",
    "Mystery & Thriller",
    "Romance",
    "Historical Fiction",
    "Biography & Memoir",
    "Non-Fiction",
    "Philosophy & Religion",
    "Poetry & Drama",
    "Children",
    "Young Adult",
    "Horror",
    "Adventure",
    "War",
    "Short Stories",
    "Other"
]

genre_map = dict()
genres_series = []


def create_genre(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS genres (
        id INTEGER PRIMARY KEY,
        genre TEXT NOT NULL
        )
        ''')


def create_event(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY,
        event TEXT NOT NULL
        )
        ''')


def create_book_genre(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS books_genres (
        book_id INTEGER NOT NULL,
        genre_id INTEGER NOT NULL,
        PRIMARY KEY(book_id, genre_id)
        )
        ''')


def create_book_event(cursor):
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS books_events (
            book_id INTEGER NOT NULL,
            event_id INTEGER NOT NULL,
            PRIMARY KEY(book_id, event_id)
            )
            ''')


def insert_genres(cursor):
    ind = 1
    for cur_genre in all_genres:
        genre_map[cur_genre] = ind
        cursor.execute('INSERT OR IGNORE INTO genres (genre) VALUES (?)', (cur_genre,))
        ind += 1


def insert_book_genre(cursor):
    for book_id, row in df.iterrows():
        genres_list = genres_series.loc[book_id]

        if isinstance(genres_list, str):
            try:
                genres_list = json.loads(genres_list)
            except:
                genres_list = []

        if not isinstance(genres_list, list):
            continue

        for genre in genres_list:
            genre_id = genre_map.get(genre)
            if genre_id is None:
                cursor.execute('INSERT OR IGNORE INTO genres (genre) VALUES (?)', (genre,))
                genre_id = cursor.lastrowid
                genre_map[genre] = genre_id
            cursor.execute('INSERT OR IGNORE INTO books_genres (book_id, genre_id) VALUES (?, ?)',
                           (book_id, genre_id))


def insert_book_event(cursor):
    event_map = {}
    for book_id, row in df.iterrows():
        events_list = events_series.loc[book_id]
        if isinstance(events_list, str):
            try:
                events_list = json.loads(events_list)
            except:
                events_list = []
        if not isinstance(events_list, list):
            continue
        unique_events = list(set(events_list))
        for event in unique_events:
            if event not in event_map:
                cursor.execute('INSERT OR IGNORE INTO events (event) VALUES (?)', (event,))
                cursor.execute('SELECT id FROM events WHERE event = ?', (event,))
                event_id = cursor.fetchone()[0]
                event_map[event] = event_id
            else:
                event_id = event_map[event]
            cursor.execute('INSERT INTO books_events (book_id, event_id) VALUES (?, ?)',
                           (book_id, event_id))


if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    json_path = os.path.join(BASE_DIR, "data", "processed", "books_enriched_clean.json")
    db_path = os.path.join(BASE_DIR, "data", "database", "books.db")

    if not os.path.exists(json_path):
        print(f"Файл {json_path} не найден")
        exit(1)

    with open(json_path, "r", encoding="utf-8") as f:
        text = f.read().replace("NaN", "null")
        data = json.loads(text)

    if isinstance(data, dict):
        data = [data]
    df = pd.DataFrame(data)
    dtype = {'publication_date': 'DATE', 'publication_year': 'INTEGER', 'index': 'INTEGER'}


    def convert_to_json(x):
        if isinstance(x, (list, dict)):
            return json.dumps(x, ensure_ascii=False)
        return x


    def get_years(data):
        years = []
        for date in data["publication_date"]:
            if isinstance(date, str):
                date = date.split(',')[1].strip()
                years.append(int(date))
            else:
                years.append(-10e6)
        return years


    df.insert(1, 'id', [i for i in range(1, len(df["book_id"]) + 1)], False)
    df.insert(11, "publication_year", get_years(df), True)
    df.reset_index()
    df.set_index('id', inplace=True)

    if 'genres' in df.columns:
        genres_series = df['genres'].copy()
    else:
        genres_series = pd.Series([[] for _ in range(len(df))], index=df.index)
        print("Внимание: столбец 'genres' отсутствует в данных")

    if 'historical_events' in df.columns:
        events_series = df['historical_events'].copy()
    else:
        events_series = pd.Series([[] for _ in range(len(df))], index=df.index)
        print("Внимание: столбец 'historical_events' отсутствует в данных")

    df = df.apply(lambda col: col.map(convert_to_json))
    df = df.fillna("")
    df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS books_genres")
    cursor.execute("DROP TABLE IF EXISTS books_events")

    create_genre(cursor)
    create_book_genre(cursor)
    create_event(cursor)
    create_book_event(cursor)

    insert_genres(cursor)
    insert_book_genre(cursor)
    insert_book_event(cursor)

    conn.commit()
    df.to_sql("books", conn, if_exists="replace", index=True, dtype=dtype)
    conn.close()
