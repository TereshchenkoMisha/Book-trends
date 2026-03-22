from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional
from connection import conncet_database
from models import Book

app = FastAPI(title="Books API")


@app.get("/books", response_model=List[Book])
def get_books(
        skip: int = Query(0, ge=0),
        limit: int = Query(50, ge=1, le=100),
        author: Optional[str] = None,
        title: Optional[str] = None,
        genres: Optional[List[str]] = Query(None),
        events: Optional[List[str]] = Query(None),
        year_start: Optional[int] = None,
        year_end: Optional[int] = None
):
    query = "SELECT * FROM books WHERE 1=1"
    params = []
    if author:
        query += " AND author LIKE ?"
        params.append(f"%{author}%")
    if title:
        query += " AND title LIKE ?"
        params.append(f"%{title}%")
    if genres:
        unique_genres = list(set(genres))
        placeholders = ','.join(['?'] * len(genres))
        query += f"""
            AND id IN (
                SELECT book_id
                FROM books_genres
                WHERE genre_id IN (
                    SELECT id FROM genres WHERE genre IN ({placeholders})
                )
                GROUP BY book_id
                HAVING COUNT(DISTINCT genre_id) = ?
            )
        """
        params.extend(unique_genres)
        params.append(len(unique_genres))
    if events:
        unique_events = list(set(events))
        placeholders = ','.join(['?'] * len(events))
        query += f"""
            AND id IN (
                SELECT book_id
                FROM books_events
                WHERE event_id IN (
                    SELECT id FROM events WHERE event IN ({placeholders})
                )
            )
        """
        params.extend(unique_events)
        params.append(len(unique_events))
    if year_start:
        query += f" AND publication_year >= ?"
        params.append(year_start)
    if year_end:
        query += f" AND publication_year <= ?"
        params.append(year_end)
    if year_start or year_end:
        query += f" AND NOT publication_year = ?"
        params.append(-1 * 10e6)

    query += " LIMIT ? OFFSET ?"
    params.extend([limit, skip])

    with conncet_database() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [Book.from_row(row) for row in rows]


@app.get("/books/{book_id}", response_model=Book)
def get_book(book_id: int):
    with conncet_database() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM books WHERE book_id = ?", (book_id,))
        row = cursor.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Book not found")
        return Book.from_row(row)
