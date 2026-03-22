from pydantic import BaseModel
from typing import Optional, List
import json


class Book(BaseModel):
    id: int
    book_id: int
    title: Optional[str] = None
    author: Optional[str] = None
    rating: Optional[float] = None
    ratings_count: Optional[int] = None
    reviews_count: Optional[int] = None
    genres: Optional[List[str]] = None
    pages: Optional[float] = None
    format: Optional[str] = None
    publication_date: Optional[str] = None
    literary_awards: Optional[str] = None
    series: Optional[str] = None
    setting: Optional[str] = None
    characters: Optional[List[str]] = None
    language: Optional[str] = None
    country: Optional[str] = None
    historical_events: Optional[List[str]] = None
    historical_figures: Optional[List[str]] = None
    time_period: Optional[str] = None

    @classmethod
    def from_row(cls, row):
        data = dict(row)
        for field in ["genres", "characters", "historical_events", "historical_figures"]:
            val = data.get(field)
            if val and isinstance(val, str):
                try:
                    data[field] = json.loads(val)
                except json.JSONDecodeError:
                    data[field] = None
        for key, value in data.items():
            if isinstance(value, str) and value == '':
                data[key] = None
        return cls(**data)
