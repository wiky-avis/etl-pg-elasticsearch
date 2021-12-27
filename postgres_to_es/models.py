from pydantic import BaseModel
from typing import Optional, List, Dict, Union, Any


class FilmWork(BaseModel):
    id: str
    title: str
    description: Optional[str]
    rating: Optional[float]
    actors_names: Optional[List[str]]
    directors_names: Optional[List[str]]
    writers_names: Optional[List[str]]
    genres_names: Optional[List[str]]
    actors_ids: Optional[str]
    writers_ids: Optional[str]
