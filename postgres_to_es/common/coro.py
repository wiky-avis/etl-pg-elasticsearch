from typing import Tuple

from base.decorators import coroutine

from postgres_to_es.models.movies import FilmWork


@coroutine
def transform_coroutine(target) -> Tuple[FilmWork]:
    pass
