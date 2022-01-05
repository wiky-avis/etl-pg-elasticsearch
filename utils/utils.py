import logging
from typing import Any, Hashable, Mapping


logger = logging.getLogger(__name__)


def get_in(m: Mapping, *keys: Hashable) -> Any:
    for key in keys:
        try:
            m = m[key]
        except (TypeError, KeyError, IndexError):
            return
    return m
