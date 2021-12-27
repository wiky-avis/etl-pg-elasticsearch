import logging
from functools import wraps
from time import sleep


logger = logging.getLogger(__name__)


def coroutine(func):
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn

    return inner


def backoff(jitter=0.1, factor=2, max_delay=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = jitter
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as err:
                    logger.error(err)
                    sleep(t)
                    if t < max_delay:
                        t += jitter * 2 ** (factor)
                    else:
                        t = max_delay

        return inner

    return func_wrapper
