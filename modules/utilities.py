import logging
from functools import wraps
from time import perf_counter
from pathlib import Path

def timer(func):
    '''Print the runtime of the decorated function'''
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = perf_counter()
        result = func(*args, **kwargs)
        stop = perf_counter()
        logging.info('running %s took %f.3 s', func.__name__, stop - start)
        if result:
            return result
        else:
            return
    return wrapper


def get_activities_list(folder_name):
    '''
    Get a list of garmin .tcx files from specified folder in current dir

    Args: folder(str)
    Returns: list(Path)
    '''
    dir = Path.cwd() / folder_name
    files = list(dir.glob('*.tcx'))
    activities_list = sorted(files, key=lambda path: int(path.stem.rsplit('_', 1)[1]))
    
    return activities_list

    