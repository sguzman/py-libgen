import os
import hashlib
import pickle
import logging
import csv
from typing import List, Any, Callable, Optional, TextIO
from functools import wraps

CACHE_DIR = ".cache/util"


def ensure_cache_dir(func_name: str) -> bytes:
    func_cache_dir = os.path.join(CACHE_DIR, func_name)
    if not os.path.exists(func_cache_dir):
        os.makedirs(func_cache_dir)

    return func_cache_dir.encode()


def serialize_args(*args, **kwargs) -> str:
    serialized = f"{str(args)}:{str(kwargs)}"
    return hashlib.md5(serialized.encode()).hexdigest()


def pickle_key(arg: Any) -> bytes:
    return pickle.dumps(arg)


def cache_result(func: Callable):
    @wraps(func)
    def wrapper(*args):
        func_cache_dir: str = ensure_cache_dir(func.__name__)
        cache_key = hashlib.md5(pickle_key(args)).hexdigest().encode()
        cache_file: str = os.path.join(func_cache_dir, cache_key)

        if os.path.exists(cache_file):
            f = open(cache_file, "rb")
            logging.debug(f"Cache hit for {func.__name__}")
            content = f.read()
            f.close()
            p = pickle.loads(content)
            return p
        else:
            logging.info(f"Cache miss for {func.__name__}")

        result = func(*args)

        f = open(cache_file, "wb")
        pickle.dump(result, f)
        f.close()
        logging.info(f"Cached result for {func.__name__}")

        return result

    return wrapper


def write_to_file(file_path: str, content: str) -> None:
    logging.info(f"Writing content to {file_path}")
    with open(file_path, "w") as f:
        f.write(content)


def append_to_csv(file_path: str, rows: List[List[Any]]) -> None:
    logging.info(f"Appending {len(rows)} rows to {file_path}")
    with open(file_path, "a", newline="") as csvf:
        writer = csv.writer(csvf)
        writer.writerows(rows)


def create_csv_with_headers(file_path: str, headers: List[str]) -> None:
    logging.info(f"Creating new CSV file with headers: {file_path}")
    with open(file_path, "w", newline="") as csvf:
        csv.writer(csvf).writerow(headers)


def get_line(file: TextIO) -> Optional[str]:
    try:
        return file.readline()
    except Exception as e:
        logging.debug(f"Error reading line: {e}")
        return None


@cache_result
def prefix_filter(input_file: str, prefix: str) -> List[int]:
    line_numbers = []
    file = open(input_file, "r")

    line_number = 1
    while True:
        line = get_line(file)
        if not line:  # End of file
            break
        if line.startswith(prefix):
            logging.debug(f"Found {prefix} at line {line_number}")
            line_numbers.append(line_number)

        line_number += 1

    file.close()
    return line_numbers


def skip_lines(input_file: str, n: int):
    """
    Skip n lines of a file buffer that is read line by line.

    Args:
    file: A file object
    n: Number of lines to skip

    Returns:
    The file object after skipping n lines
    """
    file = open(input_file, "r")
    for _ in range(n):
        next(file, None)
    return file
