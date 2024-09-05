import os
import hashlib
import pickle
import logging
from typing import List, Any, Callable
from functools import wraps
import util
import create_table

CACHE_DIR = ".cache/insert_statement"


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


@cache_result
def get_table_columns(input_file: str, table_name: str) -> List[str]:
    """
    Extract the column names for a given table from the CREATE TABLE statement in the input file.

    Args:
    input_file: The path to the input file
    table_name: The name of the table to find columns for

    Returns:
    A list of column names for the specified table
    """
    logging.info(f"Extracting columns for table '{table_name}' from {input_file}")

    create_table_lines = util.prefix_filter(input_file, f"CREATE TABLE `{table_name}`")

    if not create_table_lines:
        logging.warning(f"CREATE TABLE statement not found for table '{table_name}'")
        return []

    start_line = create_table_lines[0]
    end_line = create_table.find_sql_termination(input_file, start_line)

    create_table_statement = create_table.read_lines_range(
        input_file, start_line, end_line
    )

    columns = []
    for line in create_table_statement[1:]:  # Skip the first line (CREATE TABLE)
        line = line.strip()
        if line.startswith("`"):
            column_name = line.split("`")[1]
            columns.append(column_name)
        elif line.startswith(")"):  # End of column definitions
            break

    logging.info(f"Extracted {len(columns)} columns for table '{table_name}'")
    return columns


@cache_result
def find_insert_statements(input_file: str, table_name: str) -> List[str]:
    """
    Find all INSERT statements for a specific table in the input file.

    Args:
    input_file: The path to the input file
    table_name: The name of the table to find INSERT statements for

    Returns:
    A list of strings containing the INSERT statements for the specified table
    """
    logging.info(f"Finding INSERT statements for table '{table_name}' in {input_file}")

    insert_statements = util.prefix_filter(input_file, f"INSERT INTO `{table_name}`")

    logging.info(
        f"Found {len(insert_statements)} INSERT statements for table '{table_name}'"
    )
    return insert_statements
