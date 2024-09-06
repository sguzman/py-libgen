import os
import hashlib
import pickle
import logging
from typing import List, Any, Callable
from functools import wraps
import util
import create_table
import sqlglot
import csv
import multiprocessing
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

        result = func(*args)

        f = open(cache_file, "wb")
        pickle.dump(result, f)
        f.close()

        return result

    return wrapper


def get_nth_line(input_file: str, n: int) -> str:
    """
    Get the nth line from the input file.

    Args:
    input_file: The path to the input file
    n: The line number to retrieve (1-based index)

    Returns:
    The content of the nth line as a string
    """
    logging.info(f"Line {n}")

    # Skip n-1 lines to get to the desired line
    file = util.skip_lines(input_file, n - 1)

    # Read the nth line
    line = file.readline().strip()

    return line

@cache_result
def row(input_file: str, row_id: int, column_size: int) -> List[Any]:
    """
    Get a single row from the input file based on the row ID.

    Args:
    input_file: The path to the input file
    row_id: The ID of the row to retrieve
    column_size: The number of columns in the table
    Returns:
    A list containing the values of the specified row
    """
    logging.debug(f"Getting row with ID {row_id} from {input_file}")

    line: str = get_nth_line(input_file, row_id)
    prefix_idx = line.index("VALUES") + len("VALUES")
    prefix_str = line[:prefix_idx].strip()
    data_str = line[prefix_idx:].strip()
    vs = data_str.split("),(")

    prefix_str = prefix_str.replace("`", "")
    data = []
    for v in vs:
        v = v.strip("(); ")
        stmt: str = f'{prefix_str} ({v});'
        try:
            s = sqlglot.parse_one(stmt)
            d = []
            for a in s.find(sqlglot.exp.Values).find_all(sqlglot.exp.Literal):
                if a.this == '':
                    d.append(None)
                else:
                    d.append(a.this)

            if len(d) == column_size:
                data.append(d)
        except Exception as e:
            logging.debug(f"Error parsing {stmt}: {e}")
    return data

def row_wrapper(args):
    return row(*args)

@cache_result
def rows(input_file: str, ids: List[int], column_size: int) -> List[List[Any]]:
    """
    Extract rows from the input file for the given list of row IDs.

    Args:
    input_file: The path to the input file
    ids: A list of row IDs to extract

    Returns:
    A list of rows, where each row is a list of values
    """
    from multiprocessing import Pool
    logging.info(f"Extracting rows with IDs {len(ids)} from {input_file}")

    rs = []

    num_cores = multiprocessing.cpu_count()
    pool = Pool(processes=num_cores)
    args1 = [input_file] * len(ids)
    args2 = [column_size] * len(ids)
    z = zip(args1, ids, args2)
    rs = pool.map(row_wrapper, z)

    flat = [item for sublist in rs for item in sublist]

    logging.info(f"Extracted {len(flat)} lines")
    return flat

@cache_result
def columns_from_str(ss: str) -> List[str]:
    columns = []
    split: List[str] = ss.split()
    slice: List[str] = split[split.index('(')+1:split.index('PRIMARY')-1]
    for token in slice:
        if (
            token.startswith("`")
            and token.endswith("`")
            and all(map(lambda s: s.isalnum() or s == "_", token[1:-1]))
        ):
            columns.append(token[1:-1])
    return columns


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

    ss = create_table.script_from_table(input_file, table_name)
    columns = columns_from_str(ss)
    logging.info(f"{table_name}: {columns}")
    return columns[1:]


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

def write_csv(name: str, headers: List[str], rows: List[List[Any]]):
    full_name = f"{name}.csv"
    logging.info(f"Writing {len(rows)} rows to {full_name}")
    with open(full_name, "w") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)

def update(input_file: str, table_name: str):
    """
    Update the insert statements for a specific table in the input file.
    """
    row_ids = find_insert_statements(input_file, table_name)
    headers = get_table_columns(input_file, table_name)
    rs = rows(input_file, row_ids, column_size)
    
    logging.info(f"{table_name}: {len(rs)} rows")
    write_csv(table_name, headers, rs)

