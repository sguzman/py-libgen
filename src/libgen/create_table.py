import logging
from typing import List, Any, Callable
from functools import wraps
import util
import pickle
import hashlib
import os

SQL_FILE = "./tables.sql"

CACHE_DIR = ".cache/create_table"


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
def read_lines_range(input_file: str, start: int, end: int) -> List[str]:
    """
    Read lines from a file within a specified range.

    Args:
    input_file: The path to the input file
    start: The starting line number (inclusive)
    end: The ending line number (inclusive)

    Returns:
    A list of strings containing the lines within the specified range
    """
    logging.info(f"Reading lines {start} to {end} from {input_file}")

    lines = []
    with open(input_file, "r") as file:
        # Skip to the start line
        file = util.skip_lines(input_file, start - 1)
        diff = end - start + 1

        # Read lines within the range
        for _ in range(diff):
            line = util.get_line(file)
            if not line:  # End of file
                break
            lines.append(line.rstrip("\n"))

    logging.info(f"Read {len(lines)} lines")
    return lines


@cache_result
def find_sql_termination(input_file: str, start_offset: int) -> int:
    """
    Find the line number that terminates the SQL expression starting from a given offset.

    Args:
    input_file: The path to the input file
    start_offset: The line number to start searching from

    Returns:
    The line number of the first line ending with a semicolon after the start_offset
    """
    logging.info(f"Finding SQL termination from line {start_offset} in {input_file}")

    with open(input_file, "r") as file:
        # Skip to the start offset
        for _ in range(start_offset - 1):
            _ = util.get_line(file)

        # Search for the terminating line
        for line_number, line in enumerate(file, start=start_offset):
            if line.strip().endswith(";"):
                logging.info(f"Found SQL termination at line {line_number}")
                return line_number

    logging.warning("No SQL termination found")
    return -1  # Return -1 if no termination is found


@cache_result
def extract_create_table_statements(
    input_file: str, line_numbers: List[int]
) -> List[List[str]]:
    logging.info(f"Extracting CREATE TABLE statements from {input_file}")

    create_table_statements = []
    for line_number in line_numbers:
        term = find_sql_termination(input_file, line_number)
        stmt = read_lines_range(input_file, line_number, term)
        create_table_statements.append(stmt)

    logging.info(f"Extracted {len(create_table_statements)} CREATE TABLE statements")
    return create_table_statements


@cache_result
def script_from_table(input_file: str, table_name: str) -> List[str]:
    """
    Get the create table statement of table_name from the input file.
    """
    line_numbers = create_linenums(input_file)
    create_table_statements: List[List[str]] = extract_create_table_statements(
        input_file, line_numbers
    )
    ss = scripts_ss(create_table_statements)

    for s in ss:
        if s.startswith(f"CREATE TABLE `{table_name}`"):
            return s


@cache_result
def create_linenums(input_file: str) -> List[int]:
    logging.info("Finding CREATE TABLE statements")
    statements: List[int] = util.prefix_filter(input_file, "CREATE TABLE")

    logging.info(f"Found {len(statements)} CREATE TABLE statements")
    return statements


@cache_result
def extract_table_names(create_table_statements: List[str]) -> List[str]:
    """
    Extract table names from CREATE TABLE statements.

    Args:
    create_table_statements: A list of CREATE TABLE SQL statements

    Returns:
    A list of table names extracted from the statements
    """
    logging.info("Extracting table names from CREATE TABLE statements")
    table_names = []
    for statement in create_table_statements:
        # Split the statement and find the table name
        # It's typically the third word in a CREATE TABLE statement
        words = statement[0].split()
        if (
            len(words) >= 3
            and words[0].upper() == "CREATE"
            and words[1].upper() == "TABLE"
        ):
            # Remove any backticks or quotes from the table name
            table_name = words[2].strip('`"')
            table_names.append(table_name)

    logging.info(f"Extracted {len(table_names)} table names")
    return table_names


@cache_result
def get_tables(input_file: str) -> List[str]:
    """
    Get all table names from CREATE TABLE statements in the input file.

    Args:
    input_file: The path to the input file

    Returns:
    A list of table names
    """
    logging.info(f"Getting tables from {input_file}")
    line_numbers = create_linenums(input_file)
    create_table_statements = extract_create_table_statements(input_file, line_numbers)
    table_names = extract_table_names(create_table_statements)

    logging.info(f"Found {len(table_names)} tables")
    return table_names


@cache_result
def scripts(input_file: str) -> List[List[str]]:
    """
    Get the create table statements from the input file.
    """
    logging.info(f"Getting create table statements from {input_file}")
    line_numbers = create_linenums(input_file)
    create_table_statements: List[List[str]] = extract_create_table_statements(
        input_file, line_numbers
    )
    return create_table_statements


@cache_result
def scripts_ss(create_table_statements: List[List[str]]) -> List[str]:
    """
    Format create table statements for writing to a file.
    """
    logging.info("Formatting create table statements")
    formatted_statements = []
    for stmt in create_table_statements:
        formatted_statements.append("".join(stmt))

    return formatted_statements


@cache_result
def scripts_format(create_table_statements: List[List[str]]) -> str:
    """
    Format create table statements for writing to a file.
    """
    ss = scripts_ss(create_table_statements)

    return "\n".join(ss)


def update(input_file: str):
    """
    Write create table statements to a table.sql file.
    """
    logging.info(f"Updating {SQL_FILE}")
    create_table_statements: List[List[str]] = scripts(input_file)
    tables = open(SQL_FILE, "w")

    tables.write(scripts_format(create_table_statements))

    tables.close()

    logging.info(f"Updated {SQL_FILE}")
