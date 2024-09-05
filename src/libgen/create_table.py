import re
import logging
from util import cache_result, create_csv_with_headers
from typing import List, Dict
from util import prefix_filter

SQL_FILE = "./tables.sql"


@cache_result
def extract_column_names(create_statement: str) -> List[str]:
    logging.info("Extracting column names from CREATE TABLE statement")
    columns = re.findall(r"`(\w+)`", create_statement)
    unique_columns = list(
        dict.fromkeys(columns[1:])
    )  # Remove duplicates while preserving order
    logging.info(f"Extracted columns: {unique_columns}")
    return unique_columns


@cache_result
def find_create_table_statements(content: str) -> List[str]:
    logging.info("Finding CREATE TABLE statements")
    statements = re.findall(r"CREATE TABLE.*?;", content, re.DOTALL | re.IGNORECASE)
    logging.info(f"Found {len(statements)} CREATE TABLE statements")
    return statements


@cache_result
def extract_table_name(statement: str) -> str:
    return re.search(r"CREATE TABLE `?(\w+)`?", statement, re.IGNORECASE).group(1)


@cache_result
def process_create_statement(statement: str) -> Dict[str, List[str]]:
    table_name = extract_table_name(statement)
    logging.info(f"Processing table: {table_name}")
    columns = extract_column_names(statement)
    create_csv_with_headers(f"{table_name}.csv", columns)
    return {table_name: columns}


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
        file = skip_lines(input_file, start - 1)
        diff = end - start + 1

        # Read lines within the range
        for _ in range(diff):
            line = file.readline()
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
            next(file, None)

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
) -> List[str]:
    logging.info(f"Extracting CREATE TABLE statements from {input_file}")

    create_table_statements = []
    for line_number in line_numbers:
        term = find_sql_termination(input_file, line_number)
        stmt = read_lines_range(input_file, line_number, term)
        create_table_statements.append(stmt)

    logging.info(f"Extracted {len(create_table_statements)} CREATE TABLE statements")
    return create_table_statements


@cache_result
def create_linenums(input_file: str) -> List[int]:
    logging.info("Finding CREATE TABLE statements")
    statements: List[int] = prefix_filter(input_file, "CREATE TABLE")

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


def update(input_file: str):
    """
    Write create table statements to a table.sql file.
    """
    logging.info(f"Updating {SQL_FILE}")

    line_numbers: List[int] = create_linenums(input_file)
    create_table_statements: List[str] = extract_create_table_statements(
        input_file, line_numbers
    )
    tables = open(SQL_FILE, "w")

    for stmt in create_table_statements:
        for line in stmt:
            tables.write(line)
            tables.write("\n")
        tables.write("\n")

    tables.close()

    logging.info(f"Updated {SQL_FILE}")
