import re
import logging
from util import cache_result, create_csv_with_headers
from typing import List, Dict
from util import prefix_filter


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


@cache_result
def extract_create_table_statements(
    input_file: str, line_numbers: List[int]
) -> List[str]:
    logging.info(f"Extracting CREATE TABLE statements from {input_file}")
    with open(input_file, "r") as file:
        content = file.readlines()

    create_table_statements = []
    for line_number in line_numbers:
        statement = []
        i = line_number
        while i < len(content) and not content[i].strip().endswith(";"):
            statement.append(content[i])
            i += 1
        if i < len(content):
            statement.append(content[i])  # Include the last line with semicolon
        create_table_statements.append("".join(statement))

    logging.info(f"Extracted {len(create_table_statements)} CREATE TABLE statements")
    return create_table_statements


@cache_result
def create_linenums(input_file: str) -> List[int]:
    logging.info("Finding CREATE TABLE statements")
    statements: List[int] = prefix_filter(input_file, "CREATE TABLE")

    logging.info(f"Found {len(statements)} CREATE TABLE statements")
    return statements
