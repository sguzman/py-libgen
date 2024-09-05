import re
import csv
import logging
import os
import shelve
import hashlib
from typing import List, Dict, Any
from sqlglot import exp, parse_one
from functools import lru_cache

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

CACHE_FILE = "cache_db"


def get_file_checksum(file_path: str) -> str:
    logging.info(f"Calculating checksum for {file_path}")
    with open(file_path, "rb") as f:
        file_hash = hashlib.md5()
        chunk = f.read(8192)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(8192)
    return file_hash.hexdigest()


def cache_result(func):
    def wrapper(*args, **kwargs):
        with shelve.open(CACHE_FILE) as cache:
            key = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            if key in cache:
                logging.info(f"Cache hit for {func.__name__}")
                return cache[key]
            result = func(*args, **kwargs)
            cache[key] = result
            logging.info(f"Cached result for {func.__name__}")
        return result

    return wrapper


@cache_result
def read_file_content(file_path: str) -> str:
    logging.info(f"Reading content from {file_path}")
    with open(file_path, "r") as f:
        return f.read()


def write_to_file(file_path: str, content: str) -> None:
    logging.info(f"Writing content to {file_path}")
    with open(file_path, "w") as f:
        f.write(content)


def append_to_csv(file_path: str, rows: List[List[Any]]) -> None:
    logging.info(f"Appending {len(rows)} rows to {file_path}")
    with open(file_path, "a", newline="") as csvf:
        writer = csv.writer(csvf)
        writer.writerows(rows)


@lru_cache(maxsize=None)
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


@lru_cache(maxsize=None)
def extract_table_name(statement: str) -> str:
    return re.search(r"CREATE TABLE `?(\w+)`?", statement, re.IGNORECASE).group(1)


def create_csv_with_headers(file_path: str, headers: List[str]) -> None:
    logging.info(f"Creating new CSV file with headers: {file_path}")
    with open(file_path, "w", newline="") as csvf:
        csv.writer(csvf).writerow(headers)


@cache_result
def process_create_statement(statement: str) -> Dict[str, List[str]]:
    table_name = extract_table_name(statement)
    logging.info(f"Processing table: {table_name}")
    columns = extract_column_names(statement)
    create_csv_with_headers(f"{table_name}.csv", columns)
    return {table_name: columns}


@cache_result
def process_create_statements(input_file: str) -> Dict[str, List[str]]:
    logging.info(f"Processing CREATE TABLE statements from {input_file}")
    content = read_file_content(input_file)
    create_table_statements = find_create_table_statements(content)

    write_to_file("tables.sql", "\n\n".join(create_table_statements) + "\n\n")

    table_columns = {}
    for statement in create_table_statements:
        table_columns.update(process_create_statement(statement))

    logging.info(f"Processed {len(create_table_statements)} CREATE TABLE statement(s)")
    return table_columns


@lru_cache(maxsize=None)
def parse_single_insert_value(value: str, prefix: str) -> List[Any]:
    single = f"{prefix.replace('`', '')} VALUES ({value.replace('(', '')});"
    stmt = parse_one(single)
    data = [x.this for x in stmt.find(exp.Values).find_all(exp.Literal)]
    return [None if x == "" else x for x in data]


@cache_result
def parse_insert_values(string: str) -> List[List[Any]]:
    logging.info("Parsing INSERT statement values")
    prefix = string[: string.index("VALUES")]
    raw_values = string[string.index("VALUES") + 6 :].split("),(")

    parsed_rows = []
    for value in raw_values:
        try:
            parsed_rows.append(parse_single_insert_value(value, prefix))
        except Exception as e:
            logging.debug(f"Error parsing INSERT statement: {e}")

    logging.info(f"Parsed {len(parsed_rows)} rows from INSERT statement")
    return parsed_rows


@cache_result
def process_single_insert(statement: str, table_columns: Dict[str, List[str]]) -> None:
    table_name = statement[: statement.index("VALUES")].split()[2].replace("`", "")
    logging.info(f"Processing INSERT for table: {table_name}")

    if table_name not in table_columns:
        logging.warning(f"Skipping INSERT for unknown table: {table_name}")
        return

    csv_file = f"{table_name}.csv"
    rows = parse_insert_values(statement)
    append_to_csv(csv_file, rows)


@cache_result
def process_insert_statements(
    input_file: str, table_columns: Dict[str, List[str]]
) -> None:
    logging.info(f"Processing INSERT statements from {input_file}")
    content = read_file_content(input_file)
    insert_statements = [
        line for line in content.splitlines() if line.startswith("INSERT INTO")
    ]
    logging.info(f"Found {len(insert_statements)} INSERT statements")

    for statement in insert_statements:
        try:
            process_single_insert(statement, table_columns)
        except Exception as e:
            logging.error(f"Error processing INSERT statement: {e}")
            logging.error(f"Problematic statement: {statement[:100]}...")

    logging.info(f"Processed {len(insert_statements)} INSERT statement(s)")


def main():
    input_file = "./resources/data.sql"

    if not os.path.exists(input_file):
        logging.error(f"Input file not found: {input_file}")
        return

    logging.info(f"Starting to process {input_file}")

    file_checksum = get_file_checksum(input_file)

    with shelve.open(CACHE_FILE) as cache:
        if cache.get("input_file_checksum") == file_checksum:
            logging.info("Input file unchanged, using cached results")
            table_columns = cache.get("table_columns", {})
        else:
            logging.info("Input file changed or not cached, processing from scratch")
            table_columns = process_create_statements(input_file)
            process_insert_statements(input_file, table_columns)
            cache["input_file_checksum"] = file_checksum
            cache["table_columns"] = table_columns

    logging.info("Processing complete")


if __name__ == "__main__":
    main()
