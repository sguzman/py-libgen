import logging
from util import cache_result, read_file_content, append_to_csv
from typing import List, Dict, Any
from sqlglot import exp, parse_one


@cache_result
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
