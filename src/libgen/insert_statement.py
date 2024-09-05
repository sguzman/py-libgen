import logging
from util import cache_result
from typing import List


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

    insert_statements = []
    with open(input_file, "r") as file:
        for line in file:
            if line.startswith(f"INSERT INTO `{table_name}`"):
                insert_statements.append(line.strip())

    logging.info(
        f"Found {len(insert_statements)} INSERT statements for table '{table_name}'"
    )
    return insert_statements
