import logging
import os
from create_table import extract_create_table_statements
from create_table import create_linenums

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

CACHE_DIR = "cache"


def main():
    input_file = "./resources/data.sql"

    if not os.path.exists(input_file):
        logging.error(f"Input file not found: {input_file}")
        return

    logging.info(f"Starting to process {input_file}")

    line_numbers = create_linenums(input_file)
    out = extract_create_table_statements(input_file, line_numbers)
    print(out)
    logging.info("Processing complete")


if __name__ == "__main__":
    main()
