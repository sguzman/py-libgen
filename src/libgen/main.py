import logging
import os
import create_table

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    input_file = "./resources/data.sql"

    if not os.path.exists(input_file):
        logging.error(f"Input file not found: {input_file}")
        return

    logging.info(f"Starting to process {input_file}")

    create_table.update(input_file)

    logging.info("Processing complete")


if __name__ == "__main__":
    main()
