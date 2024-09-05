import sys
import re
import csv
import logging
from multiprocessing import Pool, Manager
from functools import partial

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_chunk(chunk, tables_file, csv_writer, lock):
    create_table_statements = re.findall(r'CREATE TABLE.*?;', chunk, re.DOTALL | re.IGNORECASE)
    insert_statements = re.findall(r'INSERT INTO.*?;', chunk, re.IGNORECASE)

    with lock:
        if create_table_statements:
            with open(tables_file, 'a') as f:
                for statement in create_table_statements:
                    f.write(statement + '\n\n')
            logging.info(f"Added {len(create_table_statements)} CREATE TABLE statement(s) to {tables_file}")

        if insert_statements:
            for statement in insert_statements:
                table_name = re.search(r'INSERT INTO `?(\w+)`?', statement, re.IGNORECASE).group(1)
                values = re.findall(r'\((.*?)\)', statement)
                for value_set in values:
                    csv_writer.writerow([table_name] + [v.strip() for v in value_set.split(',')])
            logging.info(f"Processed {len(insert_statements)} INSERT statement(s)")

def main(input_file):
    tables_file = 'tables.sql'
    csv_file = 'output.csv'
    chunk_size = 10 * 1024 * 1024  # 10 MB chunks

    # Clear existing files
    open(tables_file, 'w').close()
    open(csv_file, 'w').close()

    manager = Manager()
    lock = manager.Lock()

    with open(csv_file, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['table_name', 'values'])  # Header row

        with open(input_file, 'r') as f:
            with Pool() as pool:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    pool.apply_async(process_chunk, (chunk, tables_file, csv_writer, lock))

                pool.close()
                pool.join()

    logging.info("Processing complete")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <input_sql_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    logging.info(f"Starting to process {input_file}")
    main(input_file)
