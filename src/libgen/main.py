import sys
import re
import csv
import logging
from multiprocessing import Pool, Manager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_chunk(chunk, tables_file, csv_writers, lock):
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
                if table_name not in csv_writers:
                    csv_file = f"{table_name}.csv"
                    csv_writers[table_name] = csv.writer(open(csv_file, 'w', newline=''))
                    csv_writers[table_name].writerow(['values'])  # Header row
                    logging.info(f"Created new CSV file: {csv_file}")
                for value_set in values:
                    csv_writers[table_name].writerow([v.strip() for v in value_set.split(',')])
            logging.info(f"Processed {len(insert_statements)} INSERT statement(s)")

def main(input_file):
    tables_file = 'tables.sql'
    chunk_size = 10 * 1024 * 1024  # 10 MB chunks

    # Clear existing tables.sql file
    open(tables_file, 'w').close()

    manager = Manager()
    lock = manager.Lock()
    csv_writers = manager.dict()

    with open(input_file, 'r') as f:
        with Pool() as pool:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                pool.apply_async(process_chunk, (chunk, tables_file, csv_writers, lock))

            pool.close()
            pool.join()

    # Close all CSV writers
    for writer in csv_writers.values():
        writer.close()

    logging.info("Processing complete")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <input_sql_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    logging.info(f"Starting to process {input_file}")
    main(input_file)