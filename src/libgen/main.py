import sys
import re
import csv
import logging
import os
from multiprocessing import Pool, Manager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_column_names(create_statement):
    columns = re.findall(r'`(\w+)`', create_statement)
    return columns[1:]  # Exclude the first match, which is the table name

def process_chunk(chunk, tables_file, csv_files, lock):
    create_table_statements = re.findall(r'CREATE TABLE.*?;', chunk, re.DOTALL | re.IGNORECASE)
    insert_statements = re.findall(r'INSERT INTO.*?VALUES\s*(\(.*?\));', chunk, re.DOTALL | re.IGNORECASE)

    with lock:
        if create_table_statements:
            with open(tables_file, 'a') as f:
                for statement in create_table_statements:
                    f.write(statement + '\n\n')
                    table_name = re.search(r'CREATE TABLE `?(\w+)`?', statement, re.IGNORECASE).group(1)
                    columns = extract_column_names(statement)
                    csv_file = f"{table_name}.csv"
                    if csv_file not in csv_files:
                        with open(csv_file, 'w', newline='') as csvf:
                            csv.writer(csvf).writerow(columns)
                        csv_files.append(csv_file)
                        logging.info(f"Created new CSV file with headers: {csv_file}")
            logging.info(f"Added {len(create_table_statements)} CREATE TABLE statement(s) to {tables_file}")

        if insert_statements:
            for statement in insert_statements:
                table_name = re.search(r'INSERT INTO `?(\w+)`?', statement, re.IGNORECASE).group(1)
                csv_file = f"{table_name}.csv"
                values = re.findall(r'\((.*?)\)', statement)
                with open(csv_file, 'a', newline='') as csvf:
                    writer = csv.writer(csvf)
                    for value_set in values:
                        writer.writerow([v.strip().strip("'") for v in value_set.split(',')])
            logging.info(f"Processed {len(insert_statements)} INSERT statement(s)")

def main(input_file):
    tables_file = 'tables.sql'
    chunk_size = 10 * 1024 * 1024  # 10 MB chunks

    # Clear existing tables.sql file
    open(tables_file, 'w').close()

    # Clear existing CSV files
    for file in os.listdir():
        if file.endswith('.csv'):
            os.remove(file)

    manager = Manager()
    lock = manager.Lock()
    csv_files = manager.list()  # Changed from set() to list()

    with open(input_file, 'r') as f:
        with Pool() as pool:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                pool.apply_async(process_chunk, (chunk, tables_file, csv_files, lock))

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