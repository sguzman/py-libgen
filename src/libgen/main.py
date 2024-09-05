import sys
import re
import csv
import logging
import os
from multiprocessing import Pool

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_column_names(create_statement):
    columns = re.findall(r'`(\w+)`', create_statement)
    # Remove the first match (table name) and remove duplicates while preserving order
    unique_columns = []
    seen = set()
    for col in columns[1:]:
        if col not in seen:
            seen.add(col)
            unique_columns.append(col)
    return unique_columns

def process_create_statements(input_file):
    tables_file = 'tables.sql'
    table_columns = {}
    
    with open(input_file, 'r') as f, open(tables_file, 'w') as tf:
        content = f.read()
        create_table_statements = re.findall(r'CREATE TABLE.*?;', content, re.DOTALL | re.IGNORECASE)
        
        for statement in create_table_statements:
            tf.write(statement + '\n\n')
            table_name = re.search(r'CREATE TABLE `?(\w+)`?', statement, re.IGNORECASE).group(1)
            columns = extract_column_names(statement)
            table_columns[table_name] = columns
            
            csv_file = f"{table_name}.csv"
            with open(csv_file, 'w', newline='') as csvf:
                csv.writer(csvf).writerow(columns)
            logging.info(f"Created new CSV file with headers: {csv_file}")
        
    logging.info(f"Added {len(create_table_statements)} CREATE TABLE statement(s) to {tables_file}")
    return table_columns

def process_insert_chunk(chunk, table_columns):
    insert_statements = re.findall(r'INSERT INTO.*?VALUES\s*(\(.*?\));', chunk, re.DOTALL | re.IGNORECASE)
    results = []
    
    for statement in insert_statements:
        match = re.search(r'INSERT INTO `?(\w+)`?', statement, re.IGNORECASE)
        if not match:
            logging.warning(f"Could not extract table name from INSERT statement: {statement[:100]}...")
            continue
        
        table_name = match.group(1)
        if table_name not in table_columns:
            logging.warning(f"Skipping INSERT for unknown table: {table_name}")
            continue
        
        values = re.findall(r'\((.*?)\)', statement)
        for value_set in values:
            row = [v.strip().strip("'") for v in value_set.split(',')]
            if len(row) != len(table_columns[table_name]):
                logging.warning(f"Mismatch in column count for table {table_name}. Expected {len(table_columns[table_name])}, got {len(row)}")
                continue
            results.append((table_name, row))
    
    return results

def write_insert_results(results, table_columns):
    for table_name, row in results:
        csv_file = f"{table_name}.csv"
        with open(csv_file, 'a', newline='') as csvf:
            csv.writer(csvf).writerow(row)

def main(input_file):
    # First pass: Process CREATE TABLE statements
    table_columns = process_create_statements(input_file)
    
    # Second pass: Process INSERT statements in parallel
    chunk_size = 10 * 1024 * 1024  # 10 MB chunks
    
    with open(input_file, 'r') as f:
        with Pool() as pool:
            def read_chunk():
                chunk = f.read(chunk_size)
                if not chunk:
                    return ''
                # Read until the end of the last complete INSERT statement
                while not chunk.endswith(';\n') and not f.tell() == os.fstat(f.fileno()).st_size:
                    chunk += f.readline()
                return chunk

            chunks = iter(read_chunk, '')
            results = pool.starmap(process_insert_chunk, [(chunk, table_columns) for chunk in chunks])
            
            for chunk_results in results:
                write_insert_results(chunk_results, table_columns)
    
    logging.info("Processing complete")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <input_sql_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    logging.info(f"Starting to process {input_file}")
    main(input_file)