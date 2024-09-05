import re
import csv
import logging
import os

from sqlglot import exp, parse_one

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

def parse_insert_values(string: str) -> list[list[any]]:
    # Get prefix before VALUES
    prefix = string[:string.index('VALUES')]
    # Extract the values part of the INSERT statement
    raw_values = string[string.index('VALUES') + 6:].split('),(')

    # Clean leading and trailing parentheses
    parsed_rows = []
    for value in raw_values:
        try:
            single = prefix.replace('`', '') + ' VALUES ' + '(' + value.replace('(', '') +');'
            stmt = parse_one(single)
            data = [x.this for x in stmt.find(exp.Values).find_all(exp.Literal)]
            data = [None if x == '' else x for x in data]
            parsed_rows.append(data)
        except Exception as e:
            logging.warn(f"Error parsing INSERT statement: {e}")
    return parsed_rows


def process_insert_statements(input_file, table_columns):
    with open(input_file, 'r') as f:
        # Read the entire file, split by newlines and filter out lines that don't start with INSERT
        insert_statements = [line for line in f.read().splitlines() if line.startswith('INSERT INTO')]
        
        for statement in insert_statements:
            try:
                table_name = statement[:statement.index('VALUES')].split()[2].replace('`','')
                if table_name not in table_columns:
                    logging.warning(f"Skipping INSERT for unknown table: {table_name}")
                    continue

                csv_file = f"{table_name}.csv"
                with open(csv_file, 'a', newline='') as csvf:
                    writer = csv.writer(csvf)
                    rows = parse_insert_values(statement)
                    for row in rows:
                        logging.info(f"Writing row to {csv_file}: {row}")
                        writer.writerow(row)

            except Exception as e:
                logging.error(f"Error processing INSERT statement: {e}")
                logging.error(f"Problematic statement: {statement[:100]}...")

        logging.info(f"Processed {len(insert_statements)} INSERT statement(s)")

def main():
    # Define the path to data.sql in the resources folder
    script_dir = os.path.dirname(os.path.abspath(__file__))
    resources_dir = os.path.join(os.path.dirname(script_dir), 'resources')
    input_file = os.path.join(resources_dir, 'data.sql')

    if not os.path.exists(input_file):
        logging.error(f"Input file not found: {input_file}")
        return

    logging.info(f"Starting to process {input_file}")

    # First pass: Process CREATE TABLE statements
    table_columns = process_create_statements(input_file)
    
    # Second pass: Process INSERT statements
    process_insert_statements(input_file, table_columns)
    
    logging.info("Processing complete")

if __name__ == "__main__":
    main()