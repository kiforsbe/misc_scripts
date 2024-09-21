import csv
import os
import logging
import traceback
import string

def setup_logging():
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    
    # Remove all handlers associated with the root logger object
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # File handler
    file_handler = logging.FileHandler('file_renamer.log', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # Configure the root logger
    logging.basicConfig(level=logging.INFO, format=log_format, handlers=[file_handler, console_handler])

def read_csv(filename):
    try:
        with open(filename, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.reader(f, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
            return list(reader)
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        return None

def find_file(file_id, directory):
    for filename in os.listdir(directory):
        if file_id in filename:
            return filename
    return None

def sanitize_filename(filename):
    # Define valid characters (ASCII letters, digits, and some punctuation)
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    
    # Replace spaces with underscores
    filename = filename.replace(' ', '_')
    
    # Keep only valid ascii chars
    sanitized = ''.join(c for c in filename if c in valid_chars)
    
    # Remove leading and trailing periods and spaces
    sanitized = sanitized.strip('. ')
    
    # Ensure the filename is not empty and doesn't exceed the maximum length
    if not sanitized:
        sanitized = "unnamed_file"
    sanitized = sanitized[:240]  # Leave room for file extension
    
    return sanitized

def get_new_filename(old_filename, new_name):
    name, extension = os.path.splitext(old_filename)
    sanitized_name = sanitize_filename(new_name)
    return f"{sanitized_name}{extension}"

def remove_duplicates(mappings):
    seen = set()
    unique_mappings = []
    for old, new in mappings:
        if new not in seen:
            seen.add(new)
            unique_mappings.append((old, new))
        else:
            logging.warning(f"Duplicate target filename detected and removed: {new}")
    return unique_mappings

def main():
    setup_logging()
    logging.info("Script started")
    
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        input_csv = os.path.join(script_dir, 'clipboard.csv')
        
        if not os.path.exists(input_csv):
            logging.error(f"Error: {input_csv} not found.")
            return

        csv_data = read_csv(input_csv)
        if csv_data is None:
            return

        mappings = []

        for row in csv_data:
            if len(row) < 2:
                logging.warning(f"Warning: Skipping invalid row {row}")
                continue

            file_id, target_name = row[0], ', '.join(row[1:])
            found_file = find_file(file_id, script_dir)

            if found_file:
                new_filename = get_new_filename(found_file, target_name)
                mappings.append((found_file, new_filename))
            else:
                logging.warning(f"Warning: No file found for ID {file_id}")

        # Remove duplicates
        mappings = remove_duplicates(mappings)

        # Output mappings to CSV
        output_csv = os.path.join(script_dir, 'rename_mappings.csv')
        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writerow(['Original Filename', 'New Filename'])
            writer.writerows(mappings)

        logging.info(f"Rename mappings have been written to {output_csv}")
        logging.info("Please review the mappings and confirm if you want to proceed with renaming.")

        user_input = input("Type 'y' or 'yes' to proceed, or anything else to cancel: ").lower()

        if user_input in ['y', 'yes']:
            for old_name, new_name in mappings:
                old_path = os.path.join(script_dir, old_name)
                new_path = os.path.join(script_dir, new_name)
                os.rename(old_path, new_path)
                logging.info(f"Renamed: {old_name} -> {new_name}")
            logging.info("All files have been renamed successfully.")
        else:
            logging.info("Operation cancelled. No files were renamed.")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        logging.error(traceback.format_exc())

    finally:
        input("Press Enter to exit...")

if __name__ == "__main__":
    main()
