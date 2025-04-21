# file: csv_to_excel_sheets.py
import argparse
import logging
import os
import sys
import re
import pandas as pd

# Import the utility function from xlsxwriter
from xlsxwriter.utility import xl_col_to_name


def setup_logging(log_level_str):
    """Configures logging based on the provided level string."""
    numeric_level = getattr(logging, log_level_str.upper(), None)
    if not isinstance(numeric_level, int):
        logging.warning(f"Invalid log level: {log_level_str}. Defaulting to INFO.")
        numeric_level = logging.INFO

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        stream=sys.stderr,
        force=True,  # Override any existing handlers
    )
    logging.info(f"Logging level set to: {log_level_str.upper()}")


def sanitize_sheet_name(name):
    """
    Sanitizes a string to be a valid Excel sheet name.
    Removes invalid characters and truncates to 31 characters.
    """
    # Remove invalid characters: \ / * ? [ ] : '
    # Also remove leading/trailing single quotes sometimes added by Excel
    name = re.sub(r"[\\/*?:\[\]']", "_", name)
    name = name.strip("'")
    # Truncate to 31 characters (Excel limit)
    return name[:31]


def format_excel_sheet(writer, df, sheet_name):
    """Writes DataFrame to an Excel sheet with formatting."""
    logging.debug(f"Writing DataFrame to sheet: {sheet_name}")
    df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0)

    # Get the xlsxwriter workbook and worksheet objects.
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    # --- Define Header Format Only ---
    header_format = workbook.add_format(
        {
            "bold": True,
            "align": "center",
            "valign": "top",
            "fg_color": "#D7E4BC",
            "border": 1,
            "text_wrap": True,
        }
    )

    # Define a format specifically for word wrapping in data cells
    wrap_format = workbook.add_format({"text_wrap": True, "valign": "top"})
    # Define a default format for data cells (used when not wrapping)
    default_data_format = workbook.add_format({"valign": "top"})  # Ensure top alignment

    # --- Define Max Width ---
    # using ~3.94 units/cm, rounded up slightly
    max_allowed_width = 100  # Updated max width

    # --- Apply Header Formatting and Calculate/Set Column Widths ---
    (max_row, max_col) = df.shape

    if max_col > 0:
        for col_idx, col_name in enumerate(df.columns):
            # --- Apply Header Format ---
            worksheet.write(0, col_idx, str(col_name), header_format)

            # --- Calculate Required Width ---
            try:
                # Ensure all data is string before calculating max length
                max_len_data = df[col_name].astype(str).map(len).max()
                # Handle case where column might be empty or all NaN after astype(str)
                if pd.isna(max_len_data):
                    max_len_data = 0
            except Exception as e:
                logging.warning(
                    f"Could not calculate max data length for column '{col_name}': {e}. Using header length."
                )
                max_len_data = 0

            # Required width based on content and header
            required_width = max(
                max_len_data,
                len(str(col_name)),  # Ensure header name is also treated as string
            )
            # Add padding
            required_width += 2

            # --- Determine Display Width and Apply Formatting ---
            display_width = min(required_width, max_allowed_width)

            # Set column width and apply wrap format *only if width was capped*
            if required_width > max_allowed_width:
                worksheet.set_column(
                    col_idx, col_idx, width=display_width, cell_format=wrap_format
                )
                logging.debug(
                    f"Column {col_idx} ('{col_name}') capped at width {display_width} and wrap applied."
                )
            else:
                # Set the width and apply default top alignment format
                worksheet.set_column(
                    col_idx,
                    col_idx,
                    width=display_width,
                    cell_format=default_data_format,
                )
                logging.debug(
                    f"Column {col_idx} ('{col_name}') set to width {display_width} with top alignment."
                )

    # --- Add Excel Table ---
    # Check if there are any rows or columns to create a table
    if max_row > 0 and max_col > 0:
        # Create table range using xlsxwriter utility function for column names
        end_col_name = xl_col_to_name(max_col - 1)
        # Table range includes header row (row 1 in Excel) and data rows
        table_range = f"A1:{end_col_name}{max_row + 1}"
        logging.debug(f"Adding table to range: {table_range}")

        column_settings = [
            {"header": str(column)} for column in df.columns
        ]  # Ensure headers are strings

        try:
            # Add the table. The style 'Table Style Medium 9' will handle
            # the formatting of data rows and potentially the header row.
            worksheet.add_table(
                table_range,
                {"columns": column_settings, "style": "Table Style Medium 9"},
            )
        except Exception as e:
            logging.error(
                f"Error adding table '{table_range}' to sheet '{sheet_name}': {e}"
            )

    elif max_col == 0:
        logging.warning(
            f"Skipping table creation for sheet '{sheet_name}' as there are no columns."
        )
    else:  # max_row == 0
        logging.warning(
            f"Skipping table creation for sheet '{sheet_name}' as there are no data rows (only headers)."
        )

    logging.debug(f"Finished formatting sheet: {sheet_name}")


def main():
    parser = argparse.ArgumentParser(
        description="Convert CSV files to formatted Excel (.xlsx) files.\n"
        "Default: Creates one XLSX file per input CSV in the same directory.\n"
        "Merge Mode: Combines all CSVs into sheets within a single XLSX file.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "csv_files",
        nargs="+",  # Accept one or more file paths
        help="Path(s) to the input CSV file(s).",
    )
    parser.add_argument(
        "-m",
        "--merge",
        action="store_true",
        help="Merge all input CSVs into sheets in a single output Excel file.\n"
        "If not set, each CSV is saved as a separate XLSX file.",
    )
    parser.add_argument(
        "-o",
        "--output",
        metavar="XLSX_FILE",
        default="merged_output.xlsx",
        help="Path to the output Excel file (only used when --merge is specified).\n"
        "Default: merged_output.xlsx",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO).",
    )

    args = parser.parse_args()
    setup_logging(args.log_level)

    processed_count = 0
    fail_count = 0

    if args.merge:
        # --- Merge Mode ---
        logging.info(f"Starting MERGE mode. Outputting all sheets to: {args.output}")
        sheet_names_used = set()

        try:
            # Use a single ExcelWriter for the merged file
            with pd.ExcelWriter(args.output, engine="xlsxwriter") as writer:
                for csv_path in args.csv_files:
                    if not os.path.isfile(csv_path):
                        logging.warning(f"Skipping non-existent file: {csv_path}")
                        fail_count += 1
                        continue

                    logging.debug(f"Processing CSV for merge: {csv_path}")
                    try:
                        df = pd.read_csv(
                            csv_path, keep_default_na=False, na_values=[""]
                        )

                        if df.empty and os.path.getsize(csv_path) == 0:
                            logging.warning(
                                f"Skipping empty CSV file (0 bytes): {csv_path}"
                            )
                            fail_count += 1
                            continue
                        elif df.empty and not pd.read_csv(csv_path, nrows=0).empty:
                            logging.warning(
                                f"CSV file has headers but no data rows: {csv_path}. Proceeding with headers only."
                            )

                        # Generate and sanitize sheet name, handle duplicates
                        base_sheet_name = os.path.splitext(os.path.basename(csv_path))[
                            0
                        ]
                        sheet_name = sanitize_sheet_name(base_sheet_name)
                        original_sheet_name = sheet_name
                        counter = 1
                        while sheet_name in sheet_names_used:
                            suffix = f"_{counter}"
                            max_len = 31 - len(suffix)
                            sheet_name = sanitize_sheet_name(
                                original_sheet_name[:max_len] + suffix
                            )
                            counter += 1
                            if counter > 100:
                                logging.error(
                                    f"Could not generate unique sheet name for {csv_path} based on '{original_sheet_name}'. Skipping."
                                )
                                raise ValueError("Too many duplicate sheet names.")
                        sheet_names_used.add(sheet_name)

                        logging.info(
                            f"Adding sheet '{sheet_name}' from '{csv_path}'..."
                        )
                        format_excel_sheet(writer, df, sheet_name)
                        processed_count += 1

                    except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
                        logging.error(
                            f"Error reading/parsing CSV {csv_path}: {e}. Skipping."
                        )
                        fail_count += 1
                    except ValueError as e:  # Catch duplicate sheet name error
                        fail_count += 1
                    except Exception as e:
                        logging.exception(
                            f"Unexpected error processing {csv_path} for merge: {e}. Skipping."
                        )
                        fail_count += 1

            if processed_count > 0:
                logging.info(
                    f"Successfully wrote {processed_count} sheet(s) to {args.output}"
                )
            elif fail_count > 0:
                logging.warning(
                    f"No sheets were written to {args.output} due to errors."
                )
                # Clean up potentially empty Excel file
                if os.path.exists(args.output) and os.path.getsize(args.output) < 500:
                    try:
                        os.remove(args.output)
                        logging.info(
                            f"Removed empty/incomplete output file: {args.output}"
                        )
                    except OSError as e:
                        logging.error(
                            f"Could not remove empty/incomplete output file {args.output}: {e}"
                        )

        except (IOError, OSError) as e:
            logging.critical(f"Could not write to output file {args.output}: {e}")
            fail_count = len(args.csv_files)  # Mark all as failed if writer fails
        except Exception as e:
            logging.exception(
                f"An unexpected error occurred during Excel writing setup/save: {e}"
            )
            fail_count = len(args.csv_files)  # Mark all as failed

    else:
        # --- Separate Files Mode (Default) ---
        logging.info("Starting SEPARATE files mode. Creating one XLSX per CSV.")
        for csv_path in args.csv_files:
            if not os.path.isfile(csv_path):
                logging.warning(f"Skipping non-existent file: {csv_path}")
                fail_count += 1
                continue

            # Determine output path based on input path
            base_name = os.path.splitext(csv_path)[0]
            output_xlsx_path = f"{base_name}.xlsx"

            logging.debug(f"Processing CSV: {csv_path} -> {output_xlsx_path}")

            try:
                df = pd.read_csv(csv_path, keep_default_na=False, na_values=[""])

                if df.empty and os.path.getsize(csv_path) == 0:
                    logging.warning(f"Skipping empty CSV file (0 bytes): {csv_path}")
                    fail_count += 1
                    continue
                elif df.empty and not pd.read_csv(csv_path, nrows=0).empty:
                    logging.warning(
                        f"CSV file has headers but no data rows: {csv_path}. Proceeding with headers only."
                    )

                # Generate a sheet name (usually just one per file)
                sheet_name = sanitize_sheet_name(
                    os.path.splitext(os.path.basename(csv_path))[0]
                )
                if not sheet_name:
                    sheet_name = "Sheet1"

                logging.info(
                    f"Converting '{csv_path}' to '{output_xlsx_path}' (Sheet: '{sheet_name}')..."
                )

                # Use ExcelWriter for each file individually
                try:
                    with pd.ExcelWriter(
                        output_xlsx_path, engine="xlsxwriter"
                    ) as writer:
                        format_excel_sheet(writer, df, sheet_name)
                    processed_count += 1
                    logging.debug(
                        f"Successfully created and formatted: {output_xlsx_path}"
                    )
                except (IOError, OSError) as e:
                    logging.error(
                        f"Could not write to output file {output_xlsx_path}: {e}"
                    )
                    fail_count += 1
                except Exception as e:
                    logging.exception(
                        f"An unexpected error occurred during Excel writing for {output_xlsx_path}: {e}"
                    )
                    fail_count += 1

            except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
                logging.error(f"Error reading/parsing CSV {csv_path}: {e}. Skipping.")
                fail_count += 1
            except Exception as e:
                logging.exception(
                    f"An unexpected error occurred processing {csv_path}: {e}. Skipping."
                )
                fail_count += 1

    # --- Final Summary ---
    logging.info("--- Conversion Summary ---")
    logging.info(
        f"Successfully processed/added: {processed_count} CSV file(s)/sheet(s)"
    )
    logging.info(f"Failed/Skipped:             {fail_count} CSV file(s)")
    logging.info("--------------------------")

    if fail_count > 0 or processed_count == 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
