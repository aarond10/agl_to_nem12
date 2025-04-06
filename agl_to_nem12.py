# -*- coding: utf-8 -*-
import csv
import argparse
from datetime import datetime, date, timedelta
from collections import defaultdict
import sys
import os
import re # Import regex for suffix checking

# --- Configuration ---
NEM12_VERSION = "NEM12"
FROM_PARTICIPANT = "AGL" # Or your specific identifier if known
TO_PARTICIPANT = "Converter" # Or the intended recipient
DEFAULT_UOM = "kWh"
# Interval length now set via command-line argument or default

# Mapping from AGL Quality Flag to NEM12 Quality Method/Flag
QUALITY_MAP = {
    'A': 'A', 'F': 'F', 'E': 'E', 'S': 'S', 'N': 'N',
}
DEFAULT_QUALITY = 'E' # Default if AGL flag is unknown or missing

# --- Global Variables for Interval Config ---
INTERVAL_LENGTH = 30
INTERVALS_PER_DAY = 48

# --- Helper Functions ---
def parse_agl_datetime(dt_str):
    if not dt_str: return None
    try: return datetime.strptime(dt_str, '%d/%m/%Y %I:%M:%S %p')
    except ValueError: return None

def get_interval_index(dt_obj, interval_length_minutes):
    global INTERVALS_PER_DAY
    if not dt_obj or interval_length_minutes <= 0: return None
    minutes_past_midnight = dt_obj.hour * 60 + dt_obj.minute
    index = minutes_past_midnight // interval_length_minutes
    if 0 <= index < INTERVALS_PER_DAY: return index
    else: return None

def format_nem12_date(date_obj): return date_obj.strftime('%Y%m%d')
def format_nem12_datetime(dt_obj): return dt_obj.strftime('%Y%m%d%H%M%S')

def get_suffix_from_register(register_code):
    if not register_code: return None, False
    if '#' in register_code:
        suffix = register_code.split('#')[-1]
        if not suffix: return None, False
        is_standard_pattern = bool(re.match(r'^[EBVQKNeqvkn]\d+$', suffix))
        return suffix, is_standard_pattern
    else: return None, False

def determine_day_quality(interval_qualities):
    present_qualities = {q for q in interval_qualities if q}
    if not present_qualities: return DEFAULT_QUALITY
    if 'N' in present_qualities: return 'N'
    if 'S' in present_qualities: return 'S'
    if 'E' in present_qualities: return 'E'
    if 'F' in present_qualities: return 'F'
    if 'A' in present_qualities: return 'A'
    return DEFAULT_QUALITY

# --- Main Conversion Logic ---
def convert_agl_to_nem12(input_filepath, output_filepath):
    """Converts AGL MyUsageData CSV to NEM12 CSV."""
    global INTERVAL_LENGTH, INTERVALS_PER_DAY

    data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {
        'intervals': [None] * INTERVALS_PER_DAY, 'quality': [''] * INTERVALS_PER_DAY, })))
    nmi_details = defaultdict(dict)
    warned_suffixes = set()

    print(f"Reading AGL data from: {input_filepath}")
    print(f"Expecting {INTERVALS_PER_DAY} intervals of {INTERVAL_LENGTH} minutes.")
    try:
        with open(input_filepath, 'r', newline='', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            expected_headers = ['NMI', 'RegisterCode', 'StartDate', 'ProfileReadValue', 'QualityFlag', 'DeviceNumber']
            if not reader.fieldnames or not all(h in reader.fieldnames for h in expected_headers):
                 print(f"Error: Input CSV missing one or more expected AGL headers: {expected_headers}\nFound: {reader.fieldnames}", file=sys.stderr); sys.exit(1)

            row_count = 0; processed_rows = 0; skipped_rows = 0
            for row in reader:
                row_count += 1
                nmi = row.get('NMI'); register_code = row.get('RegisterCode')
                start_date_str = row.get('StartDate'); value_str = row.get('ProfileReadValue')
                quality_agl = row.get('QualityFlag'); device_num = row.get('DeviceNumber')
                if not all([nmi, register_code, start_date_str, value_str is not None, quality_agl, device_num]):
                    skipped_rows += 1; continue

                start_dt = parse_agl_datetime(start_date_str)
                if not start_dt: skipped_rows += 1; continue
                interval_date = start_dt.date()
                interval_index = get_interval_index(start_dt, INTERVAL_LENGTH)
                if interval_index is None: skipped_rows += 1; continue

                suffix, is_standard_pattern = get_suffix_from_register(register_code)
                if not suffix: skipped_rows += 1; continue

                try: value = float(value_str); quality_nem12 = QUALITY_MAP.get(quality_agl, DEFAULT_QUALITY)
                except (ValueError, TypeError): value = 0.0; quality_nem12 = 'N'

                day_data = data[nmi][suffix][interval_date]
                day_data['intervals'][interval_index] = value
                day_data['quality'][interval_index] = quality_nem12
                processed_rows += 1

                details_key = (nmi, suffix)
                if suffix not in nmi_details[nmi]:
                    if not is_standard_pattern and details_key not in warned_suffixes:
                         print(f"Warning: NMI {nmi}, extracted Suffix '{suffix}' does not match common patterns (E#, B#, V#, etc). Verify if correct.", file=sys.stderr)
                         warned_suffixes.add(details_key)

                    mdm_data_stream_id = 'INTERVAL'
                    s_upper = suffix.upper()
                    if s_upper.startswith('B'): mdm_data_stream_id = 'GENERATION'
                    elif s_upper.startswith('E') or s_upper.startswith('V'): mdm_data_stream_id = 'CONSUMPTION'
                    elif s_upper.startswith('Q') or s_upper.startswith('K'): mdm_data_stream_id = 'REACTIVE'

                    nmi_details[nmi][suffix] = {
                        'NMISuffix': suffix, 'MDMDataStreamIdentifier': mdm_data_stream_id,
                        'MeterSerialNumber': device_num, 'UOM': DEFAULT_UOM,
                        'IntervalLength': INTERVAL_LENGTH, 'NextScheduledReadDate': '' }

    except FileNotFoundError: print(f"Error: Input file not found: {input_filepath}", file=sys.stderr); sys.exit(1)
    except Exception as e: print(f"An unexpected error occurred reading CSV: {e}", file=sys.stderr); sys.exit(1)

    if skipped_rows > 0: print(f"Info: Skipped {skipped_rows} rows due to missing/invalid data during reading.")
    if not data: print("No valid data rows processed."); sys.exit(0)

    print(f"Finished reading AGL data. Read {row_count} rows, processed {processed_rows} valid interval readings.")
    print(f"Found data for {len(data)} NMIs.")
    print(f"Writing NEM12 data to: {output_filepath}")

    try:
        with open(output_filepath, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow([100, NEM12_VERSION, format_nem12_datetime(datetime.now()), FROM_PARTICIPANT, TO_PARTICIPANT])

            for nmi in sorted(data.keys()):
                observed_suffixes = sorted(list(nmi_details[nmi].keys()))

                # *** CORRECTION: Join with empty string "" instead of "," ***
                nmi_config_string = "".join(observed_suffixes)

                if len(nmi_config_string) > 255:
                    print(f"Warning: NMI {nmi} generated NMIConfiguration string exceeds 255 chars. Truncating.", file=sys.stderr)
                    nmi_config_string = nmi_config_string[:255]

                for suffix in sorted(nmi_details[nmi].keys()):
                    details = nmi_details[nmi][suffix]
                    nmi_suffix_date_data = data[nmi][suffix]

                    writer.writerow([
                        200, nmi, nmi_config_string, '', details['NMISuffix'],
                        details['MDMDataStreamIdentifier'], details['MeterSerialNumber'],
                        details['UOM'], details['IntervalLength'], details['NextScheduledReadDate']
                    ])

                    date_count = 0
                    for interval_date in sorted(nmi_suffix_date_data.keys()):
                        day_data = nmi_suffix_date_data[interval_date]
                        intervals = day_data['intervals']
                        qualities = day_data['quality']
                        if all(v is None for v in intervals): continue

                        processed_intervals = []; final_qualities_for_day = []
                        for i, val in enumerate(intervals):
                            interval_quality = qualities[i] if qualities[i] else DEFAULT_QUALITY
                            if val is not None:
                                processed_intervals.append(f"{val:.3f}")
                                final_qualities_for_day.append(interval_quality)
                            else:
                                processed_intervals.append("0.000")
                                final_qualities_for_day.append('S')

                        if len(processed_intervals) != INTERVALS_PER_DAY:
                             print(f"Error: Data inconsistency NMI {nmi}, Suffix {suffix}, Date {format_nem12_date(interval_date)}. Expected {INTERVALS_PER_DAY} intervals, found {len(processed_intervals)}. Skipping day.", file=sys.stderr)
                             continue

                        day_quality_method = determine_day_quality(final_qualities_for_day)
                        writer.writerow([
                            300, format_nem12_date(interval_date), *processed_intervals,
                            day_quality_method, "", "", "" ])
                        date_count += 1

            writer.writerow([900])
        print(f"Successfully converted data to {output_filepath}")

    except IOError as e: print(f"Error writing NEM12 file: {e}", file=sys.stderr); sys.exit(1)
    except Exception as e: print(f"An unexpected error occurred during NEM12 writing: {e}", file=sys.stderr); sys.exit(1)

# --- Command Line Interface ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert AGL MyUsageData CSV export to NEM12 format. Populates NMIConfiguration with observed suffixes concatenated without separators.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument("input_file", help="Path to the input AGL MyUsageData CSV file.")
    parser.add_argument("-o", "--output", dest="output_file", help="Path to output NEM12 CSV file. Defaults to input filename with .nem12.csv extension.")
    parser.add_argument("--interval", type=int, default=30, choices=[5, 15, 30], help="Interval length in minutes.")

    args = parser.parse_args()
    INTERVAL_LENGTH = args.interval
    INTERVALS_PER_DAY = (24 * 60) // INTERVAL_LENGTH
    if not args.output_file:
        base, _ = os.path.splitext(args.input_file)
        args.output_file = f"{base}.nem12.csv"
    convert_agl_to_nem12(args.input_file, args.output_file)
