#!/usr/bin/env python3

import json
import os
import re
import datetime
import argparse
import glob
import subprocess
import pytz  # Requires: pip install pytz

VALID_SEARCHHEAD_IPS = ["10.1.2.3", "10.1.22.113", "10.21.22.39"]  # Add your list of valid IPs

NORMALIZED_RESULTS_PATTERN = "normalizedResults= []"  # Constant Pattern

def parse_arguments():
    """Parses command-line arguments for time window."""
    parser = argparse.ArgumentParser(description="Parse utils.log and count occurrences by search head using grep within a time window.")
    parser.add_argument("time_window", type=str, help="Time window (e.g., '30m', '1h', '24h')")
    return parser.parse_args()

def calculate_time_delta(time_window_str):
    """Calculates a datetime.timedelta object from the time window string."""
    try:
        value = int(time_window_str[:-1])  # Extract the numeric value
        unit = time_window_str[-1].lower()  # Extract the time unit (m or h)

        if unit == 'm':
            return datetime.timedelta(minutes=value)
        elif unit == 'h':
            return datetime.timedelta(hours=value)
        else:
            raise ValueError("Invalid time unit. Use 'm' for minutes or 'h' for hours.")
    except ValueError as e:
        raise ValueError(f"Invalid time window format: {time_window_str}.  Use a number followed by 'm' or 'h'. e.g., 30m") from e

def extract_searchhead_ip(message):
    """Extracts the search head IP address from the log message."""
    searchhead_match = re.search(r'searchhead=([\d.]+)/', message)
    if searchhead_match:
        ip_address = searchhead_match.group(1)
        if ip_address in VALID_SEARCHHEAD_IPS:  # Check if IP is valid
            return ip_address
    return 'Unknown'  # Return "Unknown" if not found or not valid

def count_pattern_occurrences_with_grep(log_file, log_date_str, normalized_results_pattern):
    """
    Uses grep to count occurrences of normalized_results_pattern within a log line.
    """
    try:
        grep1_command = ['grep', normalized_results_pattern]  # normalizedResults= []
        grep1_process = subprocess.Popen(grep1_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        grep1_stdout, grep1_stderr = grep1_process.communicate(input=str.encode(log_date_str))

        if grep1_stderr:
            print(f"Error running first grep: {grep1_stderr.decode()}")
            return 0

        count = len(grep1_stdout.splitlines())
        return count

    except FileNotFoundError:
        print(f"Error: File '{log_file}' not found.")
        return 0
    except Exception as e:
        print(f"An error occurred: {e}")
        return 0

def process_log_file(log_file, time_delta):
    """Processes a single log file and extracts relevant data."""
    log_entries = {}  # Dictionary to store entries grouped by searchhead
    now_utc = datetime.datetime.now(pytz.utc)  # Current time in UTC

    try:
        with open(log_file, 'r') as f:
            for line in f:
                try:
                    log_entry = json.loads(line)
                    log_date_str = log_entry.get('log_date')
                    message = log_entry.get('message', '')

                    if not log_date_str:
                        continue

                    log_date_str_truncated = log_date_str.split('.')[0] + log_date_str[-6:]
                    log_date = datetime.datetime.fromisoformat(log_date_str_truncated)
                    log_date_utc = log_date.replace(tzinfo=pytz.utc)

                    if now_utc - log_date_utc <= time_delta:
                        searchhead_ip = extract_searchhead_ip(message)
                        if searchhead_ip != 'Unknown':
                            try:
                                before_searchhead, _ = message.split("searchhead=", 1)
                            except ValueError:
                                before_searchhead = message

                            btnames = re.findall(r"\[([^\]]+)\]", before_searchhead)

                            count = count_pattern_occurrences_with_grep(log_file, line, NORMALIZED_RESULTS_PATTERN)

                            for btname in btnames:
                                btname = btname.strip()

                                if searchhead_ip not in log_entries:
                                    log_entries[searchhead_ip] = []

                                if count > 0:
                                    log_entries[searchhead_ip].append({
                                        "btname": btname, #BTNAME
                                        "count": count
                                    })


                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON line in {log_file}: {line.strip()}")
                except ValueError as e:
                    print(f"Skipping line in {log_file} due to date parsing error: {e}")
                except Exception as e:
                    print(f"Error processing line in {log_file}: {e}")

    except FileNotFoundError:
        print(f"Log file not found: {log_file}")
    except Exception as e:
        print(f"Error processing log file {log_file}: {e}")

    return log_entries

def find_log_files(directory):
    """Finds all utils.log files recursively within the given directory."""
    log_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.startswith('utils.log'):
                log_files.append(os.path.join(root, file))
    return log_files

def main():
    """Main function to parse arguments, process logs, and print results."""
    try:
        args = parse_arguments()
        time_delta = calculate_time_delta(args.time_window)
    except ValueError as e:
        print(f"Error: {e}")
        return

    log_files = find_log_files("/opt/aiops/")
    all_search_head_counts = {}

    for log_file in log_files:
        log_entries = process_log_file(log_file, time_delta)

        # Combine log entries
        for searchhead_ip, entries in log_entries.items(): # Iterate through each of the IPs.
            if searchhead_ip not in all_search_head_counts: # Check if the search IP already exists
                all_search_head_counts[searchhead_ip] = [] # If not then create new list

            all_search_head_counts[searchhead_ip].extend(entries) # Then extend and append the list

    final_output = [] #Create empty list
    for searchhead, btname_entries in all_search_head_counts.items():
         if searchhead in VALID_SEARCHHEAD_IPS: # Check if the searchhead is found and if it meets your requirements
              total_occurrences = sum(entry["count"] for entry in btname_entries) # Calculate total occurences from list of btname
              final_output.append({
                  "searchhead": searchhead,
                  "total_occurrences": total_occurrences,
                  "btnames": btname_entries  # Use the list of BTNAME entries
              }) # Append everything in the list, including all of the BT Names



    # Print the results in JSON format
    print(json.dumps(final_output, indent=2))

if __name__ == "__main__":
    main()
