#!/usr/bin/env python3

import json
import os
import re
import datetime
import argparse
import glob
import subprocess
import shlex

def parse_arguments():
    """Parses command-line arguments for time window and patterns."""
    parser = argparse.ArgumentParser(description="Parse utils.log and count occurrences by search head using grep within a time window.")
    parser.add_argument("time_window", type=str, help="Time window (e.g., '30m', '1h', '24h')")
    parser.add_argument("--searchhead_pattern", type=str, default="searchhead=", help="Pattern for identifying search head (default: searchhead=)")
    parser.add_argument("--normalized_results_pattern", type=str, default="normalizedResults= []", help="Pattern 2 (default: normalizedResults= [])")

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
    return searchhead_match.group(1) if searchhead_match else 'Unknown'

def count_pattern_occurrences_with_grep(log_file, log_date_str, normalized_results_pattern):
    """
    Uses grep to count occurrences of normalized_results_pattern within a log line.
    """
    try:

       grep1_command = ['grep', normalized_results_pattern] # normalizedResults= []
       # Execute the first grep command
       grep1_process = subprocess.Popen(grep1_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
       grep1_stdout, grep1_stderr = grep1_process.communicate(input = str.encode(log_date_str))

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


def process_log_file(log_file, time_delta, searchhead_pattern, normalized_results_pattern):
    """Processes a single log file and extracts relevant data."""
    search_head_counts = {}
    now = datetime.datetime.now(datetime.timezone.utc)  # Current time in UTC

    try:
        with open(log_file, 'r') as f:
            for line in f:
                try:
                    log_entry = json.loads(line)  # Parse each line as JSON
                    log_date_str = log_entry.get('log_date')
                    message = log_entry.get('message', '')  # Ensure message exists, otherwise blank string

                    if not log_date_str:
                        continue # Skip if log_date is missing

                    log_date = datetime.datetime.fromisoformat(log_date_str.replace('Z', '+00:00')) # Handle the Z notation for UTC.
                    if now - log_date <= time_delta:

                      if searchhead_pattern in message:

                         # Extract BTNAMEs only if "searchhead=" is present.
                         btnames = re.findall(r"\[([^\]]+)\]", message.split(searchhead_pattern)[0]) # Extract content inside []
                         searchhead_ip = extract_searchhead_ip(message)

                         if searchhead_ip not in search_head_counts:
                             search_head_counts[searchhead_ip] = {}

                         count = count_pattern_occurrences_with_grep(log_file, line, normalized_results_pattern)
                         for btname in btnames:
                            if btname not in search_head_counts[searchhead_ip]:
                                 search_head_counts[searchhead_ip][btname] = 0 # Initialize
                            search_head_counts[searchhead_ip][btname] += int(count)

                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON line in {log_file}: {line.strip()}")
                except ValueError as e:
                    print(f"Skipping line in {log_file} due to date parsing error: {e}")
                except Exception as e:
                    print(f"Error processing line in {log_file}: {e}") # Other errors

    except FileNotFoundError:
        print(f"Log file not found: {log_file}")
    except Exception as e:
        print(f"Error processing log file {log_file}: {e}") # Generic error, include file name.

    return search_head_counts

def main():
    """Main function to parse arguments, process logs, and print results."""
    try:
        args = parse_arguments()
        time_delta = calculate_time_delta(args.time_window)  # Get the time delta
    except ValueError as e:
        print(f"Error: {e}")
        return

    log_files = glob.glob("/opt/aiops/utils.log*")  # Find utils.log and utils.log.1, utils.log.2, utils.log.3 inside /opt/aiops/
    all_search_head_counts = {}

    for log_file in log_files:
        search_head_counts = process_log_file(log_file, time_delta, args.searchhead_pattern, args.normalized_results_pattern)  # Process the log file

        # Merge the results from each file
        for searchhead, btname_counts in search_head_counts.items():
            if searchhead not in all_search_head_counts:
                all_search_head_counts[searchhead] = {}
            for btname, count in btname_counts.items():
                if btname not in all_search_head_counts[searchhead]:
                    all_search_head_counts[searchhead][btname] = 0
                all_search_head_counts[searchhead][btname] += count

    # Print the results in JSON format
    print(json.dumps(all_search_head_counts, indent=2))

if __name__ == "__main__":
    main()
