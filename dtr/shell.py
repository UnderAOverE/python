#!/usr/bin/env python3

import subprocess
import os
import shlex  # For safely joining shell arguments

def count_pattern_occurrences_with_grep(filename, pattern_list, pattern2):
    """
    Uses grep to count occurrences of pattern2 within lines matching each pattern in pattern_list individually.
    """
    results = {}  # Store the results for each pattern
    try:
        for pattern1 in pattern_list:
            # Build the grep command pipeline:
            # grep "pattern1" filename | grep "pattern2" | wc -l
            # command = f"grep '{pattern1}' '{filename}' | grep '{pattern2}' | wc -l"  # Potential security issue with shell=True

            # Construct the command as a list and shell escaping for all the arguments
            command = ['grep', pattern1, filename, '|', 'grep', pattern2, '|', 'wc', '-l'] # Shell must exists, and it's not ideal.

            # Execute the command and capture the output
            process = subprocess.Popen(" ".join(map(shlex.quote, command)), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash') # Specifying what shell to use

            # Get the output and error streams
            stdout, stderr = process.communicate()

            # Check for errors
            if stderr:
                print(f"Error running grep for pattern '{pattern1}': {stderr.decode()}")
                results[pattern1] = 0  # Or raise an exception, depending on how you want to handle errors
            else:
                # Parse the output and store the count
                try:
                    count = int(stdout.decode().strip())
                    results[pattern1] = count
                except ValueError:
                    print(f"Error parsing wc output for pattern '{pattern1}': {stdout.decode()}")
                    results[pattern1] = 0
        return results

    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return {}  # Or raise an exception
    except Exception as e:
        print(f"An error occurred: {e}")
        return {}  # Or raise an exception

if __name__ == "__main__":
    file_name = "large_log.txt"  # Replace with your log file name
    pattern_list = ["error", "warn", "critical"]  # Replace with your list of patterns for the first grep
    pattern_2 = "timeout"  # Replace with your second pattern

    occurrence_counts = count_pattern_occurrences_with_grep(file_name, pattern_list, pattern_2)

    for pattern, count in occurrence_counts.items():
        print(f"Number of occurrences of '{pattern_2}' within lines matching '{pattern}': {count}")
