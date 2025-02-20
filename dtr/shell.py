#!/usr/bin/env python3

import subprocess
import os
import shlex  # For safely joining shell arguments

def get_bash_executable():
    """
    Dynamically determines the path to the bash executable using 'which bash'.
    Returns None if bash is not found.
    """
    try:
        process = subprocess.Popen(['which', 'bash'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        if stdout:
            return stdout.decode().strip()
        else:
            print("bash not found in PATH.")
            return None
    except FileNotFoundError:
        print("which command not found. Assuming /bin/bash.")
        return '/bin/bash' # Reasonable default
    except Exception as e:
        print(f"Error determining bash path: {e}")
        return None

def count_pattern_occurrences_with_grep(filename, pattern_list, pattern2, bash_executable=None):
    """
    Uses grep to count occurrences of pattern2 within lines matching each pattern in pattern_list.
    """

    if bash_executable is None:
      bash_executable = get_bash_executable()
      if bash_executable is None:
        print("Cannot proceed without a bash executable.")
        return {}

    results = {}  # Store the results for each pattern
    try:
        for pattern1 in pattern_list:
            # Construct the command as a list and shell escaping for all the arguments
            command = ['grep', pattern1, filename, '|', 'grep', pattern2, '|', 'wc', '-l'] # Shell must exists, and it's not ideal.

            # Execute the command and capture the output
            process = subprocess.Popen(" ".join(map(shlex.quote, command)), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable=bash_executable) # Use dynamically determined bash

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
