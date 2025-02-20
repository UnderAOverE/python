#!/usr/bin/env python3  # Changed shebang to python3

import subprocess
import os
import shlex  # For safely joining shell arguments

def count_pattern_occurrences_with_grep(filename, pattern_list, pattern2):
    """
    Uses grep to count occurrences of pattern2 within lines matching any of the patterns in pattern_list.
    """
    try:
        # Combine patterns in pattern_list with '|' for grep's OR functionality
        combined_pattern1 = '|'.join(pattern_list)

        # Build the grep command pipeline:
        # grep "pattern1_1\|pattern1_2\|..." filename | grep "pattern2" | wc -l
        #command = f"grep '{combined_pattern1}' '{filename}' | grep '{pattern2}' | wc -l"  # Potential security issue with shell=True

        # Safely build the command as a list (avoids shell injection)
        command = ['grep', combined_pattern1, filename, '|', 'grep', pattern2, '|', 'wc', '-l']

        # Execute the command and capture the output
        process = subprocess.Popen(" ".join(map(shlex.quote, command)), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')

        # Get the output and error streams
        stdout, stderr = process.communicate()

        # Check for errors
        if stderr:
            print(f"Error running grep: {stderr.decode()}")
            return 0  # Or raise an exception, depending on how you want to handle errors

        # Parse the output and return the count
        count = int(stdout.decode().strip())
        return count

    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return 0  # Or raise an exception
    except Exception as e:
        print(f"An error occurred: {e}")
        return 0  # Or raise an exception

if __name__ == "__main__":
    file_name = "large_log.txt"  # Replace with your log file name
    pattern_list = ["error", "warn", "critical"]  # Replace with your list of patterns for the first grep
    pattern_2 = "timeout"  # Replace with your second pattern

    occurrence_count = count_pattern_occurrences_with_grep(file_name, pattern_list, pattern_2)

    print(f"Number of occurrences: {occurrence_count}")
