import re

def count_pattern_occurrences_in_filtered_lines(filename, pattern1, pattern2):
    """
    Searches a file for lines matching pattern1, then filters those lines
    for lines matching pattern2, and counts the number of such lines.

    Args:
        filename (str): The name of the file to search.
        pattern1 (str): The first regular expression pattern to search for.
        pattern2 (str): The second regular expression pattern to search for (within lines matching pattern1).

    Returns:
        int: The number of lines that match both pattern1 and pattern2.
              Returns -1 if there is an error opening the file.
    """

    count = 0
    try:
        with open(filename, 'r') as f:
            for line in f:
                if re.search(pattern1, line):
                    if re.search(pattern2, line):
                        count += 1
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return -1
    except Exception as e:
        print(f"An error occurred: {e}")  # Add more specific error handling if needed
        return -1

    return count


if __name__ == "__main__":
    file_name = "your_log_file.txt"  # Replace with your log file name
    pattern_1 = "error"  # Replace with your first pattern
    pattern_2 = "timeout"  # Replace with your second pattern

    occurrence_count = count_pattern_occurrences_in_filtered_lines(file_name, pattern_1, pattern_2)

    if occurrence_count != -1:
        print(f"Number of occurrences of '{pattern_2}' within lines matching '{pattern_1}' in '{file_name}': {occurrence_count}")

# Example usage to show error handling:
# file_name = "non_existent_file.txt"
# pattern_1 = "pattern1"
# pattern_2 = "pattern2"
# occurrence_count = count_pattern_occurrences_in_filtered_lines(file_name, pattern_1, pattern_2)
#
# if occurrence_count != -1:
#     print(f"Occurrences found: {occurrence_count}")
