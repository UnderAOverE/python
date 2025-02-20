import multiprocessing as mp
import re
import os

def process_chunk(filename, start, size, pattern1, pattern2):
    """Processes a chunk of the file in a separate process (synchronously)."""
    try:
        with open(filename, 'r') as f:
            f.seek(start)
            chunk = f.read(size)
            lines = chunk.splitlines()

        count = 0
        for line in lines:
            if re.search(pattern1, line):
                if re.search(pattern2, line):
                    count += 1
        return count
    except Exception as e:
        print(f"Process {os.getpid()} encountered an error: {e}")
        return 0

def count_pattern_occurrences_multiprocess(filename, pattern1, pattern2, chunk_size=4096, num_processes=4):
    """Counts occurrences using multiprocessing (synchronously)."""
    total_count = 0
    file_size = os.path.getsize(filename)
    chunk_starts = range(0, file_size, chunk_size)

    with mp.Pool(processes=num_processes) as pool:  # Create a pool of worker processes
        results = pool.starmap(process_chunk, [(filename, start, min(chunk_size, file_size - start), pattern1, pattern2) for start in chunk_starts]) # Pass arguments as tuples

    total_count = sum(results)
    return total_count


if __name__ == "__main__":
    file_name = "large_log.txt"  # Replace with your log file name
    pattern_1 = "error"  # Replace with your first pattern
    pattern_2 = "timeout"  # Replace with your second pattern
    num_processes = os.cpu_count() # Let's use all available cores

    occurrence_count = count_pattern_occurrences_multiprocess(file_name, pattern_1, pattern_2, num_processes=num_processes)

    print(f"Total number of occurrences: {occurrence_count}")
