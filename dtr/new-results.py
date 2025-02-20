import asyncio
import aiofiles
import re
import aioprocessing as mp  # Requires: pip install aioprocessing
import os

# try:
#     import uvloop  # Requires: pip install uvloop
#     asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
#     print("Using uvloop")
# except ImportError:
#     print("uvloop not found, using default event loop")

def process_chunk_in_process(filename, start, size, pattern1, pattern2, queue):
    """
    Processes a chunk of the file in a separate process. This function is designed
    to be run within a process created by `aioprocessing`.  The result is placed on the queue.
    """
    try:
        async def async_process():  # Define an async function inside
            async with aiofiles.open(filename, mode='r') as f:
                await f.seek(start)
                chunk = await f.read(size)
                lines = chunk.splitlines()

            count = 0
            for line in lines:
                if re.search(pattern1, line):
                    if re.search(pattern2, line):
                        count += 1
            queue.put(count)  # Put the result on the queue

        asyncio.run(async_process()) # Run async_process

    except Exception as e:
        print(f"Process {os.getpid()} encountered an error: {e}") # Use getpid() for process ID
        queue.put(0)  # Put a 0 on the queue to signal an error (or handle it differently)


async def count_pattern_occurrences_async_multiprocess(filename, pattern1, pattern2, chunk_size=4096, num_processes=4):
    """
    Asynchronously and in parallel (using multiprocessing) counts occurrences of
    pattern2 within lines matching pattern1.  Compatible with Python 3.9.
    """
    total_count = 0
    tasks = []
    file_size = os.path.getsize(filename)  # Use os.path for file size outside async

    chunk_starts = range(0, file_size, chunk_size)

    # Python 3.9 compatible asyncio.gather:
    for start in chunk_starts:
        size = min(chunk_size, file_size - start)
        # Create a queue for each process to receive the result
        queue = mp.Queue()
        # Use aioprocessing.AioProcess to create a new process
        process = mp.AioProcess(target=process_chunk_in_process, args=(filename, start, size, pattern1, pattern2, queue))
        process.start()
        tasks.append( (process, queue) )

    results = []
    for process, queue in tasks:
        result = await asyncio.get_event_loop().run_in_executor(None, queue.get) # Run a blocking function so we don't block main event loop
        results.append(result)
        process.join() # Important to clean up the process so it doesn't become a zombie

    for result in results:
        if isinstance(result, Exception):
            print(f"A task raised an exception: {type(result).__name__}, {result}") # Show any errors that arise
        else:
           total_count += result # Count all lines

    return total_count


async def main():
    file_name = "large_log.txt"  # Replace with your log file name
    pattern_1 = "error"  # Replace with your first pattern
    pattern_2 = "timeout"  # Replace with your second pattern
    num_processes = os.cpu_count() # Let's use all available cores

    occurrence_count = await count_pattern_occurrences_async_multiprocess(file_name, pattern_1, pattern_2, num_processes=num_processes)

    print(f"Total number of occurrences: {occurrence_count}")

if __name__ == "__main__":
    # Create a large dummy log file (replace 1000000 with the number of lines you want)
    # head /dev/urandom | tr -dc A-Za-z0-9\r\n | head -c 100000000 > large_log.txt
    # Try uvloop only in main so it does not affect other code if uvloop is not installed:
    try:
        import uvloop  # Requires: pip install uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        print("Using uvloop")
    except ImportError:
        print("uvloop not found, using default event loop")
    asyncio.run(main())
