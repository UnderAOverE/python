import asyncio
import aiofiles
import re


async def process_chunk(filename, start, size, pattern1, pattern2):
    """Processes a chunk of the file asynchronously."""
    async with aiofiles.open(filename, mode='r') as f:
        await f.seek(start)
        chunk = await f.read(size)
        lines = chunk.splitlines()  # Assuming line-based processing

    count = 0
    for line in lines:
        if re.search(pattern1, line):
            if re.search(pattern2, line):
                count += 1
    return count



async def count_pattern_occurrences_async(filename, pattern1, pattern2, chunk_size=4096):
    """Asynchronously counts occurrences by splitting the file into chunks."""

    total_count = 0
    tasks = []

    async with aiofiles.open(filename, mode='r') as f:
        file_size = await f.seek(0, 2) # Go to end to determine file size
        await f.seek(0) # Reset to beginning

        start = 0
        while start < file_size:
            size = min(chunk_size, file_size - start)
            task = process_chunk(filename, start, size, pattern1, pattern2)
            tasks.append(task)
            start += chunk_size

    results = await asyncio.gather(*tasks)  # Run all tasks concurrently
    total_count = sum(results)
    return total_count


async def main():
    file_name = "large_log.txt"  # Replace with your log file name
    pattern_1 = "error"  # Replace with your first pattern
    pattern_2 = "timeout"  # Replace with your second pattern

    occurrence_count = await count_pattern_occurrences_async(file_name, pattern_1, pattern_2)

    print(f"Number of occurrences: {occurrence_count}")

if __name__ == "__main__":
    asyncio.run(main())
