def process_log_file_chunked(log_file, time_delta):
    log_entries = {}
    now_utc = datetime.datetime.now(pytz.utc)
    found_old_entry = False
    chunk_size = 8192  # Adjust as needed (8KB is a common starting point)

    try:
        with open(log_file, 'rb') as f:  # Open in binary mode for accurate seeking
            f.seek(0, os.SEEK_END)  # Go to the end of the file
            file_size = f.tell()
            current_position = file_size

            while current_position > 0 and not found_old_entry:
                # Calculate chunk start position
                read_size = min(chunk_size, current_position)
                current_position -= read_size
                f.seek(current_position, os.SEEK_SET) #Go to position from the start

                # Read the chunk
                chunk = f.read(read_size).decode('utf-8', errors='ignore') # decode and also remove non UTF-8 error

                # Process chunk line by line (in reverse)
                lines = chunk.splitlines()
                for line in reversed(lines):
                    if not line:
                        continue # Skip empty lines that can occur

                    try:
                        log_entry = json.loads(line)
                        log_date_str = log_entry.get('log_date')
                        message = log_entry.get('message', '')

                        if not log_date_str:
                            continue

                        log_date_str_truncated = log_date_str.split('.')[0] + log_date_str[-6:]
                        try:
                           log_date = datetime.datetime.fromisoformat(log_date_str_truncated)
                        except ValueError as e:
                           continue # If datformat cannot be parsed then contineu
                        log_date_utc = log_date.replace(tzinfo=pytz.utc)

                        if now_utc - log_date_utc > time_delta:
                            found_old_entry = True
                            break  # Exit inner loop

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
                                        "btname": btname,
                                        "count": count
                                    })


                    except json.JSONDecodeError:
                        print(f"Skipping invalid JSON line in {log_file}: {line.strip()}")
                    except ValueError as e:
                        print(f"Skipping line in {log_file} due to date parsing error: {e}")
                    except Exception as e:
                        print(f"Error processing line in {log_file}: {e}")

                if found_old_entry:
                    break  # Exit outer loop

    except FileNotFoundError:
        print(f"Log file not found: {log_file}")
    except Exception as e:
        print(f"Error processing log file {log_file}: {e}")

    return log_entries

# In `main()`, replace `process_log_file` with `process_log_file_chunked`.
