{
    "job_id": "job-12345",
    "trigger_type": "cron",
    "trigger_args": {
        "hour": 14,
        "minute": 30,
        "day_of_week": "mon-fri"
    },
    "func": "path.to.your.function",  // e.g., "module_name.function_name"
    "args": [1, 2, 3],
    "kwargs": {
        "key1": "value1",
        "key2": "value2"
    }
}
