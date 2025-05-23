async def main():
    targets = ["t1", "t2", "t3"]
    tasks = [asyncio.create_task(fetch_data(t)) for t in targets]

    for task in tasks:
        try:
            result = await asyncio.wait_for(task, timeout=5)
            print(result)
        except asyncio.TimeoutError:
            print("Timeout while fetching a target.")
        except Exception as e:
            print("Unexpected error:", e)

    client.close()



async def main():
    targets = ["t1", "t2", "t3"]
    tasks = [
        asyncio.wait_for(fetch_data(target), timeout=5)
        for target in targets
    ]
    
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            print(r)
    except Exception as e:
        print("Unexpected error:", e)

    client.close()