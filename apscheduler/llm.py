import datetime
from pymongo import MongoClient

# MongoDB connection setup
MONGO_URI = "mongodb://localhost:27017/"
client = MongoClient(MONGO_URI)
db = client["your_database_name"]
errors_collection = db["errors"]

def fetch_last_24_hours_errors():
    # Get the current time and calculate the timestamp for 24 hours ago
    current_time = datetime.datetime.utcnow()
    time_24_hours_ago = current_time - datetime.timedelta(hours=24)

    # Query MongoDB for errors within the last 24 hours
    query = {"timestamp": {"$gte": time_24_hours_ago}}
    errors = list(errors_collection.find(query))

    return errors



import openai

# Set your OpenAI API key
openai.api_key = "your-api-key-here"

def summarize_errors_with_llm(errors):
    # Prepare the text for summarization by concatenating error messages
    error_messages = "\n".join([f"{error['module']} - {error['function']}: {error['error_message']}" for error in errors])

    # Request summary from OpenAI
    response = openai.Completion.create(
        model="text-davinci-003",  # or another model you prefer
        prompt=f"Summarize the following error messages:\n\n{error_messages}",
        max_tokens=150
    )

    # Extract and return the summary
    summary = response.choices[0].text.strip()
    return summary


from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()

@app.get("/summarize-errors")
async def summarize_errors():
    # Fetch errors from the last 24 hours
    errors = fetch_last_24_hours_errors()

    if errors:
        # Summarize the errors using the LLM model
        summary = summarize_errors_with_llm(errors)
        return JSONResponse(content={"summary": summary}, status_code=200)
    else:
        return JSONResponse(content={"message": "No errors found in the last 24 hours."}, status_code=404)
