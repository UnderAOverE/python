from typing import List

from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel, ValidationError
import json
from pathlib import Path

app = FastAPI()

class MyModel(BaseModel):
    name: str
    age: int
    city: str  # Make city required for this example

# Function to load JSON data from a file
def load_data_from_json(file_path: Path) -> dict:
    """
    Loads JSON data from a file.

    :param file_path: Path to the JSON file.
    :type file_path: Path
    :raises FileNotFoundError: If the file does not exists.
    :raises ValueError: If the file does not contain a valid json.
    :returns: A dictionary representing the JSON data.
    :rtype: dict
    """
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {file_path}")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON in file: {file_path}")

# Dependency to get model data from JSON file
def get_model_data(file_path: Path) -> MyModel:
    """
    Loads data from json file into the MyModel schema.

    :param file_path: The path to the JSON file.
    :type file_path: Path
    :raises HTTPException: If the loaded data from json are invalid.
    :returns: An instance of MyModel with data from the JSON file.
    :rtype: MyModel
    """
    try:
        data = load_data_from_json(file_path)
        model = MyModel.parse_obj(data)  # Parse the dictionary using Pydantic
        return model
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ValueError, ValidationError) as e:
        raise HTTPException(status_code=400, detail=str(e))


# Load data from all JSON files in a directory
def load_all_data(directory: Path) -> List[MyModel]:
    """
    Loads data from all JSON files in a directory.

    :param directory: The path to the directory containing JSON files.
    :type directory: Path
    :returns: A list of MyModel objects.
    :rtype: List[MyModel]
    """
    all_models: List[MyModel] = []
    for file_path in directory.glob("*.json"):  # Find all .json files
        try:
            model_data = get_model_data(file_path)
            all_models.append(model_data)
        except HTTPException as e:
            print(f"Error loading data from {file_path}: {e.detail}")  # Log error
            # Optionally, raise an exception if you want the process to stop
            # raise e
    return all_models


@app.get("/all_data")
async def get_all_data(all_models: List[MyModel] = Depends(load_all_data)):
    """
    Fetches data from all json files.

    :param all_models: List of schemas based on a json.
    :type all_models: List[MyModel]
    :returns: All the data from each json file.
    :rtype: List[MyModel]
    """
    return all_models

if __name__ == "__main__":
    # Create a directory and sample JSON files
    directory = Path("data_files")
    directory.mkdir(exist_ok=True)

    # Create data sample
    data = [{"name": "John Doe", "age": 30, "city": "New York"}, {"name": "Jane Doe", "age": 25, "city": "Los Angeles"}]

    # Creates 2 json files
    for i, sample_data in enumerate(data):
        file_path = directory / f"data_{i}.json"
        with open(file_path, "w") as f:
            json.dump(sample_data, f)
