import re
from typing import List, Dict, Any
from pydantic import BaseModel, validator

def sanitize_string(input_str: str) -> str:
    """
    Sanitizes a string by removing or escaping special characters.

    You can customize this function based on what characters you consider harmful.
    """
    # This example uses regex to remove non-alphanumeric characters, spaces are not removed
    return re.sub(r'[^a-zA-Z0-9\s]', '', input_str)


def sanitize_payload_recursive(data: Any):
    """
    Recursively sanitizes strings within a nested data structure (dict, list, or string).
    """
    if isinstance(data, str):
      return sanitize_string(data)
    elif isinstance(data, list):
      return [sanitize_payload_recursive(item) for item in data]
    elif isinstance(data, dict):
      return {key: sanitize_payload_recursive(value) for key, value in data.items()}
    else:
      return data

def sanitize_model(cls):
   """
    A decorator for pydantic models that sanitizes string values
    """
   
   @validator('*', pre=True, always=True)
   def clean_all_strings(cls, value):
        return sanitize_payload_recursive(value)
   return cls

@sanitize_model
class MyRequestModel(BaseModel):
  name: str
  tags: List[str]
  address: Dict[str, str]
  age: int = 10


class MyRequestModel2(BaseModel):
  name: str
  tags: List[str]
  address: Dict[str, str]
  age: int = 10

@sanitize_model
class MyNestedRequestModel(BaseModel):
  payload: MyRequestModel


# Example Usage:
if __name__ == "__main__":
  # Example 1 (sanitized)
  payload1 = {
    "name": "Test User!@#",
    "tags": ["tag1!@#", "tag2$%^"],
    "address": {"street": "Street 123!@#", "city": "City$%^"},
    "age": 25
  }
  request_model1 = MyRequestModel(**payload1)
  print("Sanitized Request Model (1):", request_model1)

  # Example 2 (sanitized)
  payload2 = {
    "name": "Test User",
    "tags": ["tag1", "tag2"],
    "address": {"street": "Street 123", "city": "City"},
    "age": 30
  }
  request_model2 = MyRequestModel(**payload2)
  print("Sanitized Request Model (2):", request_model2)

   # Example 3 (nested)
  payload3 = {
       "payload": {
         "name": "Test User!@#",
         "tags": ["tag1!@#", "tag2$%^"],
         "address": {"street": "Street 123!@#", "city": "City$%^"},
         "age": 25
        }
  }
  request_model3 = MyNestedRequestModel(**payload3)
  print("Sanitized Nested Request Model :", request_model3)

   # Example 4 (un-sanitized)
  payload4 = {
    "name": "Test User!@#",
    "tags": ["tag1!@#", "tag2$%^"],
    "address": {"street": "Street 123!@#", "city": "City$%^"},
    "age": 25
  }
  request_model4 = MyRequestModel2(**payload4)
  print("Unsanitized Request Model :", request_model4)




from html import escape
from typing import List, Dict, Any
from pydantic import BaseModel, validator


def html_escape_string_recursive(data: Any):
    """
    Recursively HTML-escapes strings within a nested data structure (dict, list, or string).
    """
    if isinstance(data, str):
      return escape(data)
    elif isinstance(data, list):
      return [html_escape_string_recursive(item) for item in data]
    elif isinstance(data, dict):
        return {key: html_escape_string_recursive(value) for key, value in data.items()}
    else:
        return data

def html_escape_model(cls):
    """
    A decorator for Pydantic models that HTML-escapes string values.
    """
    @validator('*', pre=True, always=True)
    def escape_all_strings(cls, value):
        return html_escape_string_recursive(value)
    return cls


# Example Usage:
@html_escape_model
class MyRequestModel(BaseModel):
    name: str
    tags: List[str]
    address: Dict[str, str]
    age: int = 10

class MyRequestModel2(BaseModel):
    name: str
    tags: List[str]
    address: Dict[str, str]
    age: int = 10
    
# Example Usage:
if __name__ == "__main__":
  payload1 = {
    "name": "User & Test <script>",
    "tags": ["tag1 <script>", "tag2 'hello'"],
    "address": {"street": "Street 123 &", "city": "City"},
    "age": 25
  }
  
  payload2 = {
      "name": "User & Test <script>",
      "tags": ["tag1 <script>", "tag2 'hello'"],
      "address": {"street": "Street 123 &", "city": "City"},
      "age": 25
  }

  request_model1 = MyRequestModel(**payload1)
  print("HTML Escaped Request Model :", request_model1)

  request_model2 = MyRequestModel2(**payload2)
  print("Not HTML Escaped Request Model :", request_model2)

  # You could then render it like this (for example):
  html_output = f"<p>Name: {request_model1.name}</p>\n"
  html_output += f"<p>Tags: {request_model1.tags}</p>\n"
  html_output += f"<p>Address: {request_model1.address}</p>\n"
  print("HTML Output (Escaped) :", html_output)
  
  html_output2 = f"<p>Name: {request_model2.name}</p>\n"
  html_output2 += f"<p>Tags: {request_model2.tags}</p>\n"
  html_output2 += f"<p>Address: {request_model2.address}</p>\n"
  print("HTML Output (Not Escaped) :", html_output2)
