import re
from html import escape
from typing import List, Dict, Any
from pydantic import BaseModel, field_validator

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



# Example Usage:
class MyRequestModel(BaseModel):
    name: str
    tags: List[str]
    address: Dict[str, str]
    age: int = 10

    @field_validator('*', mode='before')
    def clean_all_strings(cls, value):
      return sanitize_payload_recursive(value)


class MyRequestModel2(BaseModel):
  name: str
  tags: List[str]
  address: Dict[str, str]
  age: int = 10

  @field_validator('*', mode='before')
  def escape_all_strings(cls, value):
      return html_escape_string_recursive(value)


class MyRequestModel3(BaseModel):
  name: str
  tags: List[str]
  address: Dict[str, str]
  age: int = 10
  
  @field_validator('*', mode='before')
  def clean_all_strings(cls, value):
    return sanitize_payload_recursive(value)
  
  @field_validator('*', mode='before')
  def escape_all_strings(cls, value):
    return html_escape_string_recursive(value)


class MyRequestModel4(BaseModel):
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

  payload3 = {
      "name": "User & Test <script>",
      "tags": ["tag1 <script>", "tag2 'hello'"],
      "address": {"street": "Street 123 &", "city": "City"},
      "age": 25
  }

  payload4 = {
      "name": "User & Test <script>",
      "tags": ["tag1 <script>", "tag2 'hello'"],
      "address": {"street": "Street 123 &", "city": "City"},
      "age": 25
  }

  request_model1 = MyRequestModel(**payload1)
  print("Sanitized Request Model (Regex):", request_model1)

  request_model2 = MyRequestModel2(**payload2)
  print("Sanitized Request Model (HTML):", request_model2)

  request_model3 = MyRequestModel3(**payload3)
  print("Sanitized Request Model (Regex and HTML):", request_model3)

  request_model4 = MyRequestModel4(**payload4)
  print("Sanitized Request Model (Nothing):", request_model4)

  # You could then render it like this (for example):
  html_output1 = f"<p>Name: {request_model1.name}</p>\n"
  html_output1 += f"<p>Tags: {request_model1.tags}</p>\n"
  html_output1 += f"<p>Address: {request_model1.address}</p>\n"
  print("HTML Output (Sanitized Regex) :", html_output1)
  
  html_output2 = f"<p>Name: {request_model2.name}</p>\n"
  html_output2 += f"<p>Tags: {request_model2.tags}</p>\n"
  html_output2 += f"<p>Address: {request_model2.address}</p>\n"
  print("HTML Output (Sanitized HTML) :", html_output2)
  
  html_output3 = f"<p>Name: {request_model3.name}</p>\n"
  html_output3 += f"<p>Tags: {request_model3.tags}</p>\n"
  html_output3 += f"<p>Address: {request_model3.address}</p>\n"
  print("HTML Output (Sanitized Both): ", html_output3)

  html_output4 = f"<p>Name: {request_model4.name}</p>\n"
  html_output4 += f"<p>Tags: {request_model4.tags}</p>\n"
  html_output4 += f"<p>Address: {request_model4.address}</p>\n"
  print("HTML Output (Sanitized None): ", html_output4)
