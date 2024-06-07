def trim_spaces_from_fields(data: dict, fields_to_trim: list) -> dict:
    """
    Recursively trim trailing spaces from specified fields in a dictionary.

    :param data: The dictionary to process.
    :param fields_to_trim: A list of field names to trim.
    :return: The processed dictionary with trimmed fields.
    """
    for key, value in data.items():
        if isinstance(value, dict):
            # If the value is a dictionary, recursively trim it
            trim_spaces_from_fields(value, fields_to_trim)
        elif key in fields_to_trim and isinstance(value, str):
            # If the key is in the fields_to_trim list and value is a string, trim the trailing spaces
            data[key] = value.rstrip()
    return data

# Example dictionary
example_dict = {
    "a": "some text ",
    "b": "other text ",
    "c": "no trim needed",
    "nested": {
        "a": "nested text ",
        "b": "another nested text "
    }
}

# Fields to trim
fields_to_trim = ["a", "b"]

# Process the dictionary
trimmed_dict = trim_spaces_from_fields(example_dict, fields_to_trim)

# Print the processed dictionary
print(trimmed_dict)
