from typing import Dict, Optional

from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel

app = FastAPI()

class AppData(BaseModel):
    business_unit: Optional[str] = None
    sector: Optional[str] = None


class InvertedMapper:
    def __init__(self, mapping_config: Dict[str, str]):
        """
        Initializes the mapper with a configuration for inverted keys.
        :param mapping_config: Dict[str, str]: A dictionary mapping source keys to target keys.
        :type mapping_config: Dict[str, str]
        """
        self.mapping_config = mapping_config

    def invert_data(self, data: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
        """
        Converts dict to dictionary mapping source keys to target keys
        :param data: Dict[str, Optional[str]]: A dict of attributes
        :type data: Dict[str, Optional[str]]
        :raises ValueError: If a key not found.
        :returns: converted schema
        :rtype: Dict[str, Optional[str]]
        """
        inverted_data: Dict[str, Optional[str]] = {}
        for source_key, value in data.items():
            if value is not None:
                target_key = self.mapping_config.get(source_key)
                if target_key:
                    inverted_data[target_key] = value
                else:
                    print(f"Skipping unknown source key: {source_key}")
        return inverted_data


# Example Usage
if __name__ == "__main__":
    # Example 1: Sales scenario
    data1 = {"business_unit": "sales", "sector": "b"}

    # Example 2: Business group scenario
    data2 = {"business_unit": "business_l6", "sector": "business_group"}

    # Mapping configuration
    mapping_config = {"business_unit": "business_l6", "sector": "business_group"}

    # Initialize the mapper
    mapper = InvertedMapper(mapping_config=mapping_config)

    # Invert the data using the mapper
    inverted_data1 = mapper.invert_data(data1)
    inverted_data2 = mapper.invert_data(data2)

    # Print the inverted data
    print(f"Inverted Data 1: {inverted_data1}")
    print(f"Inverted Data 2: {inverted_data2}")
