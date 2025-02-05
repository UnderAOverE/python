from typing import Dict
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel

app = FastAPI()

# 1. Application Identifier (Core)
class ApplicationIdentifier:
    def __init__(self, business_unit: str, region: str, environment: str, functional_area: str):
        self.business_unit = business_unit
        self.region = region
        self.environment = environment
        self.functional_area = functional_area

    def __repr__(self):  # for printing purposes
        return (f"ApplicationIdentifier(business_unit='{self.business_unit}', region='{self.region}', "
                f"environment='{self.environment}', functional_area='{self.functional_area}')")


# 2. Mapping
class TerminologyMapper:
    def __init__(self, mapping_config: dict):
        self.mapping_config = mapping_config

    def to_application_identifier(self, data: dict) -> ApplicationIdentifier:
        """Translates external terminology to the core ApplicationIdentifier."""
        try:
            business_unit = data[self.mapping_config["business_unit_key"]]
            region = data[self.mapping_config["region_key"]]
            environment = data[self.mapping_config["environment_key"]]
            functional_area = data[self.mapping_config["functional_area_key"]]

            return ApplicationIdentifier(
                business_unit=business_unit,
                region=region,
                environment=environment,
                functional_area=functional_area
            )
        except KeyError as e:
            raise ValueError(f"Missing key in input data: {e}")


# Example configuration
mapping_config = {
    "business_unit_key": "Business_L6",
    "region_key": "Region",
    "environment_key": "Environment",
    "functional_area_key": "Technology_L7"
}


# Dependency to create ApplicationIdentifier
def get_application_identifier(data: dict, mapper: TerminologyMapper = TerminologyMapper(mapping_config)):
    """Dependency to create ApplicationIdentifier."""
    try:
        return mapper.to_application_identifier(data)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# Example API Endpoint
from fastapi import Depends, HTTPException

class AppData(BaseModel):
    Business_L6: str
    Region: str
    Environment: str
    Technology_L7: str


@app.post("/applications")
async def create_application(
        app_data: AppData,  # using Pydantic model as request body
        identifier: ApplicationIdentifier = Depends(get_application_identifier)  # Inject ApplicationIdentifier
):
    """Creates an application."""

    print("Application Identifier:", identifier)  # Core code only uses ApplicationIdentifier
    return {"message": f"Application created for: {identifier}"}

# Example Usage (if you want to run it directly without the API)
if __name__ == "__main__":
    # Sample Data
    application_data = {
        "Business_L6": "Finance",  # Updated key name
        "Region": "North America",
        "Environment": "Production",
        "Technology_L7": "Trading System"  # Updated Key Name
    }

    # Create a FastAPI test client (for easier testing)
    from fastapi.testclient import TestClient
    client = TestClient(app)

    # Send the request to the API
    response = client.post("/applications", json=application_data) #test the request to the api

    # Assert the api has worked correctly.
    assert response.status_code == 200
    print(response.json()) #see the result of api on console.
