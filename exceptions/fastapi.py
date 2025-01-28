from fastapi import Request, status
from fastapi.responses import JSONResponse

class BaseAPIException(Exception):
    """Base class for custom API exceptions."""
    status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR
    error: str = "Internal Server Error"
    message: str = "An unexpected error occurred"

    def __init__(self, message: str = None):
        if message:
            self.message = message

    def to_response(self):
        return JSONResponse(
            status_code=self.status_code,
            content={"error": self.error, "message": self.message},
        )


class BadRequestError(BaseAPIException):
    status_code = status.HTTP_400_BAD_REQUEST
    error = "Bad Request"


class ForbiddenError(BaseAPIException):
    status_code = status.HTTP_403_FORBIDDEN
    error = "Forbidden"


class NotFoundError(BaseAPIException):
    status_code = status.HTTP_404_NOT_FOUND
    error = "Not Found"


class MethodNotAllowedError(BaseAPIException):
    status_code = status.HTTP_405_METHOD_NOT_ALLOWED
    error = "Method Not Allowed"


class ConflictError(BaseAPIException):
    status_code = status.HTTP_409_CONFLICT
    error = "Conflict"


class UnprocessableEntityError(BaseAPIException):
    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    error = "Unprocessable Entity"


class InternalServerError(BaseAPIException):
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    error = "Internal Server Error"




@app.exception_handler(BaseAPIException)
async def custom_api_exception_handler(request: Request, exc: BaseAPIException):
    return exc.to_response()



from fastapi import FastAPI

app = FastAPI()

@app.get("/example")
async def example_route():
    # Simulate a condition that raises a 409 Conflict error
    raise ConflictError("The resource already exists.")

