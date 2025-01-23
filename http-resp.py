from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import JSONResponse

app = FastAPI()

@app.middleware("http")
async def handle_incorrect_http_methods(request: Request, call_next):
    try:
        # Pass the request to the actual route handler
        response = await call_next(request)
        return response
    except HTTPException as http_exception:
        # Handle HTTPException normally
        raise http_exception
    except Exception:
        # Catch unsupported HTTP methods or any uncaught exceptions
        return JSONResponse(
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
            content={
                "detail": "The HTTP method used is not allowed for this endpoint.",
                "method": request.method,
                "url": str(request.url)
            },
        )




# -----------------------------------------------------

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

app = FastAPI()

@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    """
    Custom handler for 404 Not Found errors.
    """
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={
            "detail": "The requested resource was not found.",
            "method": request.method,
            "url": request.url.path
        }
    )

# Example endpoint for testing
@app.get("/")
async def root():
    return {"message": "Welcome to the API!"}
