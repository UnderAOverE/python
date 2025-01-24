@app.post(
    "/update-job",
    response_model=UpdateJobResponse,
    status_code=status.HTTP_200_OK
)
async def update_job(request: UpdateJobRequest):
    """
    Updates or reschedules a job based on its job_id.
    Optionally updates the function and its arguments.
    """
    try:
        updated_jobs = update_job_in_scheduler(
            scheduler=scheduler,
            job_id=request.job_id,
            trigger_type=request.trigger_type,
            trigger_args=request.trigger_args,
            func=request.func if hasattr(request, 'func') else None,
            args=request.args if hasattr(request, 'args') else None,
            kwargs=request.kwargs if hasattr(request, 'kwargs') else None
        )
        return UpdateJobResponse(
            message="Job(s) updated successfully.",
            updated_jobs=updated_jobs
        )
    except JobLookupError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.exception("Unexpected error while updating job.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while updating the job."
        )
