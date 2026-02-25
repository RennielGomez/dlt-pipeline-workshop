"""Template for building a `dlt` pipeline to ingest data from a REST API."""

import dlt
import os
from datetime import datetime
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator


@dlt.resource(columns={"rate_code":{"data_type":"double"},"mta_tax":{"data_type":"double"}})
def taxi_pipeline_rest_api_resource():
    """Define dlt resources from REST API endpoints."""
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net/",
        # paginator=  OffsetPaginator(offset_param="offset", limit_param="limit", offset=1, limit=1000,maximum_offset=100000,stop_after_empty_page=True,total_path=None)
        # paginator=HeaderLinkPaginator(links_next_key="page")
        paginator=PageNumberPaginator(page=1,page_param="page",total_path=None,stop_after_empty_page=True,maximum_page=100)
    )
    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page

@dlt.source
def taxi_pipeline_rest_api_source():
    yield taxi_pipeline_rest_api_resource()

pipeline = dlt.pipeline(
    pipeline_name='taxi_pipeline',
    destination='duckdb',
    dataset_name='nyc_taxi_data',
    progress="log"
)


if __name__ == "__main__":
    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_filename = f"pipeline_result_{timestamp}.txt"
    results_path = os.path.join(logs_dir, results_filename)
    
    # Start writing the results file immediately
    with open(results_path, 'w') as f:
        f.write("Pipeline Execution Summary\n")
        f.write("="*50 + "\n\n")
        f.write(f"Timestamp: {timestamp}\n")
        f.write(f"Pipeline Name: {pipeline.pipeline_name}\n")
        f.write(f"Destination: {pipeline.destination}\n\n")
        f.write("Starting pipeline execution...\n")
        f.write("-"*50 + "\n\n")
    
    print("Pipeline starting...")  # noqa: T201
    try:
        load_info = pipeline.run(taxi_pipeline_rest_api_source())
        print("Pipeline completed successfully!")  # noqa: T201
        
        # Append execution details to the results file
        with open(results_path, 'a') as f:
            f.write("Pipeline Execution Details:\n")
            f.write(str(load_info))
            f.write("\n\n")
            f.write("Pipeline completed successfully!\n")
        
        print(f"Pipeline results written to: {results_path}")  # noqa: T201
    except Exception as e:
        print(f"Error during pipeline execution: {e}")  # noqa: T201
        import traceback
        import sys
        
        # Capture the full traceback as a string  
        exc_type, exc_value, exc_traceback = sys.exc_info()
        full_traceback = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        
        # Write error to file with full details
        try:
            with open(results_path, 'a') as f:
                f.write("Pipeline Execution FAILED\n")
                f.write(f"Exception Type: {type(e).__name__}\n")
                f.write(f"Exception Message: {str(e)}\n\n")
                
                # Try to extract the cause if available
                cause = e.__cause__ if hasattr(e, '__cause__') else None
                context = e.__context__ if hasattr(e, '__context__') else None
                
                if cause:
                    f.write(f"Caused By: {type(cause).__name__}: {str(cause)}\n\n")
                if context:
                    f.write(f"Context: {type(context).__name__}: {str(context)}\n\n")
                
                f.write("Full Traceback:\n")
                f.write("-"*50 + "\n")
                f.write(full_traceback)
                f.write("-"*50 + "\n")
        except Exception as log_error:
            # If logging fails, try to write a minimal error report
            try:
                with open(results_path, 'a') as f:
                    f.write(f"\nError while logging exception: {str(log_error)}\n")
                    f.write("Original exception:\n")
                    f.write(full_traceback)
            except Exception as fatal_error:  # noqa: BLE001
                print(f"FATAL: Could not write error log: {fatal_error}")  # noqa: T201
                print(f"Original error: {full_traceback}")  # noqa: T201
        
        # Also print to console for immediate feedback
        print(f"\nFull error details written to: {results_path}")  # noqa: T201
        print(full_traceback)  # noqa: T201
