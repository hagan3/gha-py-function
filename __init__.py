import logging

import azure.functions as func
import shared.utils as utils
from etl.process import Pipeline


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    job_name = req.params.get('job_name')

    if not job_name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            job_name = req_body.get('job_name')

    if not job_name:
        return func.HttpResponse(utils.valid_json_response(f"job_name is required. job_name={job_name}", 400))         

    logging.debug(f'job_name={job_name}')

    Pipeline().get_job(job_name).perform()  

    return func.HttpResponse(utils.valid_json_response("ETL process completed successfully.", 200), status_code=200)

