import azure.functions as func
import logging
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Test HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(
            json.dumps({
                "message": f"Hello, {name}! Function is working!",
                "status": "success"
            }),
            mimetype="application/json",
            status_code=200
        )
    else:
        return func.HttpResponse(
            json.dumps({
                "message": "Please pass a name in the query string or request body",
                "status": "error"
            }),
            mimetype="application/json",
            status_code=400
        )