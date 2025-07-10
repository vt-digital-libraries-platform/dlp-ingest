import os
if os.getenv("GUI") is not None and os.getenv("GUI").lower() == "true":
    from src.dlp_ingest.src.fixity.lambda_function import lambda_handler
else:
    from src.fixity.lambda_function import lambda_handler

def checksum_handler(event, context):
    response = lambda_handler(event, context)
    
    return response