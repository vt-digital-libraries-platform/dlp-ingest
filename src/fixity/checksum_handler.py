from src.fixity.lambda_function import lambda_handler

def checksum_handler(event, context):
    response = lambda_handler(event, context)
    
    return response