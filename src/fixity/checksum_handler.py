from src.fixity.fileCharacterization import lambda_handler

def checksum_handler(event, context):
    response = lambda_handler(event, context)
    
    return response