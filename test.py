from lambda_function import lambda_handler

event = {"index": "india"}
resp = lambda_handler(event, None)

print(resp)