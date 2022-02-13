import requests

def request_url_json(url):
    req = requests.get(url)
    print(req.url)
    if req.status_code == requests.codes.ok:
        response_data = req.json()
        #print(response_data)
    else:
        response_data = {}
        print("HTTP status code: "+str(req.status_code))
        #if req.status_code != 204:
            #TODO
    return response_data

def request_post_json(url: str, data_: dict) -> dict:
  headers = {'Content-Type': 'application/json'}
  req = requests.post(url, data=json.dumps(data_), headers=headers)
  # req = requests.post(url, data=data_)
  print(req.request.url)
  # print(req.request.headers)
  # print(req.request.data)
  
  if req.status_code == requests.codes.ok:
    response_data = req.json()
    # print(response_data)
  else:
    response_data = {}
    print('HTTP status code: ' + str(req.status_code))
    #if req.status_code != 204:
    #TODO
  return response_data