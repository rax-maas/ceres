import requests, sys

response = requests.get(sys.argv[1])
result = response.text
print(result)
if result != "SUCCESS" :
      raise Exception("Ceres end-to-end tests failed..!!")
  
