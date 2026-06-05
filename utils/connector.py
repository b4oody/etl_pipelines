import requests

response = requests.get(
    "https://jsonplaceholder.typicode.com/users",
    timeout=30,
)
response.raise_for_status()

users = response.json()