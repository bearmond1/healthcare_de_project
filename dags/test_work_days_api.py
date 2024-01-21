import requests

url = "https://working-days.p.rapidapi.com/1.3/analyse"

querystring = {"start_date":"2013-05-27","end_date":"2013-05-27","country_code":"US","end_time":"18:15","start_time":"09:14"}

headers = {
	"X-RapidAPI-Key": "e9ec906d04msh3dc9b0bd01dc330p1374b8jsn39463cd75bd2",
	"X-RapidAPI-Host": "working-days.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())