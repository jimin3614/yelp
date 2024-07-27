#create environment 
#pip3 install 
# () -> function-related
# [] -> Dict, List 

import requests
import json
import mysql.connector
import os 
from dotenv import load_dotenv

 #API reference: https://docs.developer.yelp.com/reference/v3_business_info 

 #Extracting data from Yelp API 
def extract(offset):
    load_dotenv()

    url = "https://api.yelp.com/v3/businesses/search?"
    
    param = {
        "sort_by": "best_match",
        "limit": 50, 
        "offset": offset,
        "location": "New York City",
        "term": "restaurants" 
    }
    headers = {
        "accept": "application/json",
        "Authorization": os.getenv("AUTHORIZATION")
    }
   
    response = requests.get(url, params = param, headers=headers)

    if response.status_code ==200:
        data = response.json()
        
        return data
    
    else:
        print("Error: ", response.status_code)
    
        return

#Transforming data 
def transform(received_data):
    restaurant_info = []

    for i in range(len(received_data["businesses"])):
        temp = []

        if received_data["businesses"][i].get("name"):
            temp.append(received_data["businesses"][i]["name"])
        else:
            temp.append("N/A")

        if received_data["businesses"][i].get("rating"):
            temp.append(received_data["businesses"][i]["rating"])
        else:
            temp.append("N/A")

        if received_data["businesses"][i].get("price"):
            temp.append(received_data["businesses"][i]["price"])
        else:
            temp.append("N/A")            

        if received_data["businesses"][i].get("review_count"):
            temp.append(received_data["businesses"][i]["review_count"])
        else:
            temp.append("N/A")

        if received_data["businesses"][i].get("is_closed"):
            temp.append(received_data["businesses"][i]["is_closed"])
        else:
            temp.append("N/A") 
 
        restaurant_info.append(temp)

    return restaurant_info

#Loading the data to mySQL 
def load(data):
    load_dotenv()

    mydb = mysql.connector.connect(
        host = os.getenv("HOST"),
        user = os.getenv("USER"),
        password = os.getenv("PASSWORD"),
        database = os.getenv("DATABASE")
    )

    mycursor = mydb.cursor()

    for i in range(len(data)):
        sql = f"INSERT INTO yelp VALUES (\"{data[i][0]}\", {data[i][1]}, '{data[i][2]}', {data[i][3]}, '{data[i][4]}')"
        print(sql)
        mycursor.execute(sql)

    try:
        mydb.commit()
        print("Success")
    except:
        print("An error occured")

if __name__ == "__main__":

    offset = 1 

    for i in range(20): #offset limit to 1000 
        if i == 0:
            print(offset)
            data = extract(offset)
        else:
            print(offset)
            offset +=50
            data = extract(offset)

        transformed_data = transform(data)
    # for i in range(len(transformed_data)):
    #     print(transformed_data[i])

        load(transformed_data)


    





