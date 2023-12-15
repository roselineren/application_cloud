from pymongo import MongoClient
import pandas as pd

def get_database():
 
   # Provide the mongodb atlas url to connect python to mongodb using pymongo
   CONNECTION_STRING = "mongodb+srv://roselineren:0123456789@employee.ddhm6zh.mongodb.net/"
 
   # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
   client = MongoClient(CONNECTION_STRING)
 
   # Create the database for our example (we will use the same database throughout the tutorial
   return client['Projet_cloud']
  
# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":   
  
   # Get the database
   dbname = get_database()

   collection_name = dbname["Employees"]

   item_details = collection_name.find()

   df = pd.DataFrame(item_details)

   print(df.head())

   