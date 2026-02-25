import json
import os
import re
from dotenv import load_dotenv
from neo4j import GraphDatabase
import traceback

load_dotenv()

URI = os.getenv("NEO4J_URI")
USERNAME = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")
DATABASE = os.getenv("NEO4J_DATABASE")

AUTH = (USERNAME,PASSWORD)

FILE_PATH  = "data/realestate_data.json"

def extract_builder_location(text):
    match = re.search(r'By\s+(.*?)\s+in\s+(.*)', text, re.IGNORECASE)
    
    if match:
        builder = match.group(1).strip()
        location = match.group(2).strip()
        
        # If builder is empty, set to Unknown
        if not builder:
            builder = "Unknown"
    else:
        builder = "Unknown"
        location = "Unknown"  # or "" if you prefer
    
    return {'builder':builder.strip(), 'location':location.strip()}


def extract_currency(text):
    matches = re.findall(r'(\d+\.?\d*)\s*(Cr|L)', text, re.IGNORECASE)

    values = []
    
    for number, unit in matches:
        number = float(number)
        
        if unit.lower() == 'cr':
            number *= 1_00_00_000      # Crore to rupees
        elif unit.lower() == 'l':
            number *= 1_00_000         # Lakh to rupees
            
        values.append(int(number))
    
    if not values:
        return {"min": 0, "max": 0}
    
    if len(values) == 1:
        return {"min": values[0], "max": values[0]}
    
    return {"min": values[0], "max": values[1]}

def extract_area(s):
    match = re.search(r'(\d+(?:\s*-\s*\d+)?)\s*sq\.?ft', s, re.IGNORECASE)    
    if match:
        numbers = re.findall(r'\d+', match.group(1))
        values =  [int(num) for num in numbers]
        if len(values) >0:
            if len(values) == 1:
                return {"min": values[0], "max": values[0]}
            else:
                return {"min": values[0], "max": values[1]}   
    return {"min": 0, "max": 0}       


def load_data(tx, params):
    result = tx.run("""MERGE (prj:Project {name: $prjname}) 
                       MERGE (bldr:Developer {name: $buildername})
                       MERGE (location:Location {name: $location})
                       CREATE (prj)-[:DEVELOPED_BY{launched:$launched,possession:$possession}]->(bldr)
                       CREATE (prj)-[:LOCATED_IN]->(location)
                       WITH prj
                       UNWIND $config AS row
                       CREATE (prp:Property {bhk: row.bhk,
                                           min_price:row.min_price,
                                           max_price:row.max_price,
                                           min_builtup_area:row.min_builtup_area,
                                           max_builtup_area:row.max_builtup_area,
                                           min_carpet_area:row.min_carpet_area,
                                           max_carpet_area:row.max_carpet_area})
                       CREATE (prp)-[:DETAILS_OF]->(prj)
                       """, 
                       prjname=params["prjname"],
                       buildername=params["buildername"],
                       location=params["location"],
                       launched=params["launched"],
                       possession=params["possession"],
                       config = params["config"] 
                       )


def main():
    lstconfig = []
    dictconfig = {}
    try:
        with open(FILE_PATH, 'r') as file:
            real_estate_data = json.load(file)

        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity() 
            print("Connection established successfully.")
            with driver.session(database=DATABASE) as session:
                for data in real_estate_data:
                    try:
                        builder_location = extract_builder_location(data["builder"])
                        launched = data['launched'].replace("Launched:","").strip()
                        possession = data['possession'].replace("Possession:","").strip()

                        for config in data["config"]:                            
                            bhk_list = re.findall(r'z[-+]?\d*\.\d+|\d+', config["bhk"])
                            bhk = bhk_list[0]

                            min_builtup_area = extract_area(config["super_builtup_area"]) ["min"]
                            max_builtup_area = extract_area(config["super_builtup_area"]) ["max"]
                            
                            min_carpet_area = extract_area(config["carpet_area"]) ["min"]
                            max_carpet_area = extract_area(config["carpet_area"]) ["max"]
                            
                            min_price = extract_currency(config["price"]) ["min"]
                            max_price = extract_currency(config["price"]) ["max"]
                            
                            dictconfig["bhk"] = bhk
                            dictconfig["min_builtup_area"] = min_builtup_area
                            dictconfig["max_builtup_area"] = max_builtup_area
                            dictconfig["min_carpet_area"] = min_carpet_area
                            dictconfig["max_carpet_area"] = max_carpet_area
                            dictconfig["min_price"] = min_price
                            dictconfig["max_price"] = max_price
                            lstconfig.append(dictconfig)
                            dictconfig = {}
                        result = session.execute_write(load_data,params=
                                                   {'prjname':data["project"],
                                                    'buildername':builder_location["builder"],
                                                    'location':builder_location["location"],
                                                    'launched':launched,
                                                    'possession':possession,
                                                    'config':lstconfig
                                                    }
                                                    )
                        lstconfig = []

                    except Exception as e:
                        traceback.print_exc()

    except FileNotFoundError:
        print(f"Error: The file {FILE_PATH} was not found.")
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON from the file (invalid JSON format).")


if __name__ == "__main__":
    main()
