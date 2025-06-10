import streamlit as st
from pymongo import MongoClient
import pandas as pd
import os
import json
import ollama
import requests
from bson import json_util
import io

#File upload function
def upload_file():
    try:
        uploaded_file = st.file_uploader("Choose an Excel file", type=["xlsx","csv"]) #ui option to choose excel file
        return uploaded_file
    except Exception:
        print("Error while uploading file")

#Load data to mongoDB
def load_data(file):
    try:
        MONGODB_URI = "mongodb://localhost:27017/"
        DATABASE_NAME = "productInfo"
        COLLECTION_NAME = "productData"
        client = MongoClient(MONGODB_URI)#connecting to mongoDB client
        
        # Get the file extension
        file_extension = os.path.splitext(file.name)[1].lower()
        #Read file data
        if file_extension == '.csv':
            file_data = pd.read_csv(file)
        else:
            file_data = pd.read_excel(file,engine="openpyxl")
        dict_output = file_data.to_dict(orient='records')# convert data to dictionary

        #Database and collection creation
        db_name = client[DATABASE_NAME]
        collect_name = db_name[COLLECTION_NAME]
        
        #inserting data into database from excel
        if dict_output:
            print(f"Inserting data into database '{db_name}'")
            result = collect_name.insert_many(dict_output)
            print(f"Successfully inserted {len(result.inserted_ids)} documents.")
            return dict_output
            if client:
                client.close()
                print("\nMongoDB connection closed.")
        else:
            print("No documents to insert.")
             
    except Exception:
        print("Error while uploading data to database")
    

def generate_mongodb_query_with_ollama(user_input: str, schema: dict) -> dict:
    OLLAMA_API_URL = "http://localhost:11434/api/generate"
    OLLAMA_MODEL = "llama3:8b"
    prompt = f"""
    You are an AI assistant that converts natural language questions into MongoDB find queries.
    The database collection has the following schema (field name: example_type):
    {json_util.dumps(schema, indent=2)}

    Please generate a valid MongoDB find query (the part that goes inside collection.find())
    as a JSON object. Do NOT include collection.find() or any other Python code.
    Only provide the JSON object for the query.

    Example:
    User Input: "Find products with price greater than 50"
    Generated Query: {{ "Price": {{ "$gt": 50 }} }}

    User Input: "List products launched after January 1, 2022, in the Home & Kitchen or Sports categories with a discount of 10% or more, sorted by price in descending order."
    Generated Query: {{ "$and": [ {{ "Launch Date": {{ "$gt": "2022-01-01" }} }}, {{ "Category": {{ "$in": ["Home & Kitchen", "Sports"] }} }}, {{ "Discount": {{ "$gte": 0.10 }} }} ] }}

    User Input: "Find all products with a rating below 4.5 that have more than 200 reviews and are offered by the brand 'Nike' or 'Sony'."
    Generated Query: {{ "$and": [ {{ "Rating": {{ "$lt": 4.5 }}}}, {{ "Reviews": {{ "$gt": 200 }}}}, {{ "Brand": {{ "$in": ["Nike", "Sony"] }} }} ] }}

    User Input: "Which products in the Electronics category have a rating of 4.5 or higher and are in stock?"
    Generated Query: {{ "$and": [ {{ "Category": "Electronics" }}, {{ "Rating": {{ "$gte": 4.5 }} }}, {{ "In Stock": true }} ] }}

    Your task is to generate the MongoDB query for the following user input:
    "{user_input}"

    Ensure the query is syntactically correct MongoDB JSON.
    For date comparisons, assume dates are stored as strings in 'YYYY-MM-DD' format if not specified otherwise.
    For boolean fields like "In Stock", assume true/false (lowercase).
    """
    headers = {'Content-Type': 'application/json'}
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False, # We want a single response, not a stream
        "format": "json" # Ask Ollama to format its output as JSON
    }
    try:
        response = requests.post(OLLAMA_API_URL, headers=headers, data=json.dumps(payload))
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        
        # Ollama's /api/generate endpoint returns a JSON object with a 'response' field
        # that contains the actual generated text.
        result = response.json()
        generated_text = result.get('response', '').strip()

        # Clean up potential markdown code block formatting
        # Llama 3 often wraps JSON in ```json ... ```
        if generated_text.startswith('```json') and generated_text.endswith('```'):
            json_string = generated_text[len('```json'):-len('```')].strip()
        else:
            json_string = generated_text

        # Attempt to parse the JSON string into a Python dictionary
        mongodb_query = json.loads(json_string)
        st.success("Query generated successfully!")
        return mongodb_query
    
    except requests.exceptions.ConnectionError:
        st.error(f"Error: Could not connect to Ollama server at {OLLAMA_API_URL}.")

def load_output_llama(record_dict):
    user_input = st.text_input(
        label ="Enter Text Here:",
        value ="",  # Initial value of the text box
        max_chars = 200, # Optional: Maximum number of characters allowed
        help ="Type anything you want!", # Optional: Tooltip for the input box
        placeholder ="Start typing...", # Optional: Placeholder text when empty
        key ="text_input_box"
    )
    
    if st.button("Get Result", key="get_result_button"):
        if user_input:
            with st.spinner("Generating query..."):
                generated_mongodb_query = generate_mongodb_query_with_ollama(user_input, record_dict)
                if generated_mongodb_query:
                    st.subheader("Generated MongoDB Query:")
                    st.code(json.dumps(generated_mongodb_query, indent=2), language="json")
                    return generated_mongodb_query
                else:
                    st.warning("Could not generate a valid MongoDB query")

        else:
            st.error("Please enter a query before clicking the button.")

        
def get_result(query):
    try:
        MONGODB_URI = "mongodb://localhost:27017/"
        DATABASE_NAME = "productInfo"
        COLLECTION_NAME = "productData"
        client = MongoClient(MONGODB_URI)
        db_name = client[DATABASE_NAME]
        collect_name = db_name[COLLECTION_NAME]
        
        data_cursor = collect_name.find(query)
        data = list(data_cursor) # Convert cursor to a list of dictionaries

        if not data:
            st.warning("No data found")

        # Convert list of dictionaries to Pandas DataFrame
        df = pd.DataFrame(data)

        # Remove the MongoDB '_id' field if it exists, as it's often not needed in Excel
        if '_id' in df.columns:
            df = df.drop(columns=['_id'])

        # Create an in-memory Excel file
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='MongoDB Data')
        processed_data = output.getvalue()

        st.success(f"Successfully fetched {len(data)} documents and generated Excel file!")

        # Provide download button
        st.download_button(
            label="Download Excel File",
            data=processed_data,
            file_name="mongodb_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
    except Exception:
        print("Error while displaying query data")

if __name__ == '__main__':
    # try:
        path = upload_file()
        if path:
            record_dict = load_data(path)
            query = load_output_llama(record_dict)
            if query:
                get_result(query)

    # except Exception:
    #     print("Someting went wrong")




