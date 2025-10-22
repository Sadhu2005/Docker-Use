# You only need to run this cell once in your notebook.
# The '!' runs this as a shell command.
#!pip install pymongo

# -------------------------------------------------
# Now, you can run the rest of this in another cell.
# -------------------------------------------------

from pymongo import MongoClient
from pprint import pprint  # For pretty-printing JSON output

# --- 1. CONNECT TO MONGODB ---

# This is the connection string for the database you set up in docker-compose.
# Format: mongodb://USERNAME:PASSWORD@HOST:PORT/
CONNECTION_STRING = "mongodb://root:password@localhost:27017/"

try:
    # Create a client to connect to your running MongoDB instance
    client = MongoClient(CONNECTION_STRING)
    
    # You can ping the server to confirm a successful connection
    client.admin.command('ping')
    print("✅ Connection Successful!")
    
    # If the database or collection doesn't exist, MongoDB creates it on the fly
    # when you first add data.
    db = client["learning_db"]         # Select/Create a database named 'learning_db'
    collection = db["user_profiles"]  # Select/Create a collection named 'user_profiles'

    # Let's clear the collection from previous runs to start fresh
    collection.delete_many({})
    print("🧹 Cleaned 'user_profiles' collection for a fresh start.")


    # --- 2. CREATE (INSERT) DOCUMENTS ---
    
    print("\n--- 2. CREATE ---")
    
    # Create a single document (Python dictionary)
    doc_one = {
        "name": "Alice",
        "age": 30,
        "job": "Data Scientist",
        "location": "New York"
    }
    insert_result_one = collection.insert_one(doc_one)
    print(f"Inserted one document with ID: {insert_result_one.inserted_id}")

    # Create multiple documents (list of dictionaries)
    docs_many = [
        {"name": "Bob", "age": 25, "job": "Software Engineer", "tags": ["python", "docker"]},
        {"name": "Charlie", "age": 42, "job": "Project Manager", "location": "London"},
        {"name": "Alice", "age": 28, "job": "UX Designer", "location": "Paris"} # A different Alice
    ]
    insert_result_many = collection.insert_many(docs_many)
    print(f"Inserted {len(insert_result_many.inserted_ids)} documents.")


    # --- 3. READ (FIND) DOCUMENTS ---

    print("\n--- 3. READ ---")

    # Find a single document that matches a query
    # This will find the *first* document where the name is "Alice"
    print("\nFinding one document (name='Alice'):")
    alice_profile = collection.find_one({"name": "Alice"})
    pprint(alice_profile)

    # Find all documents that match a query
    # 'find' returns a 'cursor', which is an iterable.
    print("\nFinding all documents (age > 28):")
    query = {"age": {"$gt": 28}}  # $gt means "greater than"
    
    for profile in collection.find(query):
        pprint(profile)

    # Find all documents in the collection
    print("\nFinding ALL documents in collection:")
    all_docs = collection.find({})
    print(f"Found {collection.count_documents({})} total documents.")
    for doc in all_docs:
        pprint(doc)


    # --- 4. UPDATE DOCUMENTS ---

    print("\n--- 4. UPDATE ---")

    # Update a single document
    # Let's give Bob a new job. We use the '$set' operator.
    query_bob = {"name": "Bob"}
    new_values_bob = {"$set": {"job": "Senior Software Engineer", "location": "San Francisco"}}
    
    update_result_one = collection.update_one(query_bob, new_values_bob)
    print(f"Updated {update_result_one.modified_count} document.")

    # Let's check the update
    print("Bob's updated profile:")
    pprint(collection.find_one(query_bob))

    # Update multiple documents
    # Let's add a "company" field to everyone.
    query_all = {} # An empty query matches all documents
    new_company = {"$set": {"company": "Tech Corp Inc."}}
    
    update_result_many = collection.update_many(query_all, new_company)
    print(f"Updated {update_result_many.modified_count} documents with a company name.")


    # --- 5. DELETE DOCUMENTS ---
    
    print("\n--- 5. DELETE ---")

    # Delete a single document
    # Let's delete Charlie
    query_charlie = {"name": "Charlie"}
    delete_result_one = collection.delete_one(query_charlie)
    print(f"Deleted {delete_result_one.deleted_count} document (Charlie).")

    # Delete multiple documents
    # Let's delete everyone named "Alice"
    query_all_alices = {"name": "Alice"}
    delete_result_many = collection.delete_many(query_all_alices)
    print(f"Deleted {delete_result_many.deleted_count} documents (all Alices).")

    # Let's see what's left
    print("\nFinal documents in collection:")
    for doc in collection.find({}):
        pprint(doc)


except Exception as e:
    print(f"❌ An error occurred: {e}")

finally:
    # It's good practice to close the connection,
    # though in scripts it's less critical.
    if 'client' in locals():
        client.close()
        print("\nConnection closed.")
