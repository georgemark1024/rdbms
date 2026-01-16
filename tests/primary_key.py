from engine import Engine

db = Engine()

# Create table with 'id' as the Primary Key
db.create_table("users", {"id": int, "name": str}, primary_key="id")
users = db.get_table("users")

# First insertion: Success
users.create_record({"id": 1, "name": "Alice"})

# Second insertion with same ID: Fails
try:
    users.create_record({"id": 1, "name": "Duplicate Alice"})
except ValueError as e:
    print(f"Error caught: {e}") # Output: Duplicate entry: PK '1' already exists.