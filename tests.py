from engine import Engine

# Initialize Engine
db = Engine()

# 1. Create a Table
db.create_table("users", {"id": int, "name": str, "email": str})
users = db.get_table("users")

# 2. Create Records
users.create_record({"id": 1, "name": "Alice", "email": "alice@example.com"})
users.create_record({"id": 2, "name": "Bob", "email": "bob@example.com"})

# 3. Read Records (Select * WHERE name == 'Alice')
result = users.read_records(lambda r: r['name'] == "Alice")
print("Search Result:", result)

# 4. Update Records
users.update_records({"email": "alice_new@example.com"}, lambda r: r['id'] == 1)

# 5. Delete Records
users.delete_records(lambda r: r['id'] == 2)

print("Final Table State:", users.data)