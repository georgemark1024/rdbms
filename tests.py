import time
from engine import Engine

# Initialize Engine
db = Engine()

# 1. Create a Table
db.create_table("users", {"id": int, "name": str, "email": str})
users = db.get_table("users")

# 2. Create Records
users.create_record({"id": 1, "name": "Alice", "email": "alice@example.com"})
users.create_record({"id": 2, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 3, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 4, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 5, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 6, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 7, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 8, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 9, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 10, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 11, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 12, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 13, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 14, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 15, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 16, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 17, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 18, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 19, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 20, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 21, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 22, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 23, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 24, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 25, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 26, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 27, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 28, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 29, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 30, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 31, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 32, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 33, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 34, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 35, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 36, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 37, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 38, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 39, "name": "Bob", "email": "bob@example.com"})
users.create_record({"id": 40, "name": "Bob", "email": "bob@example.com"})

# 3. Read Records (Select * WHERE name == 'Alice')
result = users.read_records(lambda r: r['name'] == "Alice")
# print("Search Result:", result)

# Testing index speed
# normal read (SELECT * WHERE id == 1)
normal_start = time.perf_counter()
id = users.read_records(lambda r: r['id'] == 1)
normal_end = time.perf_counter()

normal_time = normal_end - normal_start
print(f"Normal time = {normal_time}")
print(id)

# create index
users.create_index("id")

# indexed read
indexed_start = time.perf_counter()
id = users.read_by_index("id", 1)
indexed_end = time.perf_counter()

indexed_time = indexed_end - indexed_start
print(f"Indexed time = {indexed_time}")
print(id)

# 4. Update Records
users.update_records({"email": "alice_new@example.com"}, lambda r: r['id'] == 1)

# 5. Delete Records
users.delete_records(lambda r: r['id'] == 2)

# print("Final Table State:", users.data)