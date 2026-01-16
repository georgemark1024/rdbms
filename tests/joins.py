from engine import Engine

db = Engine()

# 1. Setup Users
db.create_table("users", {"id": int, "name": str}, primary_key="id")
users = db.get_table("users")
users.create_record({"id": 1, "name": "Alice"})
users.create_record({"id": 2, "name": "Bob"})

# 2. Setup Orders (with an index on 'user_id' for fast joins)
db.create_table("orders", {"order_id": int, "user_id": int, "item": str}, primary_key="order_id")
orders = db.get_table("orders")
orders.create_index("user_id") # This makes the join O(N) instead of O(N^2)

orders.create_record({"order_id": 101, "user_id": 1, "item": "Laptop"})
orders.create_record({"order_id": 102, "user_id": 1, "item": "Mouse"})
orders.create_record({"order_id": 103, "user_id": 2, "item": "Keyboard"})

# 3. Perform the Join
# SQL Equivalent: SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id
joined_data = db.inner_join("users", "orders", left_on="id", right_on="user_id")

for row in joined_data:
    print(row)