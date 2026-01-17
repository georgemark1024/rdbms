from flask import Flask, render_template_string, request, redirect
from engine import Engine
from parser import Parser

import os

app = Flask(__name__)
db = Engine()
parser = Parser(db)
DB_FILE = "task_manager.db"

# Initialize Schema
def init_db():
    if os.path.exists(DB_FILE):
        db.load_from_disk(DB_FILE)
    else:
        # Create categories and tasks tables
        db.create_table("categories", {"id": int, "name": str}, primary_key="id")
        db.create_table("tasks", {"id": int, "name": str, "cat_id": int}, primary_key="id")
        # Add some default categories
        parser.execute("INSERT INTO categories VALUES (1, 'Work')")
        parser.execute("INSERT INTO categories VALUES (2, 'Personal')")
        db.save_to_disk(DB_FILE)

init_db()

# Simple HTML Template
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head><title>RDBMS Task Manager</title></head>
<body>
    <h1>Task Manager (Powered by Custom RDBMS)</h1>
    
    <h3>Add Task</h3>
    <form action="/add" method="post">
        ID: <input type="number" name="id" required>
        Task: <input type="text" name="name" required>
        Category: 
        <select name="cat_id">
            {% for cat in categories %}
                <option value="{{ cat.id }}">{{ cat.name }}</option>
            {% endfor %}
        </select>
        <button type="submit">Add</button>
    </form>

    <h3>Your Tasks</h3>
    <table border="1">
        <tr><th>ID</th><th>Task</th><th>Category</th><th>Action</th></tr>
        {% for row in joined_tasks %}
        <tr>
            <td>{{ row.tasks_id }}</td>
            <td>{{ row.tasks_name }}</td>
            <td>{{ row.categories_name }}</td>
            <td><a href="/delete/{{ row.tasks_id }}">Delete</a></td>
        </tr>
        {% endfor %}
    </table>
</body>
</html>
"""

@app.route('/')
def index():
    categories = db.get_table("categories").read_records()
    # Demonstrate the JOIN feature of your engine
    joined_data = db.inner_join("tasks", "categories", "cat_id", "id")
    return render_template_string(HTML_TEMPLATE, categories=categories, joined_tasks=joined_data)

@app.route('/add', methods=['POST'])
def add():
    tid = request.form['id']
    name = request.form['name']
    cid = request.form['cat_id']
    # Use your SQL Parser!
    query = f"INSERT INTO tasks VALUES ({tid}, '{name}', {cid})"
    parser.execute(query)
    db.save_to_disk(DB_FILE)
    return redirect('/')

@app.route('/delete/<int:task_id>')
def delete(task_id):
    # Demonstrates filtering/deletion
    table = db.get_table("tasks")
    table.delete_records(lambda r: int(r['id']) == task_id)
    db.save_to_disk(DB_FILE)
    return redirect('/')

if __name__ == '__main__':
    app.run(port=5000, debug=True)
