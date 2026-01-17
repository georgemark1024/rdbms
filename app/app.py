from flask import Flask, render_template, request, redirect, flash
from engine import Engine
from parser import Parser

import os, sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

app = Flask(__name__)
app.secret_key = "secret_db_key" # For flash messages
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

@app.route('/')
def index():
    search_query = request.args.get('search')
    cats = db.get_table("categories").read_records()
    
    # 1. Get the tasks table
    tasks_table = db.get_table("tasks")
    
    # 2. If searching, use the engine's ability to filter
    if search_query:
        # We simulate the WHERE clause here
        # SQL: SELECT * FROM tasks WHERE name = 'search_query'
        filtered_tasks = tasks_table.read_records(lambda r: search_query.lower() in r['name'].lower())
    else:
        filtered_tasks = tasks_table.read_records()

    # 3. Perform the Join on the (possibly filtered) tasks
    # Our join method needs to handle the filtered list
    joined_data = db.inner_join("tasks", "categories", "cat_id", "id", custom_left_data=filtered_tasks)
    
    return render_template('index.html', categories=cats, joined_tasks=joined_data)

@app.route('/categories')
def show_categories():
    cats = db.get_table("categories").read_records()
    return render_template('categories.html', categories=cats)

@app.route('/add_category', methods=['POST'])
def add_category():
    try:
        cid = request.form['id']
        name = request.form['name']
        # This will trigger Primary Key constraint if ID exists
        msg = parser.execute(f"INSERT INTO categories VALUES ({cid}, '{name}')")
        db.save_to_disk(DB_FILE)
        flash(msg)
    except Exception as e:
        flash(f"Error: {e}")
    return redirect('/categories')

@app.route('/edit/<task_id>')
def edit_view(task_id):
    task = db.get_table("tasks").read_records(lambda r: str(r['id']) == str(task_id))[0]
    cats = db.get_table("categories").read_records()
    return render_template('edit_task.html', task=task, categories=cats)

@app.route('/update_task', methods=['POST'])
def update_task():
    tid = request.form['id']
    new_name = request.form['name']
    new_cat = request.form['cat_id']
    
    # Use your UPDATE logic
    parser.execute(f"UPDATE tasks SET name = '{new_name}' WHERE id = {tid}")
    parser.execute(f"UPDATE tasks SET cat_id = {new_cat} WHERE id = {tid}")
    
    db.save_to_disk(DB_FILE)
    return redirect('/')

@app.route('/delete/<task_id>')
def delete(task_id):
    db.get_table("tasks").delete_records(lambda r: str(r['id']) == str(task_id))
    db.save_to_disk(DB_FILE)
    return redirect('/')

if __name__ == '__main__':
    app.run(port=5000, debug=True)
