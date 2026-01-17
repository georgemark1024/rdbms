# Simple RDBMS & Task Manager

A custom-built, in-memory Relational Database Management System (RDBMS) implemented from scratch in Python. This project features a full database engine, a SQL-like parser, a CLI REPL, and a Flask-based web application to demonstrate real-world CRUD operations and relational joins.

## 🚀 How to Run the Web App

To launch the Task Manager web interface, ensure you are in the root directory of the project and execute the following command:

```bash
python -m app.app
```

**Note:** Using the `-m` flag ensures that the application handles the modular imports for the engine and parser correctly.

## 🏗️ Architecture

The project is divided into two core modules to maintain a clear separation of concerns:

- **DB Engine (engine.py)**: The core storage logic. It manages tables, handles in-memory data structures (list of dictionaries), and implements high-performance features like Hash Indexing.
- **Parser (parser.py)**: The interface layer. It tokenizes SQL-like strings and routes them to the appropriate engine methods, allowing users to interact with the data using familiar commands.

## ✨ Features

### 🛠️ Core Database Engine

- **Data Types**: Support for INT and STR column types with schema validation.
- **CRUD Operations**: Full support for Creating, Reading, Updating, and Deleting records.
- **Indexing**: Automatic Hash Indexing for Primary Keys and optional columns to achieve $O(1)$ lookup speeds.
- **Data Integrity**: Enforcement of Primary Key (uniqueness and non-null) and Unique Key constraints.
- **Relational Joins**: Implementation of Inner Joins to merge data between tables (e.g., linking tasks to categories).
- **Persistence**: Ability to SAVE the entire database state to disk and LOAD it back using Python's serialization.

### ⌨️ Interface

- **SQL-like Syntax**: A custom parser that understands commands such as SELECT, INSERT, UPDATE, DELETE, and CREATE TABLE.
- **Interactive REPL**: A command-line interface (repl.py) for direct database interaction and debugging.

### 🌐 Web Application

- **Flask Integration**: A task management dashboard that demonstrates the engine's capabilities.
- **Relational Dashboard**: Real-time joining of tasks and categories.
- **Constraint Feedback**: Integrated error handling that displays database constraint violations (like duplicate IDs) directly in the UI.

## 📂 Project Structure

```
.
├── app/
│   ├── app.py           # Flask Web Server
│   └── templates/       # HTML Views (Dashboard, Edit, Categories)
├── engine.py            # RDBMS Core Logic
├── parser.py            # SQL Tokenizer & Command Router
├── repl.py              # CLI Database Interface
├── tests/               # Unit tests for Engine & Parser
└── requirements.txt     # Project Dependencies (Flask)
```

## 🛠️ Requirements

- Python 3.12+
- Flask

## 📖 SQL Reference

The Parser translates the following SQL-like syntax into Engine operations. Keywords are case-insensitive, but identifiers (table/column names) are case-sensitive.

### 1. Data Definition (DDL)

#### Create Table

Defines a new table schema.

```sql
CREATE TABLE table_name (column1 TYPE, column2 TYPE) PRIMARY KEY column1
```

- **Supported Types**: INT, STR
- **Constraints**: PRIMARY KEY (Required for indexing and uniqueness).

### 2. Data Manipulation (DML)

#### Insert Record

Adds a new row to a table.

```sql
INSERT INTO table_name VALUES (val1, 'val2')
```

- Values must match the order of columns defined during creation.
- Strings must be enclosed in single quotes.

#### Update Record

Modifies existing records matching a condition.

```sql
UPDATE table_name SET column1 = 'new_value' WHERE column2 = 'match_value'
```

#### Delete Record

Removes records matching a condition.

```sql
DELETE FROM table_name WHERE column_name = 'value'
```

### 3. Data Querying (DQL)

#### Select All

Retrieves all columns from a table.

```sql
SELECT * FROM table_name
```

#### Select with Filter

Retrieves records matching a specific criteria.

```sql
SELECT * FROM table_name WHERE column_name = 'value'
```

### 4. Database Management

#### Save to Disk

Serializes the current state of all tables to a binary file.

```sql
SAVE 'filename.db'
```

#### Load from Disk

Restores the database state from a binary file.

```sql
LOAD 'filename.db'
```

## 🛠️ Implementation Highlights

- **Tokenizer**: Uses Regular Expressions (re module) to identify keywords, literals, and operators.
- **Command Router**: Uses a dispatcher pattern to route commands to specialized handler methods (_handle_create, _handle_select, etc.).
- **Error Handling**: The parser catches ValueError and IndexError to provide descriptive syntax error messages in the REPL and Web UI.