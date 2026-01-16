class Engine:
    """The core DB engine that manages multiple tables."""
    
    def __init__(self):
        self.tables = {}

    def create_table(self, name, schema):
        if name in self.tables:
            raise ValueError(f"Table '{name}' already exists.")
        new_table = Table(name, schema)
        self.tables[name] = new_table
        return f"Table '{name}' created."

    def get_table(self, name):
        if name not in self.tables:
            raise ValueError(f"Table '{name}' not found.")
        return self.tables[name]

    def drop_table(self, name):
        if name in self.tables:
            del self.tables[name]
            return f"Table '{name}' dropped."

class Table:
    """A database table representing a collection of records."""
    
    def __init__(self, name, schema):
        self.name = name
        # schema: {'col_name': type} e.g., {'id': int, 'name': str}
        self.schema = schema
        self.data = []  # List of dictionaries
        self._next_id = 1 # Simple auto-increment for a Primary Key behavior

    def create_record(self, record_data):
        """Inserts a record after validating against the schema."""
        # 1. Validation: Ensure all columns exist and types match
        for col, value in record_data.items():
            if col not in self.schema:
                raise ValueError(f"Column '{col}' does not exist in table '{self.name}'.")
            if not isinstance(value, self.schema[col]):
                raise TypeError(f"Invalid type for '{col}'. Expected {self.schema[col]}.")

        # 2. Storage
        self.data.append(record_data)
        return "Record inserted successfully."

    def read_records(self, filter_func=None):
        """Returns records, optionally filtered by a lambda function."""
        if filter_func:
            return [r for r in self.data if filter_func(r)]
        return self.data

    def update_records(self, updates, filter_func):
        """Updates records that match the filter_func."""
        count = 0
        for record in self.data:
            if filter_func(record):
                record.update(updates)
                count += 1
        return f"Updated {count} records."

    def delete_records(self, filter_func):
        """Deletes records that match the filter_func."""
        original_count = len(self.data)
        self.data = [r for r in self.data if not filter_func(r)]
        return f"Deleted {original_count - len(self.data)} records."
    
