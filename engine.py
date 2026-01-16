class Engine:
    """The core DB engine that manages multiple tables."""
    
    def __init__(self):
        self.tables = {}

    def create_table(self, name, schema, primary_key=None, unique_keys=None):
        if name in self.tables:
            raise ValueError(f"Table '{name}' already exists.")
        new_table = Table(name, schema, primary_key=primary_key, unique_keys=unique_keys)
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
    
    def __init__(self, name, schema, primary_key=None, unique_keys=None):
        self.name = name
        # schema: {'col_name': type} e.g., {'id': int, 'name': str}
        self.schema = schema
        self.data = []  # List of dictionaries

        # indexes: {'column_name': {value: [list_of_matching_records]}}
        self.indexes = {}
        self.unique_keys = unique_keys or []

        # Set the primary key
        if primary_key and primary_key not in schema:
            raise ValueError(f"Primary key '{primary_key}' not in schema.")
        self.primary_key = primary_key
        
        # All Primary Keys and Unique keys MUST have an index for fast uniqueness checks
        if self.primary_key:
            self.create_index(self.primary_key)

        for key in self.unique_keys:
            if key not in self.indexes:
                self.create_index(key)

        self._next_id = 1 # Simple auto-increment for a Primary Key behavior

    def create_index(self, column_name):
        """Builds a hash index for an existing column."""
        if column_name not in self.schema:
            raise ValueError(f"Column '{column_name}' does not exist.")
        
        # Initialize the index structure
        self.indexes[column_name] = {}
        
        # Populate the index with existing data
        for record in self.data:
            self._add_to_index(column_name, record)
        return f"Index created on '{column_name}'."
    
    def _add_to_index(self, column_name, record):
        """Internal helper to insert a record into an index."""
        val = record.get(column_name)
        if val not in self.indexes[column_name]:
            self.indexes[column_name][val] = []
        self.indexes[column_name][val].append(record)

    def create_record(self, record_data):
        """Inserts a record after validating against the schema."""
        # 1. Validation: Ensure all columns exist and types match
        for col, value in record_data.items():
            if col not in self.schema:
                raise ValueError(f"Column '{col}' does not exist in table '{self.name}'.")
            if not isinstance(value, self.schema[col]):
                raise TypeError(f"Invalid type for '{col}'. Expected {self.schema[col]}.")
            
        # 2. Primary Key Uniqueness Check
        if self.primary_key:
            pk_val = record_data.get(self.primary_key)
            if pk_val is None:
                raise ValueError(f"Primary key '{self.primary_key}' cannot be null.")
            
            # Use the index to check if this PK already exists (O(1) check)
            if pk_val in self.indexes[self.primary_key]:
                raise ValueError(f"Duplicate entry: PK '{pk_val}' already exists.")

        # 2. Storage
        self.data.append(record_data)
       
        # Update indexes
        for col in self.indexes:
            self._add_to_index(col, record_data)
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
    
    def read_by_index(self, column_name, value):
        """Fast O(1) lookup using the hash index."""
        if column_name not in self.indexes:
            # Fallback to linear search if no index exists
            print(f"No index on {column_name}, performing slow scan...")
            return [r for r in self.data if r.get(column_name) == value]
        
        # Return the list of records from the hash map (dictionary)
        return self.indexes[column_name].get(value, [])
    