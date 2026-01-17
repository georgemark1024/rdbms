import pickle

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

    def inner_join(self, left_table_name, right_table_name, left_on, right_on):
        """
        Performs an Inner Join between two tables.
        Returns a list of combined dictionaries.
        """
        left_table = self.get_table(left_table_name)
        right_table = self.get_table(right_table_name)
        
        results = []

        # Optimization: We iterate through the left table and 
        # use the index on the right table for O(1) lookups.
        for left_row in left_table.data:
            key_value = left_row.get(left_on)
            
            # Use the existing index on the right table if it exists
            # Otherwise, it performs a search (we can fallback to a scan)
            matches = right_table.read_by_index(right_on, key_value)

            for match in matches:
                # Merge the two dictionaries with prefixes to avoid overlaps
                combined_row = {}
                for k, v in left_row.items():
                    combined_row[f"{left_table_name}.{k}"] = v
                for k, v in match.items():
                    combined_row[f"{right_table_name}.{k}"] = v
                results.append(combined_row)

        return results
    
    def save_to_disk(self, filename):
        """Serializes the entire tables dictionary to a file."""
        try:
            with open(filename, 'wb') as f:
                pickle.dump(self.tables, f)
            return f"Database successfully saved to '{filename}'."
        except Exception as e:
            return f"Save failed: {e}"

    def load_from_disk(self, filename):
        """Deserializes the tables dictionary from a file."""
        try:
            with open(filename, 'rb') as f:
                self.tables = pickle.load(f)
            return f"Database successfully loaded from '{filename}'."
        except FileNotFoundError:
            return f"Error: File '{filename}' not found."
        except Exception as e:
            return f"Load failed: {e}"

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

    def update_records(self, updates, filter_func=None):
        """
        updates: dict of {column: new_value}
        filter_func: logic to identify rows to change
        """
        count = 0
        for record in self.data:
            # If no filter is provided, update ALL rows. 
            # If filter is provided, only update matches.
            if filter_func is None or filter_func(record):
                # Update the record data
                for col, val in updates.items():
                    if col in self.schema:
                        # Ensure type consistency
                        record[col] = self.schema[col](val)
                
                # IMPORTANT: If an indexed column was updated, 
                # you would technically need to rebuild that index here.
                count += 1
        return f"Updated {count} records."

    def delete_records(self, filter_func):
        """Deletes records that match the filter_func."""
        original_count = len(self.data)
        if filter_func is None:
            self.data = [] # Truncate table
        else:
            self.data = [r for r in self.data if not filter_func(r)]

        # After deletion, indexes should be cleared/rebuilt
        for col in self.indexes:
            self.create_index(col)
            
        return f"Deleted {original_count - len(self.data)} records."
    
    def read_by_index(self, column_name, value):
        """Fast O(1) lookup using the hash index."""
        if column_name not in self.indexes:
            # Fallback to linear search if no index exists
            print(f"No index on {column_name}, performing slow scan...")
            return [r for r in self.data if r.get(column_name) == value]
        
        # Return the list of records from the hash map (dictionary)
        return self.indexes[column_name].get(value, [])
    