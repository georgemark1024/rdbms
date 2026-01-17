import re

class Tokenizer:
    """Converts a SQL string into a list of meaningful tokens."""
    
    TOKEN_SPECIFICATION = [
        ('KEYWORD',    r'\b(CREATE|TABLE|INSERT|INTO|VALUES|SELECT|FROM|WHERE|JOIN|ON|PRIMARY|KEY|UNIQUE|INT|STR)\b'),
        ('NUMBER',     r'\d+'),                     # Integer literals
        ('STRING',     r"'(?:[^'\\]|\\.)*'"),       # String literals inside single quotes
        ('ID',         r'[a-zA-Z_][a-zA-Z0-9_]*'),  # Identifiers (table/column names)
        ('OP',         r'[=*,()]'),                 # Operators and punctuations
        ('SKIP',       r'[ \t\n]+'),                # Whitespace (to be ignored)
        ('MISMATCH',   r'.'),                       # Any other character
    ]

    def __init__(self):
        # Compile all patterns into one big regular expression
        self.regex = re.compile('|'.join(f'(?P<{name}>{pattern})' 
                                       for name, pattern in self.TOKEN_SPECIFICATION), re.IGNORECASE)

    def tokenize(self, text):
        tokens = []
        for mo in self.regex.finditer(text):
            kind = mo.lastgroup
            value = mo.group()
            
            if kind == 'KEYWORD':
                tokens.append((kind, value.upper())) # Normalize keywords to uppercase
            elif kind == 'NUMBER':
                tokens.append((kind, int(value)))
            elif kind == 'STRING':
                tokens.append((kind, value.strip("'"))) # Remove quotes
            elif kind == 'ID':
                tokens.append((kind, value))
            elif kind == 'OP':
                tokens.append((kind, value))
            elif kind == 'SKIP':
                continue
            elif kind == 'MISMATCH':
                raise RuntimeError(f'Unexpected character: {value}')
                
        return tokens
    

class Parser:
    def __init__(self, engine):
        self.engine = engine
        self.tokenizer = Tokenizer()

    def execute(self, sql_string):
        try:
            tokens = self.tokenizer.tokenize(sql_string)
            if not tokens: return None
            
            _, command = tokens[0]
            if command == 'CREATE': return self._handle_create(tokens[1:])
            if command == 'INSERT': return self._handle_insert(tokens[1:])
            if command == 'SELECT': return self._handle_select(tokens[1:])
            return f"Error: Unknown command '{command}'"
        except Exception as e:
            return f"Syntax Error: {str(e)}"

    def _handle_create(self, tokens):
        # Syntax: TABLE <name> ( <col> <type>, ... ) PRIMARY KEY <col>
        name = tokens[1][1]
        schema = {}
        # Simple loop to find columns inside ()
        idx = 3
        while tokens[idx][1] != ')':
            col_name = tokens[idx][1]
            col_type = int if tokens[idx+1][1] == 'INT' else str
            schema[col_name] = col_type
            idx += 2
            if tokens[idx][1] == ',': idx += 1
        
        pk = None
        if len(tokens) > idx + 1 and tokens[idx+1][1] == 'PRIMARY':
            pk = tokens[idx+3][1]
            
        return self.engine.create_table(name, schema, primary_key=pk)

    def _handle_insert(self, tokens):
        # Target syntax: INTO <table_name> VALUES ( <val1>, <val2> )
        if tokens[0][1] != 'INTO':
            raise ValueError("Expected 'INTO' after 'INSERT'")
        
        table_name = tokens[1][1]
        table = self.engine.get_table(table_name)
        
        # Find where the values start (after 'VALUES' and '(' )
        try:
            values_start = -1
            for i, (kind, value) in enumerate(tokens):
                if kind == 'OP' and value == '(':
                    values_start = i + 1
                    break
            
            if values_start == -1:
                raise ValueError("Expected '(' before values")

            # Extract values until we hit ')'
            raw_values = []
            for i in range(values_start, len(tokens)):
                kind, val = tokens[i]
                if kind == 'OP' and val == ')':
                    break
                if val != ',': # Skip commas
                    raw_values.append(val)
            
            # Map values to schema and handle type conversion
            cols = list(table.schema.keys())
            if len(raw_values) != len(cols):
                raise ValueError(f"Expected {len(cols)} values, got {len(raw_values)}")
            
            record = {}
            for i, col_name in enumerate(cols):
                expected_type = table.schema[col_name]
                record[col_name] = expected_type(raw_values[i])
                
            return table.create_record(record)
        except Exception as e:
            raise ValueError(f"Insert failed: {str(e)}")

    def _handle_select(self, tokens):
        # Search for the table name following the 'FROM' keyword
        try:
            # Find the index of the 'FROM' token
            from_index = -1
            for i, (kind, value) in enumerate(tokens):
                if kind == 'KEYWORD' and value == 'FROM':
                    from_index = i
                    break
            
            if from_index == -1 or from_index + 1 >= len(tokens):
                raise ValueError("Syntax Error: Expected table name after FROM")
            
            table_name = tokens[from_index + 1][1]
            table = self.engine.get_table(table_name)
            
            # In a real RDBMS, we'd handle WHERE here. 
            # For now, we return all records.
            return table.read_records()
            
        except IndexError:
            raise ValueError("Syntax Error: SELECT statement is incomplete.")