from engine import Engine
from parser import Parser

def run_repl():
    db_engine = Engine()
    parser = Parser(db_engine)
    
    print("Python SimpleDB REPL - Type 'EXIT' to quit.")
    print("-" * 40)
    
    while True:
        try:
            query = input("db> ")
            if query.upper() == "EXIT":
                break
            
            result = parser.execute(query)
            
            # Pretty print results
            if isinstance(result, list):
                if not result:
                    print("Empty set.")
                else:
                    # Print headers
                    headers = result[0].keys()
                    print(" | ".join(headers))
                    print("-" * (len(headers) * 15))
                    for row in result:
                        print(" | ".join(str(v) for v in row.values()))
            else:
                print(result)
                
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    run_repl()