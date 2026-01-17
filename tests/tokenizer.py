from parser import Tokenizer


tokenizer = Tokenizer()
sql = "CREATE TABLE users (id INT, name STR) PRIMARY KEY id"

tokens = tokenizer.tokenize(sql)

for t in tokens:
    print(t)