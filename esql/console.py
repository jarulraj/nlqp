#!/usr/bin/env python

from __future__ import print_function

import os
import sys
import getpass
import psycopg2
import traceback

from colorama import init
from cmd2 import Cmd
from numbers import Number
from tabulate import tabulate
from termcolor import colored

# DB Connection
db_conn = None    

tables = set()
keywords = set(['limit', "only", "just",
                'count', "many", "number",
                'is' , "equal", 'equals', 
                'greater', 'less', 'than', 'to'
                'between', 'like', 'in',
                'and', 'or', 'not',
                'larger', 'smaller', 'higher', 'lower'])


column_to_table = {}
table_to_column = {}

limit = False
limit_cnt = 0

count = False

## ============================================================================================
## SEQ SCAN
## ============================================================================================

def execute_query(query):
    "Execute the query on the given connection"
    
    print(colored("SQL Query :: " + query, 'blue'))
    
    try:
        cursor = db_conn.cursor()                        
        cursor.execute(query)

        columns = [desc[0] for desc in cursor.description]        
                
        rows = cursor.fetchall();               
        if rows is not None:

            try:               
                print (colored(tabulate(rows, headers=columns, tablefmt="psql"), 'red'))
            except UnicodeDecodeError:
                print(colored(rows, 'red'))                

    except Exception:
        print (traceback.format_exc())
        print ("Unable to perform operation. Please try again.")
        if db_conn is not None:
            db_conn.rollback()

    finally:
        if cursor is not None:
            cursor.close()

def isfloat(value):
    "Check if value is a float"

    try:
      float(value)
      return True
    except ValueError:
      return False        
          
def isquoted(value):
    "Check if value is quoted"

    if value.startswith("'") and value.endswith("'"):          
        return True;
    elif value.startswith("\"") and value.endswith("\""):          
        return True;            

    return False;


def get_limit_string():
    "Get LIMIT string"
    
    if limit is True:
        limit_string = " LIMIT " + str(limit_cnt)
    else:
        limit_string = " "

    return limit_string

def get_double_quoted(token):
    "Get token with double quotes in front and back"
    
    return " \"" +  token + "\" "

def get_single_quoted(token):
    "Get token with single quotes in front and back"
    
    return " '" +  token + "' "

def get_operator(tokens):
    "Get next operator in tokens"
    
    print ("Get Operator :: " + str(tokens))
    greater = False
    lesser = False
    not_seen = False
    or_seen = False
    
    for token in tokens:
        if token == "equal" or token == "equals" or token == "is":
            if not_seen is False:
                return " = "
            else:
                return " <> "
        elif token == "not":
            not_seen = True;
        elif token == "greater" or token == "great" or token == "higher" or token == "larger":
            greater = True
        elif token == "lesser" or token == "less" or token == "lower" or token == "smaller":
            lesser = True
        elif token == "or":
            or_seen = True
            if greater is True:               
                return " >= "
            else:
                return " <= "
        elif token == "between":
            return " BETWEEN "
        elif token == "like":
            return " LIKE "
        elif token == "in":
            return "IN"

        elif token in column_to_table.keys():
            break;
        elif token in tables:
            break;

        else:
            continue;
        
    if greater:
        return " > "
    elif lesser:
        return " < "
        
    return " unk "
        
def run_seq_scan(tokens):
    "Execute seq scan"

    for token in tokens:
        if token in tables:
                
            if count:
                execute_query("SELECT COUNT(*) FROM " + token + get_limit_string())
            else:                
                execute_query("SELECT * FROM " + token + get_limit_string())
            break

def get_connective(token):
    "Check and return connective"
    
    if token == "and":
        return " AND ";
    elif token == "or":
        return " OR "

    return ""

def run_index_scan(tokens):
    "Execute index scan"

    lookup_string = ' WHERE '
    
    # Figure out columns
    for token in tokens:
        if token in column_to_table.keys():

            # Attr offset
            offset = tokens.index(token)

            # Value offset
            value = None                    
            for value in tokens[offset:]:
                if value.isdigit() or isquoted(token) or isfloat(token):
                    break;

            # Get operator
            operator = None
            operator = get_operator(tokens[offset+1:])
            
            print ("Operator : " + operator)

            if value is not None and operator is not None:
                if value.isdigit() or isfloat(token):
                    lookup_string = lookup_string  + get_double_quoted(token) + operator + value 
                else:
                    lookup_string = lookup_string  + get_double_quoted(token) + operator + get_single_quoted(value)
                
        lookup_string = lookup_string + get_connective(token)

    # Figure out table
    for token in tokens:
        if token in tables:
            table = token
            break
        
    if count:
        execute_query("SELECT COUNT(*) FROM " + table + lookup_string + get_limit_string())
    else:                
        execute_query("SELECT * FROM " + table + lookup_string + get_limit_string())


## ============================================================================================
## Command Interpreter
## ============================================================================================

class console(Cmd):
    intro = """English to SQL translator.\nType \"help\" for help.\n"""
    
    ruler = '-'
    prompt = colored('query=# ', 'green')

## ============================================================================================
## Basic commands
## ============================================================================================

    def do_clear(self, line):
        "Clear the shell"        
        os.system('clear')    

    def do_ls(self, line):
        "List the current dir"
        os.system('ls')    
        
## ============================================================================================
## Pick database
## ============================================================================================
    
    def do_db(self, line = "parallels"):
        "Pick the database to analyze"
        print ("Looking up database : ", line)

        try:
            db = line
            #c_user = raw_input('Enter username: ')
            #c_password = getpass.getpass(prompt="Enter password : ")
            #c_host = raw_input("Enter hostname : ") 
            #c_port = raw_input("Enter port : ")
    
            #if not c_host:
            c_host = 'localhost'
            #if not c_port:
            c_port = '5432'
                
            c_user = "parallels";
            c_password = "parallels"
            
            global db_conn;            
            cursor = None
            
            db_conn = psycopg2.connect(database=db, user=c_user, password=c_password, 
                                    host=c_host, port=c_port)

                        
            ## POPULATE TABLES
            
            cursor = db_conn.cursor()                        
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

            global tables;
            rows = cursor.fetchall();                
            
            for row in rows:
                table = row[0]
                tables.add(table)
                keywords.add(table)

            ## POPULATE COLUMNS

            global table_to_column;
            global column_to_table;

            for table in tables:
                cursor.execute("SELECT * FROM " + table +" LIMIT 0")
                columns = [desc[0] for desc in cursor.description]
                
                table_to_column[table] = columns;
                
                for column in columns:
                    column_to_table[column] = table;
                    keywords.add(column)
                                                            
        except Exception:
            print (traceback.format_exc())
            print ("Unable to perform operation. Please try again.")
            if db_conn is not None:
                db_conn.rollback()

        finally:
            if cursor is not None:
                cursor.close()

## ============================================================================================
## SQL Query directly
## ============================================================================================
                                
    def do_sql(self, line):
        "Execute SQL query directly"

        if db_conn == None:
            print ("Pick database first")        
        else:
            execute_query(line);


## ============================================================================================
## List tables
## ============================================================================================
                                
    def do_list_tables(self, line):
        "List the tables in the database"

        if db_conn == None:
            print ("Pick database first")        
        else:
            print (tables)

## ============================================================================================
## List columns
## ============================================================================================
            
    def do_list_columns(self, line):
        "List the columns in the table"        
        
        if db_conn == None:
            print ("Pick database first")        
        else:
            table = line;
            try:
                print (table_to_column[table])
            except Exception:
                print (traceback.format_exc())
                print ("Unable to perform operation. Please try again.")
 

    def complete_list_columns(self, text, line, start_index, end_index):        
        "Auto complete list column command"

        if text:
            return [
                table for table in tables
                if table.startswith(text)
            ]
        else:
            return tables

## ============================================================================================
## Parse a query
## ============================================================================================
        
    def do_p(self,line):
        "Parse an english query"
                    
        # Tokenize
        tokens = line.split(' ')
        
        filtered_tokens = []
          
        table_ref = 0
        column_ref = 0
        point_lookup = 0
        range_lookup = 0
            
        for token in tokens:
            if token in keywords or isfloat(token) or isquoted(token):                
                if(isquoted(token)):
                    token = token[1:-1]
                                
                filtered_tokens.append(token)        
        
                if token in tables:
                    table_ref = table_ref + 1
                    
                if token in column_to_table.keys():
                    column_ref = column_ref + 1                    
                
        print("Filtered Tokens :: " + str(filtered_tokens))

        # Set LIMIT
        global limit;
        global limit_cnt;
        
        limit = False
        if "limit" in tokens or "only" in tokens or "just" in tokens:
            limit = True

            try:
                if "limit" in tokens:
                    offset = tokens.index("limit")
                elif "only" in tokens:
                    offset = tokens.index("only")
                else:
                    offset = tokens.index("just")

                limit_cnt = None
                for token in tokens[offset+1:]:
                    if token.isdigit():
                        limit_cnt = token;    
                    
                limit = limit_cnt.isdigit()
                
            except ValueError:
                limit = False;

        # Set COUNT
        global count;
        
        count = False
        if "count" in tokens or "many" in tokens or "number" in tokens:
            count = True
            
        # Set sequential scan
        if table_ref == 1 and column_ref == 0:
            run_seq_scan(filtered_tokens);

        # Set index scan
        if table_ref == 1 and column_ref != 0:
            run_index_scan(filtered_tokens);
            

        # Index scan    
        
        
if __name__ == '__main__':
    
    init()
    
    console = console()
    console.cmdloop()
