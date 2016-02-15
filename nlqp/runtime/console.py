#!/usr/bin/env python

from __future__ import print_function

import os
import colorama

from cmd2 import Cmd
from termcolor import colored

import drivers
import parser

from executor import execute

# Driver
driver = None

## ============================================================================================
## Command Interpreter
## ============================================================================================

class console(Cmd):
    intro = """English to SQL translator.\nType \"help\" for help.\n"""
    
    prompt = colored('query=# ', 'green')

    ## Basic commands

    def do_clear(self, line):
        "Clear the shell"        
        os.system('clear')    

    def do_ls(self, line):
        "List the contents of current dir"
        os.system('ls')    

    def do_pwd(self, line):
        "List the current dir"
        os.system('pwd')    

    ## Execute SQL query directly
                                
    def do_sql(self, query):
        "Execute SQL query directly"
        driver.executeQuery(query);
        
    ## Select database
    
    def do_db(self, db_name):
        "Select the database to analyze"
                
        print ("Looking up database : ", db_name)
        driver.selectDatabase(db_name)

    ## List tables
                                
    def do_list_tables(self, line):
        "List the tables in the database"        
        tables = driver.listTables()
        
        if tables is not None:
            print (tables)

    ## List columns
            
    def do_list_columns(self, table_name):
        "List the columns in the table"        
        columns = driver.listTableColumns(table_name)
        
        if columns is not None:
            print(columns)

    def complete_list_columns(self, text, line, start_index, end_index):        
        "Auto complete list column"

        if text:
            return [
                table for table in driver.listTables()
                if table.startswith(text)
            ]
        else:
            return tables

    ## Parse an human query
        
    def do_p(self, query):
        "Parse an human query"
                               
        tokens = parser.parse(driver, query)

        execute(driver, tokens)
                
## ==============================================
## loadDriverClass
## ==============================================

def loadDriverClass(name):
    "Load the appropriate driver"
    
    full_name = "%sDriver" % name.title()

    driver_name = "%s_driver" % name.title().lower()
    mod = __import__('drivers.%s' % driver_name, globals(), locals(), [full_name])

    print (colored("Loaded :: " + full_name, 'red'))    
    klass = getattr(mod, full_name)
    return klass

## ==============================================
## getDrivers
## ==============================================
def getDrivers():
    "Get driver list"
    
    drivers = []
    for f in map(lambda x: os.path.basename(x).replace("_driver.py", ""), glob.glob("./drivers/*_driver.py")):
        if f != "abstract": 
            drivers.append(f)

    return (drivers)

## ==============================================
## Entry point for console
## ==============================================        
def startConsole():
    "Start Console"
        
    system = 'postgres'
    
    ## Create a handle to the target client driver
    global driver
    
    driver_class = loadDriverClass(system)
    assert driver_class != None, "Failed to find '%s' class" % system
    driver = driver_class()
    
    # Start console    
    c = console()        
    c.cmdloop()
