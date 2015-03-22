#!/usr/bin/env python

import parser

## ============================================================================================
## SEQ SCAN
## ============================================================================================

def executeSeqScan(driver, tokens):
    "Execute seq scan"
    
    for token in tokens:
        if token in driver.tables:
                
            if parser.count:
                driver.executeQuery("SELECT COUNT(*) FROM " + token + parser.getLimitString())
            else:                
                driver.executeQuery("SELECT * FROM " + token + parser.getLimitString())
            break

## ============================================================================================
## INDEX SCAN
## ============================================================================================

def executeIndexScan(driver, tokens):
    "Execute index scan"

    lookup_string = ' WHERE '
        
    # Figure out columns
    for token in tokens:
        if token in driver.column_to_table.keys():

            # Attr offset
            offset = tokens.index(token)

            # Value offset
            value = None                    
            for value in tokens[offset:]:
                if value.isdigit() or parser.isQuoted(token) or parser.isFloat(token):
                    break;

            # Get operator
            operator = None
            operator = parser.getOperator(driver, tokens[offset+1:])
            
            print ("Operator : " + operator)

            if value is not None and operator is not None:
                if value.isdigit() or parser.isFloat(token):
                    lookup_string = lookup_string  + parser.getDoubleQuoted(token) + operator + value 
                else:
                    lookup_string = lookup_string  + parser.getDoubleQuoted(token) + operator + parser.getSingleQuoted(value)
                
        lookup_string = lookup_string + parser.getConnective(token)

    # Figure out table
    for token in tokens:
        if token in driver.tables:
            table = token
            break
        
    if parser.count:
        driver.executeQuery("SELECT COUNT(*) FROM " + table + lookup_string + parser.getLimitString())
    else:                
        driver.executeQuery("SELECT * FROM " + table + lookup_string + parser.getLimitString())
        

## EXECUTE
        
def execute(driver, tokens):        

    # Set sequential scan
    if parser.table_ref == 1 and parser.column_ref == 0:
        executeSeqScan(driver, tokens);

    # Set index scan
    if parser.table_ref == 1 and parser.column_ref != 0:
        executeIndexScan(driver, tokens);
            

        return
