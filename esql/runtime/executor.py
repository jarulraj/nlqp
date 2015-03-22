#!/usr/bin/env python

import parser

## ============================================================================================
## SELECT HELPER
## ============================================================================================

def getSelectString(driver, tokens):
    
    select_string = "SELECT "    
    
    if parser.distinct:
        select_string = select_string + " DISTINCT "

    if parser.count:
        select_string = select_string + " COUNT(*) "
    elif parser.project:
        
        project_set = set()
        
        project_keyword = set(tokens) & parser.project_keywords
        offset = tokens.index(project_keyword.pop())
        
        for token in tokens[offset:]:
            if token in driver.columns:
                project_set.add(token)
                
        print("PROJECT SET : " + str(project_set))
        
        for token in list(project_set):
            select_string = select_string + parser.getDoubleQuoted(token) + " , "

        # Trim last 3 chars                
        select_string = select_string[:-3]
                
    else:
        select_string = select_string + " * " 

    select_string = select_string + " FROM "
        
    return select_string
    

## ============================================================================================
## SEQ SCAN
## ============================================================================================

def executeSeqScan(driver, tokens):
    "Execute seq scan"
        
    select_string = getSelectString(driver, tokens)
    
    for token in tokens:
        if token in driver.tables:
                
            driver.executeQuery(select_string + token + parser.getLimitString())
            break

## ============================================================================================
## INDEX SCAN
## ============================================================================================

def executeIndexScan(driver, tokens):
    "Execute index scan"

    select_string = getSelectString(driver, tokens)
    lookup_string = ' WHERE TRUE '
            
    # Figure out attributes
    for idx, token in enumerate(tokens):
        
        # Stop parsing after encountering project
        if token in parser.project_keywords:
            break
        
        if token in driver.column_to_table.keys():
        
            # Value offset
            value = None                 
            for value in tokens[idx+1:]:
                if value.isdigit() or parser.isFloat(value):
                    break;
                
                if parser.isQuoted(value):
                    value = value[1:-1]
                    break;
            
            print ("Value : " + value)

            # Get operator
            operator = None
            operator = parser.getOperator(driver, tokens[idx+1:])
            
            print ("Operator : " + operator)

            if value is not None and operator is not None:
                if lookup_string == ' WHERE TRUE ':
                    lookup_string = ' WHERE '
                
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
        
    driver.executeQuery(select_string + table + lookup_string + parser.getLimitString())
        

## EXECUTE
        
def execute(driver, tokens):

    # Set sequential scan
    if parser.table_ref == 1 and parser.column_ref == 0:
        executeSeqScan(driver, tokens);

    # Set index scan
    if parser.table_ref == 1 and parser.column_ref != 0:
        executeIndexScan(driver, tokens);
            

        return
