# -*- coding: utf-8 -*-

import runtime.executor

## ============================================================================================
## KEYWORDS
## ============================================================================================

limit_keywords = set(['limit', 'only', 'just'])
count_keywords = set(['count', 'many', 'number'])
distinct_keywords = set(['distinct','unique'])
project_keywords = set(['project','restrict'])

equal_keywords = set(['equal', 'equals'])
greater_keywords = set(['greater', 'larger', 'higher']) 
smaller_keywords = set(['less', 'smaller', 'lesser', 'lower'])
misc_operator_keywords = set(['between', 'like', 'in'])

operator_keywords = equal_keywords | greater_keywords | smaller_keywords | misc_operator_keywords

connective_keywords = set(['and', 'or', 'not'])

keywords = operator_keywords | connective_keywords | limit_keywords | count_keywords | distinct_keywords | project_keywords

## ============================================================================================
## STATE
## ============================================================================================

table_ref = 0
column_ref = 0

point_lookup = 0
range_lookup = 0

distinct = False
count = False

limit = False
limit_cnt = 0

## ============================================================================================
## UTILITIES
## ============================================================================================

def isFloat(value):
    "Check if value is a float"

    try:
      float(value)
      return True
    except ValueError:
      return False        
          
def isQuoted(value):
    "Check if value is quoted"

    if value.startswith("'") and value.endswith("'"):          
        return True;
    elif value.startswith("\"") and value.endswith("\""):          
        return True;            

    return False;

## ============================================================================================
## HELPERS
## ============================================================================================

def getConnective(token):
    "Check and return connective"
    
    if token == "and":
        return " AND ";
    elif token == "or":
        return " OR "

    return ""


def getLimitString():
    "Get LIMIT string"
    
    if limit is True:
        limit_string = " LIMIT " + str(limit_cnt)
    else:
        limit_string = " "

    return limit_string

def getDoubleQuoted(token):
    "Get token with double quotes in front and back"
    
    return " \"" +  token + "\" "

def getSingleQuoted(token):
    "Get token with single quotes in front and back"
    
    return " '" +  token + "' "

def getOperator(driver, tokens):
    "Get next operator in tokens"
    
    print ("Get Operator :: " + str(tokens))
    greater = False
    lesser = False
    not_seen = False
    or_seen = False
    
    for token in tokens:
        if token == "equal" or token == "equals":
            if not_seen is False:
                return " = "
            else:
                return " <> "

        elif token == "not":
            not_seen = True;
            
        elif token == "greater" or token == "great" or token == "higher" or token == "larger":
            greater = True

            if not_seen:
                lesser = True
                greater = False

        elif token == "lesser" or token == "less" or token == "lower" or token == "smaller":
            lesser = True
            if not_seen:
                lesser = False
                greater = True
            
        elif token == "or":
            or_seen = True
        
        elif token == "between":
            return " BETWEEN "
        
        elif token == "like":
            return " LIKE "
        
        elif token == "in":
            return "IN"

        elif token in driver.column_to_table.keys():
            break;
        
        elif token in driver.tables:
            break;

        else:
            continue;
        
    if greater:
        return " > "
    elif lesser:
        return " < "
        
    return " = "

## ============================================================================================
## PARSER CORE
## ============================================================================================

def parse(driver, query):
        "Parse a human query"
                            
        # Tokenize
        tokens = query.split(' ')
        
        filtered_tokens = []

        global table_ref
        global column_ref
        global point_lookup
        global range_lookup
                  
        table_ref = 0
        column_ref = 0
        point_lookup = 0
        range_lookup = 0
            
        for token in tokens:
            if token in driver.keywords or isFloat(token) or isQuoted(token):                                                
                filtered_tokens.append(token)        
        
                if token in driver.tables:
                    table_ref = table_ref + 1
                    
                if token in driver.column_to_table.keys():
                    column_ref = column_ref + 1                    
                
        print("Filtered Tokens :: " + str(filtered_tokens))


        # Set DISTINCT
        global distinct;        
        distinct = False
        if set(tokens) & distinct_keywords:
            distinct = True

        # Set COUNT
        global count;        
        count = False
        if set(tokens) & count_keywords:
            count = True

        # Set PROJECT
        global project;        
        project = False
        if set(tokens) & project_keywords:
            project = True
        
        # Set LIMIT
        global limit;
        global limit_cnt;
        
        limit = False
        limit_keyword = set(tokens) & limit_keywords
        if limit_keyword:
            limit = True

            try:
                offset = tokens.index(limit_keyword.pop())

                limit_cnt = None
                for token in tokens[offset+1:]:
                    if token.isdigit():
                        limit_cnt = token;
                    
                limit = limit_cnt.isdigit()
                
            except ValueError:
                limit = False;
                return None

        return filtered_tokens