# TableMerger

## What it does :
   - This script takes 2 csv tables : a base table and an input table.
   - It takes a selection of columns (fields) from the input table, and appends their data to the base table.
        (So at his core, this script just builds a simple SQL "LEFT JOIN")
   - It returns the result in an output csv, without altering the two original files.
   - Only rows from the input table that match a certain amount of configurables conditions are appended to the base table :
        remanining, non-matching data is not kept.
   - All rows from the base table are preserved : they are just completed with the right data from the input table.
   - If two or more rows from the input data match the same row from the base data, the script just keeps the first and ditches the others.
        (The number of such deleted "ambivalents rows" is logged in the end of the execution.)

## Tetchnical setup : 
   -You must have python 3 installed on your machine, or in your python env if you use one (> 3.10 preferably)
   -You also need to install the package pandas, by typing "pip install pandas" into your machine prompt. ("cmd" terminal on Windows)
   -The previous instruction will only work if you have not installed pip yet.
       If it's not the case : install it. (Check these instructions on windows https://phoenixnap.com/kb/install-pip-windows )

## Instructions :
   -This script must be executed within the same directory as the base and input data files. (Just execute "python table_merger.py", or run it on something like spyder)
   -The output data file will be writen within the same directory.
   -CSV files must not be opened in other softwares like excel, for example. (This can lead to concurrency error)
   -At launch, a "menu" allows you yo choose which columns from the input data you want to append to the base data. (this is clearly the dirtiest part of the code, please don't look at it) 

## Configuration :
Modify theses constants at the beginning of the script for customization.
   - BASE_FILENAME (str) : The name of the base file. CSV format is required here, with ";" separators.
   - INPUT_FILENAME (str) : The name of the input file. CSV format is required here, with ";" separators 
   - OUTPUT_FILENAME (str) : The name you want for your output table.
   - ALL_FIELDS_SELECTED_BY_DEFAULT (bool) : When set to True, all fields from the input base are by default selected into the menu. (you can still unselect them after)   
    
   - DEPTH_WITHIN_RANGE_CONFIG (dict) : 
        A special configuration dict that defines how base table and input table are joined based on fields that must be close but not stricly equal.
        (See the "Depth" in the example below)
        For this kind of condition, the match is validated if the input Depth value is close to the base Depth value, "close enough" being defined by **a range**.
        
        The rules below specify how this range is determined.
            - "defaultMarginValue" is the base acceptance range.
            - When "alwaysUseDefaultValue" is True, the script always applies defaultMarginValue and ignores the "marginErrorVariationsConfig"
            - When "alwaysUseDefaultValue" is False, the script consults the "marginErrorVariationsConfig" : the accepted marge then depends on the base table Depth value.
        
        **Example for a "Depth [m]" field in input_base that can match a "Depth [m]" field in base_input**
        The more elevated is the "Depth [m]" is, the more large is the margin of error.
        ```
        {
            "defaultMarginValue": 0.8,
            "alwaysUseDefaultValue": True,
            "marginErrorVariationsConfig": [
                {"min": 0, "max": 200, "margin +/-": 0.2},
                {"min": 200, "max": 800, "margin +/-": 0.2},
                {"min": 800, "max": 2000, "margin +/-": 0.8},
                {"min": 2000, "max": 1000000, "margin +/-": 1},
            ],
        }
        ```
    
   - JOIN_FIELDS_CONFIGURATION (list[dict]) : 
        The main configuration object that defines how base table and input table are joined. (on which fields and on at which conditions)
        Each element of this list represents a relation that must be respected between two rows of the table for being merged together.
        Each relation is caracterized by two fields involved (one on base table, and another on input table), and by a join condition.
        For each item of the configuration list : 
            - FieldNameInBase and FieldNameInInput are required. They define respectively the field in base table and the field in input table that must match.
            - JoinCondition is required too. It can only accepts two value (for now) : "Equality" or "WithinRange"
            - If JoinCondition is "WithinRange", a "SpecialConditionConfig" field is required too, and must follow a format such as "DEPTH_WITHIN_RANGE_CONFIG" described above.
        
        Example :
        This configuration set a double check : one on "Station" field, that must be strictly equal into the two tables, and another on the "Depth [m]", that must match with a margin of error.
        ```
        [
            {
                "FieldNameInBase": "Station",
                "FieldNameInInput": "Station",
                "JoinCondition": "Equality",
            },
            {
                "FieldNameInBase": "Depth [m]",
                "FieldNameInInput": "Depth [m]",
                "JoinCondition": "WithinRange",
                "SpecialConditionConfig": DEFAULT_DEPTH_WITHIN_RANGE_CONFIG,
            },
        ]
        ```
