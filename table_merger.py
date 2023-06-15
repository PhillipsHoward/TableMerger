# -*- coding: utf-8 -*-
"""
Created on Wed Jan 12 12:08:29 2022

@author: Thibault P
"""

import pandas as pd
import time
import sqlite3


# README /!\ : https://github.com/PhillipsHoward/TableMerger

BASE_FILENAME = "SWINGS_CTD_clean_nut.csv"
INPUT_FILENAME = "swings_ctd_completed_O2.csv"
OUTPUT_FILENAME = "output.txt"
ALL_FIELDS_SELECTED_BY_DEFAULT = False
DEPTH_WITHIN_RANGE_CONFIG = {
    "defaultMarginValue": 0.8,
    "alwaysUseDefaultValue": True,
    "marginErrorVariationsConfig": [
        {"min": 0, "max": 200, "margin +/-": 0.8},
        {"min": 200, "max": 800, "margin +/-": 0.8},
        {"min": 800, "max": 2000, "margin +/-": 0.8},
        {"min": 2000, "max": 1000000, "margin +/-": 0.8},
    ],
}
JOIN_FIELDS_CONFIGURATION = [
    {
        "FieldNameInBase": "Station",
        "FieldNameInInput": "Station",
        "JoinCondition": "Equality",
    },
    {
        "FieldNameInBase": "Depth [m]",
        "FieldNameInInput": "Depth [m]",
        "JoinCondition": "WithinRange",
        "SpecialConditionConfig": DEPTH_WITHIN_RANGE_CONFIG,
    },
]


def extract_potentials_fields_to_add_from_input(input_data):
    """ Extract from input_data the potential fields to append to base. """
    return input_data.columns.values.tolist().copy()[  # TODO This should not stay hardcoded like that
        1:
    ]


def generate_selection_menu(all_fields, all_selected=False):
    """
    Create a menu that let the user choose the fields from the input table to add to the base table.
    ( Sorry for this dirty function. This should be refactored someday. )
    """
    print("Select the fields to append from the new input matrix to the reference one.")

    if all_selected:
        selected_fields = all_fields.copy()
    else:
        selected_fields = []
    exit_index = len(all_fields) + 1

    def print_menu():
        """Generate dynamically the menu options selection"""
        for index, value in enumerate(all_fields):
            print(
                str(index + 1),
                "--",
                value,
                "SELECTED" if value in selected_fields else "UNSELECTED",
            )
        print("----")
        print(str(exit_index), "--", "VALIDATE")

    def reorder_fields(selected_fields, all_fields):
        """ Reorder selected fields into the original input table order """
        ordered_fields = []
        for field in all_fields:
            if field in selected_fields:
                ordered_fields.append(field)
        return ordered_fields

    while True:
        print_menu()
        option = ""
        try:
            option = int(input("Type the field number to select it or unselect it : "))
        except:
            print("Wrong input. Please enter a number ...")
        # Check what choice was entered and act accordingly
        if 1 <= option and option <= len(all_fields):
            if all_fields[option - 1] in selected_fields:
                print("Field unselected")
                selected_fields.remove(all_fields[option - 1])
            else:
                print("Field selected")
                selected_fields.append(all_fields[option - 1])
        elif option == exit_index:
            print("Selection done.")
            return reorder_fields(selected_fields, all_fields)
            break
        else:
            print(
                "Invalid option. Please enter a number between 1 and "
                + str(exit_index)
                + "."
            )


class DataTablesMerger:
    '''
    A class that merges two data tables based on common fields.

    :param base_data: The base data table to which the input data table will be joined.
    :type base_data: pandas.DataFrame
    :param input_data: The input data table to be joined to the base data table.
    :type input_data: pandas.DataFrame
    :param fields_to_append_list: The list of fields from the input data table to append to the base data table.
    :type fields_to_append_list: list of str
    :param join_fields_configuration: The configuration of the join fields between the two tables.
    :type join_fields_configuration: list of dict (see README for format example)
    :ivar pre_query_select_parts: The list of select query parts that will be added to the main join query.
    :type pre_query_select_parts: list of str
    :ivar pre_query_join_parts: The list of join query parts that will be added to the main join query.
    :type pre_query_join_parts: list of str
    :ivar join_fields_name_list: The list of fields names used to establish the join. 
    :type join_fields_name_list: list of str
    :ivar start_time: The time at which the merge process started.
    :type start_time: float
    '''
    
    def __init__(
        self,
        base_data,
        input_data,
        fields_to_append_list,
        join_fields_configuration,
    ):
        self.base_data = base_data
        self.input_data = input_data
        self.fields_to_append_list = fields_to_append_list
        self.join_fields_configuration = join_fields_configuration
        self.pre_query_select_parts = []
        self.pre_query_join_parts = []
        self.join_fields_name_list = [
            configuration_element["FieldNameInBase"]
            for configuration_element in self.join_fields_configuration
        ]
        self.start_time = time.time()

    def logger(self, raw_message="", display_time=True):
        message = ""
        if display_time:
            message = f"-- time : {(time.time() - self.start_time):.2f} s - "
        else:
            message += "-- "
        print(message + raw_message)

    def get_marge_error_according_to_config(self, value, join_within_range_config):
        variations_config = join_within_range_config["marginErrorVariationsConfig"]
        default_margin_error = join_within_range_config["defaultMarginValue"]

        for variation_range in variations_config:
            if variation_range["min"] < value and variation_range["max"] >= value:
                return variation_range["margin +/-"]
        return default_margin_error

    def add_helpers_columns_for_join_within_range_field(
        self, field_in_input, join_within_range_config
    ):
        marg_of_error_column_name = f"{field_in_input}_mg_error"
        max_limit_column_name = f"max_{field_in_input}"
        min_limit_column_name = f"min_{field_in_input}"

        if join_within_range_config["alwaysUseDefaultValue"] is True:
            self.input_data[marg_of_error_column_name] = join_within_range_config[
                "defaultMarginValue"
            ]
        else:
            self.input_data[marg_of_error_column_name] = self.input_data.apply(
                lambda row: self.get_marge_error_according_to_config(
                    row[field_in_input],
                    join_within_range_config=join_within_range_config,
                ),
                axis=1,
            )
        self.input_data[min_limit_column_name] = self.input_data.apply(
            lambda row: row[field_in_input] - row[marg_of_error_column_name],
            axis=1,
        )
        self.input_data[max_limit_column_name] = self.input_data.apply(
            lambda row: row[field_in_input] + row[marg_of_error_column_name],
            axis=1,
        )

    def write_within_range_conditions(self, field_in_base, field_in_input):
        return [
            f"base_data.'{field_in_base}' > input_data.'min_{field_in_input}'",
            f"base_data.'{field_in_base}' <= input_data.'max_{field_in_input}'",
        ]

    def setup_one_within_range_join(
        self, field_in_base, field_in_input, join_within_range_config
    ):

        self.add_helpers_columns_for_join_within_range_field(
            field_in_input=field_in_input,
            join_within_range_config=join_within_range_config,
        )
        query_conditions = self.write_within_range_conditions(
            field_in_base, field_in_input
        )
        self.pre_query_join_parts += query_conditions

    def setup_within_range_joins(self):
        for join_field_configuration in self.join_fields_configuration:
            if (
                "JoinCondition" in join_field_configuration
                and join_field_configuration["JoinCondition"] == "WithinRange"
            ):
                self.setup_one_within_range_join(
                    field_in_base=join_field_configuration["FieldNameInBase"],
                    field_in_input=join_field_configuration["FieldNameInInput"],
                    join_within_range_config=join_field_configuration[
                        "SpecialConditionConfig"
                    ],
                )

    def setup_equality_joins(self):
        for join_field_configuration in self.join_fields_configuration:
            if (
                "JoinCondition" in join_field_configuration
                and join_field_configuration["JoinCondition"] == "Equality"
            ):
                self.pre_query_join_parts += [
                    f"base_data.'{join_field_configuration['FieldNameInBase']}' = input_data.'{join_field_configuration['FieldNameInInput']}'"
                ]

    def build_final_query(self):
        select_query_section = ", ".join(
            ["SELECT base_data.*"]
            + [
                f"input_data.'{field}'"
                if field not in self.join_fields_name_list
                else f"input_data.'{field}' AS '{field}_input'"
                for field in self.fields_to_append_list
            ]
        )  
        """ We add suffixe "_input" for fields that are used as join fields,
        otherwise they would lead to falsy ambivalent row deletions. (see "drop_ambivalent_matching_rows()")"""
        join_query_section = " AND ".join(self.pre_query_join_parts)

        query = f"""
            {select_query_section}
            FROM base_data
            LEFT JOIN input_data
            ON {join_query_section} 
            """
        return query

    def join_tables(self, join_query):
        conn = sqlite3.connect("TheBlattabase")
        self.base_data.to_sql("base_data", conn, index=False, if_exists="replace")
        self.input_data.to_sql("input_data", conn, index=False, if_exists="replace")
        raw_data = pd.read_sql_query(join_query, conn)
        return raw_data

    def drop_ambivalent_matching_rows(self, data):

        initial_rows_number = len(data.index)
        cleaned_data = data.drop_duplicates(
            subset=self.join_fields_name_list, keep="first"
        )
        self.logger(
            f"{initial_rows_number-len(cleaned_data.index)} ambivalent rows had to be removed."
        )
        return cleaned_data

    def launch(self):
        self.start_time = time.time()
        self.logger("Job launched")
        self.logger("-------------", display_time=False)

        self.logger("Prepare join.")
        self.setup_equality_joins()
        self.setup_within_range_joins()
        self.logger("-------------", display_time=False)

        self.logger("Build query")
        join_query = self.build_final_query()
        # print(join_query) # kept for debug purpose
        self.logger("-------------", display_time=False)

        self.logger(
            "Merge the two tables according to configured matching conditions. This could take a while."
        )
        joined_data = self.join_tables(join_query)
        self.logger("-------------", display_time=False)

        self.logger("Removing duplicated rows.")
        final_data = self.drop_ambivalent_matching_rows(joined_data)
        self.logger("-------------", display_time=False)

        self.logger("Job achieved.")
        self.logger("-------------", display_time=False)

        return final_data


def main():
    base_data = pd.read_csv(
        BASE_FILENAME, sep=";", encoding="ISO-8859-1", engine="python"
    )
    input_data = pd.read_csv(
        INPUT_FILENAME, sep=";", encoding="ISO-8859-1", engine="python"
    )
    potentials_fields_to_add = extract_potentials_fields_to_add_from_input(input_data)
    selected_fields = generate_selection_menu(
        potentials_fields_to_add, ALL_FIELDS_SELECTED_BY_DEFAULT
    )

    tables_merger = DataTablesMerger(
        base_data=base_data,
        input_data=input_data,
        fields_to_append_list=selected_fields,
        join_fields_configuration=JOIN_FIELDS_CONFIGURATION,
    )
    output = tables_merger.launch()
    output.to_csv(OUTPUT_FILENAME, sep="\t", index=False)


main()

