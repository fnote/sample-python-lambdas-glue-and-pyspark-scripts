"""
validator to validate columns in incoming files
"""
# pylint: disable=line-too-long

from pyspark.sql.functions import isnan, length


def validate_opcos(df,active_opco_list,column):
    invalid_df = df.filter(~df[column].isin(active_opco_list) | df[column].isNull())
    if len(invalid_df.head(1)) > 0:
        invalid_df.show(truncate=False)
        print("Invalid or inactive opco records found in the file: refer dataframe above ")
    return get_opco_list(invalid_df)


def validate_column(df, column):
    invalid_df = df.filter((df[column] == "") | df[column].isNull() | (df[column].rlike('[^0-9]')) | isnan(df[column]))
    if len(invalid_df.head(1)) > 0:
        invalid_df.show(truncate=False)
        print("Data can not be null/empty/non-numeric of column: " + column)
    return get_opco_list(invalid_df)


def get_opco_list(df):
    """
        get opco list
    """
    return [row.opco_id for row in df.select('opco_id').distinct().collect()]


def remove_records_of_given_opcos(df, failed_opco_list):
    """
        remove records of error opco
    """
    return df.filter(~df.opco_id.isin(failed_opco_list))


def validate_column_length_less_than(df, column, col_length):
    """
        validate column length
    """
    invalid_df = df.filter((length(df[column]) > col_length))
    if len(invalid_df.head(1)) > 0:
        invalid_df.show(truncate=False)
        print("Data length can not exceed length: " + str(col_length) + " of column: " + column)
    return get_opco_list(invalid_df)


def validate_column_length_equals(df, column, col_length):
    """
        validate column length
    """
    invalid_df = df.filter((length(df[column]) != col_length))
    if len(invalid_df.head(1)) > 0:
        invalid_df.show(truncate=False)
        raise ValueError("Data length should be equal to : " + str(col_length) + " of column: " + column)


def validate_data_range(df, column, min_val, max_val):
    """
        validate data range
    """
    invalid_df = df.filter((df[column] < min_val) | (df[column] > max_val))
    if len(invalid_df.head(1)) > 0:
        invalid_df.show(truncate=False)
        print("Invalid data-range received for column: "
                         + column + ".should be between " + str(min_val)
                         + "and " + str(max_val))
    return get_opco_list(invalid_df)


def validate_date_format(df, column, input_date_regex, input_date_format):
    """
        validate date format
    """
    invalid_df = df.filter(df[column].isNull() | ~(df[column].rlike(input_date_regex)))
    if len(invalid_df.head(1)) > 0:
        invalid_df.show(truncate=False)
        print("Invalid date format for column: " + column
              + ".Accept only format " + input_date_format
              + 'matching date time regex: ' + input_date_regex)
    return get_opco_list(invalid_df)


def validate_date_time_field(df, output_column):
    """
        validate_date_time_field
    """
    invalid_df = df.filter(df[output_column].isNull())
    if len(invalid_df.head(1)) > 0:
        invalid_df.show(truncate=False)
        print("Received invalid date value for column: " + output_column)
    return get_opco_list(invalid_df)
