from pyspark.sql.functions import isnan, length

def validate_column(df, column):
    invalidDF = df.filter((df[column] == "") | df[column].isNull() | isnan(df[column]))
    if len(invalidDF.head(1)) > 0:
        raise ValueError("Received invalid data for column " + column)

def validate_column_length(df, column, col_length):
    invalidDF = df.filter((length(df[column]) > col_length))
    if len(invalidDF.head(1)) > 0:
        raise ValueError("Received invalid data length for column " + column)

def validate_mandatory_columns(df, mandatory_columns):
    columns = df.columns
    not_found_list = []
    for column in mandatory_columns:
        if column not in columns:
            not_found_list.append(column)
    if len(not_found_list) > 0:
        raise Exception(",".join(not_found_list) + " mandatory columns not found")


def validate_data_range(df, column, min_val, max_val):
    invalidDF = df.filter((df[column] < min_val) | (df[column] > max_val))
    if len(invalidDF.head(1)) > 0:
        raise ValueError("Invalid data length for column " + column)