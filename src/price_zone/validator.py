from pyspark.sql.functions import isnan, length, to_timestamp

def validate_opcos(df,active_opco_list):
    column = "opco_id"
    invalidDF = df.filter(~df[column].isin(active_opco_list) | df[column].isNull())
    if len(invalidDF.head(1)) > 0:
        invalidDF.show(truncate=False)
        raise ValueError("Invalid or inactive opco records found in the file: refer dataframe above ")

def validate_column(df, column):
    invalidDF = df.filter((df[column] == "") | df[column].isNull() | (df[column].rlike('[^0-9]')) | isnan(df[column]))
    if len(invalidDF.head(1)) > 0:
        invalidDF.show(truncate=False)
        raise ValueError("Data can not be null/empty/non-numeric of column: " + column)


def validate_column_length_less_than(df, column, col_length):
    invalidDF = df.filter((length(df[column]) > col_length))
    if len(invalidDF.head(1)) > 0:
        invalidDF.show(truncate=False)
        raise ValueError("Data length can not exceed length: " + str(col_length) + " of column: " + column)


def validate_column_length_equals(df, column, col_length):
    invalidDF = df.filter((length(df[column]) != col_length))
    if len(invalidDF.head(1)) > 0:
        invalidDF.show(truncate=False)
        raise ValueError("Data length should be equal to : " + str(col_length) + " of column: " + column)


def validate_data_range(df, column, min_val, max_val):
    invalidDF = df.filter((df[column] < min_val) | (df[column] > max_val))
    if len(invalidDF.head(1)) > 0:
        invalidDF.show(truncate=False)
        raise ValueError("Invalid data-range received for column: "
                         + column + ".should be between " + str(min_val)
                         + "and " + str(max_val))


def validate_date_format(df, column, input_date_regex, input_date_format):
    invalidDF = df.filter(df[column].isNull() | ~(df[column].rlike(input_date_regex)))
    if len(invalidDF.head(1)) > 0:
        invalidDF.show(truncate=False)
        raise ValueError("Invalid date format for column: " + column
                         + ".Accept only format " + input_date_format
                         + 'matching date time regex: ' + input_date_regex)


def validate_and_get_as_date_time(df, input_column, output_column, output_format):
    df = df.withColumn(output_column, to_timestamp(df[input_column], output_format))
    invalidDF = df.filter(df[output_column].isNull())
    if len(invalidDF.head(1)) > 0:
        invalidDF.show(truncate=False)
        raise ValueError("Received invalid date value for column: " + input_column)
    return df
