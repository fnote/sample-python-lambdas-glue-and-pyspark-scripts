from pyspark.sql.functions import isnan, length, to_timestamp, to_date


def validate_column(df, column):
    invalidDF = df.filter((df[column] == "") | df[column].isNull() | (df[column].rlike('[^0-9]')) | isnan(df[column]))
    if len(invalidDF.head(1)) > 0:
        raise ValueError("Received invalid data for column: " + column)


def validate_column_length(df, column, col_length):
    invalidDF = df.filter((length(df[column]) > col_length))
    if len(invalidDF.head(1)) > 0:
        raise ValueError("Received invalid data length for column: " + column)


def validate_data_range(df, column, min_val, max_val):
    invalidDF = df.filter((df[column] < min_val) | (df[column] > max_val))
    if len(invalidDF.head(1)) > 0:
        raise ValueError("Invalid data length for column: " + column)


def validate_date_format(df, column, format):
    invalidDF = df.filter(df[column].isNull() | ~(df[column].rlike(format)))
    if len(invalidDF.head(1)) > 0:
        raise ValueError("Invalid date format for column: " + column)


def validate_and_get_as_date(df, input_column, output_column, format):
    df = df.withColumn(output_column, to_date(df[input_column], format))
    invalidDF = df.filter(df[output_column].isNull())
    if len(invalidDF.head(1)) > 0:
        raise ValueError("Received invalid date value for column: " + input_column)

    return df