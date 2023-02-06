from pyspark.sql.functions import col, explode_outer

    
def rename_df_cols(df, col_names):
    return df.select(*[col(col_name).alias(col_names.get(col_name, col_name)) for col_name in df.columns])

def update_column_names(df, index):
    df_temp = df
    all_cols = df_temp.columns
    new_cols = dict((column, f"{column}*{index}") for column in all_cols)
    df_temp = df_temp.transform(lambda df_x: rename_df_cols(df_x, new_cols))
    return df_temp

def flatten(df_arg, index=1):
    df = update_column_names(df_arg, index) if index == 1 else df_arg
    fields = df.schema.fields

    for field in fields:
        data_type = str(field.dataType)
        column_name = field.name

        first_10_chars = data_type[0:10]

        if first_10_chars == 'ArrayType(':
            df_temp = df.withColumn(column_name, explode_outer(col(column_name)))
            return flatten(df_temp, index + 1)

        elif first_10_chars == 'StructType':
            current_col = column_name
            append_str = current_col
            data_type_str = str(df.schema[current_col].dataType)

            df_temp = df.withColumnRenamed(column_name, column_name + "#1") \
                if column_name in data_type_str else df
            current_col = current_col + "#1" if column_name in data_type_str else current_col

            df_before_expanding = df_temp.select(f"{current_col}.*")
            newly_gen_cols = df_before_expanding.columns

            begin_index = append_str.rfind('*')
            end_index = len(append_str)
            level = append_str[begin_index + 1: end_index]
            next_level = int(level) + 1

            custom_cols = dict((field, f'{append_str}->{field}*{next_level}') for field in newly_gen_cols)
            df_temp2 = df_temp.select("*", f'{current_col}.*').drop(current_col)
            df_temp3 = df_temp2.transform(lambda df_x: rename_df_cols(df_x, custom_cols))
            return flatten(df_temp3, index + 1)

    return df