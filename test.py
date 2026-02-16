import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from utils.math import add

def main(session: snowpark.Session):
    tableName = 'information_schema.packages'
    dataframe = session.table(tableName).filter(col("language") == 'python')
    result = add(5, 10)
    print(f"The result of add(5, 10) is: {result}")
    
    dataframe.show()

    return dataframe
    