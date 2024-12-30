import duckdb
connection = duckdb.connect(":memory:")

csvFiles = 'data/raw/crop_yield.csv'
outputParquetFile = 'D:/VS Code/FullStackDashboardsProject/AgroWebFull/AgroWebEtl/data/transformed/1.Stage1/CropsData.parquet'

connection.execute(
    f"""
    CREATE TABLE CropsData AS SELECT * FROM read_csv_auto('{csvFiles}')
    """
)

query = f""" SELECT * FROM CropsData"""

connection.execute(
    f""" 
     COPY ({query}) TO '{outputParquetFile}'
     (FORMAT PARQUET)
     """
)
print("done conversion 🎀🎉")