import duckdb
connection = duckdb.connect(":memory:")

outputParquetFile = 'D:/VS Code/FullStackDashboardsProject/AgroWebFull/AgroWebEtl/data/transformed/3.finalStage/CropsData.parquet'

connection.execute(
                   f"""
                   CREATE TABLE cropsData AS SELECT * FROM read_parquet('data/transformed/1.Stage1/CropsData.parquet')
                   """)

query = f"""SELECT * FROM cropsData cd """

connection.execute(
    f"""
    COPY ({query}) TO '{outputParquetFile}' (FORMAT PARQUET)
    """
)

print("done reading parquet 🤩")