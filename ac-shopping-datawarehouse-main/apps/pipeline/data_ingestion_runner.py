# from ingest_data import  DataIngestionTask
from projects.data_ingestion.data_ingestion import DataIngestionTask


def execute():
    task = DataIngestionTask("DataIngestionTask")
    task.run()


if __name__ == "__main__":
    execute()

# test CICD in code pipeline
