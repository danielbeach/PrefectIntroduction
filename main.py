from prefect import task, Flow
from prefect.executors import DaskExecutor
import csv


@task
def stream_file(file_uri: str) -> list:
    with open(file_uri) as f:
        rdr = csv.reader(f, delimiter=',')
        rows = [row for row in rdr]
    return rows

@task
def process_stream(trip: str):
    print(trip)


with Flow("CSV Flow") as flow:
    trips = stream_file('data/202203-divvy-tripdata.csv')
    process_stream.map(trips)
executor = DaskExecutor(address="tcp://localhost:8786")
flow.run(executor=executor)