from datetime import timedelta

from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import IntervalSchedule

from lab_103 import alphavantage

deploy = Deployment.build_from_flow(
    flow=alphavantage,
    name="Alphavantage deployment from file",
    schedule=IntervalSchedule(interval=timedelta(minutes=1)),
)

if __name__ == "__main__":
    deploy.apply()
