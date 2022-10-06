from prefect.deployments import Deployment
from prefect.filesystems import GitHub

from lab_103 import alphavantage

github_block = GitHub.load("prefect-pacc-repo")

deploy = Deployment.build_from_flow(
    flow=alphavantage,
    name="Alphavantage deployment from GitHub",
    storage=github_block
)

if __name__ == "__main__":
    deploy.apply()
