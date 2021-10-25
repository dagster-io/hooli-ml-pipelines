import os

from dagster import AssetKey, RunRequest, SkipReason, sensor
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

from ..jobs import story_recommender_prod_job

DAGIT_URL = "https://hooli.dagster.cloud/dataengprod/graphql"
HEADERS = {"Dagster-Cloud-Api-Token": os.getenv("CROSS_DEPLOYMENT_API_TOKEN")}
TRANSPORT = RequestsHTTPTransport(url=DAGIT_URL, headers=HEADERS, use_json=True)

gql_client = Client(transport=TRANSPORT)

QUERY = gql(
    """
    query AssetQuery($assetKey: AssetKeyInput!, $afterTimestamp: String!) {
        assetOrError(assetKey: $assetKey) {
            ... on Asset {
                assetMaterializations(afterTimestampMillis: $afterTimestamp) {
                    materializationEvent {
                        runId
                        timestamp
                    }
                }
            }
        }
    }
    """
)


@sensor(job=story_recommender_prod_job)
def hn_tables_updated_sensor(context):
    try:
        last_mtime = int(context.cursor) if context.cursor else 0
    except ValueError:
        last_mtime = 0

    max_mtime = last_mtime

    materializations = gql_client.execute(
        QUERY,
        variable_values={
            "assetKey": {"path": ["hacker_news_tables"]},
            "afterTimestamp": str(last_mtime),
        },
    )["assetOrError"].get("assetMaterializations", [])

    if not materializations:
        yield SkipReason(f"No materializations found since last run ({last_mtime})")
        return

    yield RunRequest(run_key=None)

    for materialization in materializations:
        max_mtime = max(
            max_mtime, int(materialization["materializationEvent"]["timestamp"])
        )

    context.update_cursor(str(max_mtime + 1))


hn_tables_updated_sensor.asset_key = AssetKey("hacker_news_tables")
