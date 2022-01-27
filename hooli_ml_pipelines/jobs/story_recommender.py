from dagster import fs_io_manager, graph
from dagster_aws.s3 import s3_resource
from dagstermill.io_managers import local_output_notebook_io_manager

from ..ops.comment_stories import build_comment_stories
from ..ops.recommender_model import (
    build_component_top_stories,
    build_recommender_model,
    model_perf_notebook,
)
from ..ops.user_story_matrix import build_user_story_matrix
from ..ops.user_top_recommended_stories import (
    build_user_top_recommended_stories,
)
from ..resources.fixed_s3_pickle_io_manager import fixed_s3_pickle_io_manager
from ..resources.s3_notebook_io_manager import s3_notebook_io_manager
from ..resources.snowflake_io_manager import snowflake_io_manager

import os

# Hack since the snowflake password env var is being wrapped in single quotes
os.environ["SNOWFLAKE_PASSWORD"] = os.getenv("SNOWFLAKE_PASSWORD", "").strip("'")

snowflake_manager = snowflake_io_manager.configured(
    {
        "account": {"env": "SNOWFLAKE_ACCOUNT"},
        "user": {"env": "SNOWFLAKE_USER"},
        "password": {"env": "SNOWFLAKE_PASSWORD"},
        "database": "DEMO_DB",
        "warehouse": "TINY_WAREHOUSE",
    }
)

STORY_RECOMMENDER_RESOURCES_DEV = {
    "io_manager": fs_io_manager,
    "warehouse_io_manager": fs_io_manager,
    "warehouse_loader": snowflake_manager,
    "output_notebook_io_manager": local_output_notebook_io_manager,
}

STORY_RECOMMENDER_RESOURCES_STAGING = {
    "io_manager": fixed_s3_pickle_io_manager.configured(
        {"bucket": "hackernews-elementl-dev"}
    ),
    "warehouse_io_manager": snowflake_manager,
    "warehouse_loader": snowflake_manager,
    "s3": s3_resource,
    "output_notebook_io_manager": s3_notebook_io_manager.configured(
        {"bucket": "hackernews-elementl-dev"}
    ),
}

STORY_RECOMMENDER_RESOURCES_PROD = {
    "io_manager": fixed_s3_pickle_io_manager.configured(
        {"bucket": "hackernews-elementl-prod"}
    ),
    "warehouse_io_manager": snowflake_manager,
    "warehouse_loader": snowflake_manager,
    "s3": s3_resource,
    "output_notebook_io_manager": s3_notebook_io_manager.configured(
        {"bucket": "hackernews-elementl-prod"}
    ),
}


@graph(
    description="""
    Trains a collaborative filtering model that can recommend HN stories to users based on what
    stories they've commented on in the past.
    """,
)
def story_recommender():
    comment_stories = build_comment_stories()
    user_story_matrix = build_user_story_matrix(comment_stories)
    recommender_model = build_recommender_model(user_story_matrix)
    model_perf_notebook(recommender_model)
    build_component_top_stories(recommender_model, user_story_matrix)
    build_user_top_recommended_stories(recommender_model, user_story_matrix)


story_recommender_prod_job = story_recommender.to_job(
    resource_defs=STORY_RECOMMENDER_RESOURCES_PROD
)

story_recommender_staging_job = story_recommender.to_job(
    resource_defs=STORY_RECOMMENDER_RESOURCES_STAGING
)

story_recommender_dev_job = story_recommender.to_job(
    resource_defs=STORY_RECOMMENDER_RESOURCES_DEV
)
