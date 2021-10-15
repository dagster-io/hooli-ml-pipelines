from dagster import repository

from .jobs import story_recommender_prod_job


@repository
def repo():
    return [story_recommender_prod_job]
