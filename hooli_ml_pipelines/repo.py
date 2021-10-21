from dagster import repository

from .jobs import story_recommender_prod_job
from .sensors import hn_tables_updated_sensor


@repository
def repo():
    return [story_recommender_prod_job, hn_tables_updated_sensor]
