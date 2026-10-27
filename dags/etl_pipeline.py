from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task


@dag(
    dag_id='jsonplaceholder_etl',
    dag_display_name='JSONPlaceholder · Reliable ETL',
    description='API snapshots → validated records → atomic PostgreSQL upserts',
    schedule='@daily',
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={'retries': 2, 'retry_delay': timedelta(seconds=15), 'execution_timeout': timedelta(minutes=3)},
    tags=['API', 'PostgreSQL', 'retry-safe'],
)
def jsonplaceholder_etl():
    @task
    def extract_users(batch_id):
        from etl.pipeline import extract
        return extract('users', batch_id)

    @task
    def extract_posts(batch_id):
        from etl.pipeline import extract
        return extract('posts', batch_id)

    @task
    def validate_and_transform(users_path, posts_path, batch_id):
        from etl.pipeline import transform
        return transform(users_path, posts_path, batch_id)

    @task
    def publish_to_postgres(prepared_path, batch_id):
        from etl.pipeline import load
        return load(prepared_path, batch_id)

    @task
    def run_summary(summary):
        from etl.pipeline import summarize
        return summarize(summary)

    batch_id = '{{ run_id }}'
    users = extract_users(batch_id)
    posts = extract_posts(batch_id)
    prepared = validate_and_transform(users, posts, batch_id)
    run_summary(publish_to_postgres(prepared, batch_id))


jsonplaceholder_etl()
