import hashlib
import json
import logging
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.request import Request, urlopen

LOGGER = logging.getLogger(__name__)
API_URL = 'https://jsonplaceholder.typicode.com'
USER_COLUMNS = ['id', 'name', 'username', 'email', 'phone', 'website', 'street', 'suite',
                'city', 'zipcode', 'latitude', 'longitude', 'company_name', 'company_catch_phrase', 'company_bs']
POST_COLUMNS = ['id', 'user_id', 'title', 'body']


def write_json(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True), encoding='utf-8')
    temporary.replace(path)


def run_directory(run_id):
    path = Path(os.environ.get('ETL_DATA_DIR', '/opt/airflow/data')) / hashlib.sha256(run_id.encode()).hexdigest()[:20]
    path.mkdir(parents=True, exist_ok=True)
    return path


def extract(resource, run_id):
    if resource not in {'users', 'posts'}:
        raise ValueError(f'Unsupported resource: {resource}')
    path = run_directory(run_id) / f'{resource}.json'
    if path.exists():
        LOGGER.info('Reusing source snapshot: %s', path)
    else:
        request = Request(f'{API_URL}/{resource}', headers={'User-Agent': 'jsonplaceholder-airflow-demo/1.0'})
        with urlopen(request, timeout=30) as response:
            payload = json.load(response)
        if not isinstance(payload, list) or not payload:
            raise ValueError(f'{resource}: expected a non-empty JSON array')
        write_json(path, payload)
        LOGGER.info('Downloaded %s: %s records', resource, len(payload))
    return str(path)


def identifier(value, field):
    if type(value) is not int or value <= 0:
        raise ValueError(f'{field}: expected a positive integer')
    return value


def text(value, field):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{field}: expected a non-empty string')
    return value.strip()


def coordinate(value, field, maximum):
    try:
        number = Decimal(str(value))
        if not number.is_finite() or not -maximum <= number <= maximum:
            raise ValueError(f'{field}: coordinate outside valid bounds')
    except InvalidOperation as error:
        raise ValueError(f'{field}: invalid coordinate') from error
    return str(number)


def row_hash(row):
    return hashlib.sha256(json.dumps(row, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def transform(users_path, posts_path, run_id):
    raw_users = json.loads(Path(users_path).read_text())
    raw_posts = json.loads(Path(posts_path).read_text())
    users, posts = [], []
    for raw in raw_users:
        user = {'id': identifier(raw['id'], 'users.id')}
        for field in ['name', 'username', 'email', 'phone', 'website']:
            user[field] = text(raw[field], f'users.{field}')
        for field in ['street', 'suite', 'city', 'zipcode']:
            user[field] = text(raw['address'][field], f'users.address.{field}')
        user['latitude'] = coordinate(raw['address']['geo']['lat'], 'users.latitude', 90)
        user['longitude'] = coordinate(raw['address']['geo']['lng'], 'users.longitude', 180)
        for field, source in [('company_name', 'name'), ('company_catch_phrase', 'catchPhrase'), ('company_bs', 'bs')]:
            user[field] = text(raw['company'][source], f'users.company.{source}')
        user['content_hash'] = row_hash(user)
        users.append(user)
    user_ids = {u['id'] for u in users}
    if not users or len(user_ids) != len(users):
        raise ValueError('users: empty dataset or duplicate IDs')
    for raw in raw_posts:
        post = {'id': identifier(raw['id'], 'posts.id'), 'user_id': identifier(raw['userId'], 'posts.userId'),
                'title': text(raw['title'], 'posts.title'), 'body': text(raw['body'], 'posts.body')}
        if post['user_id'] not in user_ids:
            raise ValueError(f"posts.{post['id']}: user {post['user_id']} is missing from this source snapshot")
        post['content_hash'] = row_hash(post)
        posts.append(post)
    if not posts or len({p['id'] for p in posts}) != len(posts):
        raise ValueError('posts: empty dataset or duplicate IDs')
    path = run_directory(run_id) / 'prepared.json'
    write_json(path, {'users': users, 'posts': posts})
    LOGGER.info('Validated %s users, %s posts, and all user/post relationships', len(users), len(posts))
    return str(path)


def load(prepared_path, run_id):
    import psycopg2
    from psycopg2.extras import Json, execute_values
    from psycopg2 import sql

    prepared = json.loads(Path(prepared_path).read_text())
    summary = {}
    connection = psycopg2.connect(os.environ['WAREHOUSE_DSN'], connect_timeout=10)
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('jsonplaceholder_load'))")
                cursor.execute('SELECT summary FROM api_json.pipeline_runs WHERE run_id = %s', (run_id,))
                previous = cursor.fetchone()
                if previous:
                    LOGGER.info('Run %s is already committed; returning its summary', run_id)
                    return previous[0]
                for table, columns in [('users', USER_COLUMNS), ('posts', POST_COLUMNS)]:
                    stage = f'staged_{table}'
                    cursor.execute(sql.SQL('CREATE TEMP TABLE {} (LIKE api_json.{} INCLUDING DEFAULTS) ON COMMIT DROP').format(sql.Identifier(stage), sql.Identifier(table)))
                    fields = columns + ['content_hash']
                    values = [tuple(row[field] for field in fields) for row in prepared[table]]
                    execute_values(cursor, sql.SQL('INSERT INTO {} ({}) VALUES %s').format(sql.Identifier(stage), sql.SQL(', ').join(map(sql.Identifier, fields))).as_string(cursor), values)
                    cursor.execute(sql.SQL('''SELECT count(*) FILTER (WHERE t.id IS NULL),
                        count(*) FILTER (WHERE t.id IS NOT NULL AND t.content_hash <> s.content_hash),
                        count(*) FILTER (WHERE t.content_hash = s.content_hash)
                        FROM {} s LEFT JOIN api_json.{} t ON t.id = s.id''').format(sql.Identifier(stage), sql.Identifier(table)))
                    inserted, updated, unchanged = cursor.fetchone()
                    summary[table] = dict(source=len(values), inserted=inserted, updated=updated, unchanged=unchanged)
                    assignments = [sql.SQL('{} = EXCLUDED.{}').format(sql.Identifier(c), sql.Identifier(c)) for c in fields if c != 'id']
                    assignments.append(sql.SQL('last_changed_at = now()'))
                    cursor.execute(sql.SQL('''INSERT INTO api_json.{} AS target ({}) SELECT {} FROM {} WHERE true
                        ON CONFLICT (id) DO UPDATE SET {} WHERE target.content_hash <> EXCLUDED.content_hash''').format(
                            sql.Identifier(table), sql.SQL(', ').join(map(sql.Identifier, fields)),
                            sql.SQL(', ').join(map(sql.Identifier, fields)), sql.Identifier(stage), sql.SQL(', ').join(assignments)))
                cursor.execute('INSERT INTO api_json.pipeline_runs(run_id, summary) VALUES (%s, %s)', (run_id, Json(summary)))
        LOGGER.info('Committed run %s: %s', run_id, json.dumps(summary, sort_keys=True))
        return summary
    finally:
        connection.close()


def summarize(summary):
    LOGGER.info('PIPELINE RESULT\n%s', json.dumps(summary, indent=2, sort_keys=True))
    return summary
