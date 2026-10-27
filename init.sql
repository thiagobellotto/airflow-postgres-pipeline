CREATE ROLE warehouse LOGIN PASSWORD 'warehouse';
CREATE DATABASE jsonplaceholder OWNER warehouse;
\connect jsonplaceholder
SET ROLE warehouse;
CREATE SCHEMA IF NOT EXISTS api_json;

CREATE TABLE IF NOT EXISTS api_json.users (
    id integer PRIMARY KEY CHECK (id > 0),
    name text NOT NULL,
    username text NOT NULL,
    email text NOT NULL,
    phone text NOT NULL,
    website text NOT NULL,
    street text NOT NULL,
    suite text NOT NULL,
    city text NOT NULL,
    zipcode text NOT NULL,
    latitude numeric CHECK (latitude BETWEEN -90 AND 90),
    longitude numeric CHECK (longitude BETWEEN -180 AND 180),
    company_name text NOT NULL,
    company_catch_phrase text NOT NULL,
    company_bs text NOT NULL,
    content_hash text NOT NULL,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_changed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS api_json.posts (
    id integer PRIMARY KEY CHECK (id > 0),
    user_id integer NOT NULL REFERENCES api_json.users(id),
    title text NOT NULL,
    body text NOT NULL,
    content_hash text NOT NULL,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_changed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS posts_user_id_idx ON api_json.posts(user_id);

CREATE TABLE IF NOT EXISTS api_json.pipeline_runs (
    run_id text PRIMARY KEY,
    completed_at timestamptz NOT NULL DEFAULT now(),
    summary jsonb NOT NULL
);

CREATE OR REPLACE VIEW api_json.author_summary AS
SELECT u.id AS user_id, u.name, u.username, u.company_name,
       count(p.id) AS post_count,
       round(avg(length(p.body)), 1) AS average_post_length
FROM api_json.users u
LEFT JOIN api_json.posts p ON p.user_id = u.id
GROUP BY u.id, u.name, u.username, u.company_name;
