CREATE TABLE IF NOT EXISTS workflow
(
    id         SERIAL PRIMARY KEY,
    name       TEXT,
    params     TEXT,
    status     INTEGER,
    type       TEXT,
    start_time BIGINT,
    end_time   BIGINT
);


CREATE TABLE IF NOT EXISTS step
(
    id       SERIAL PRIMARY KEY,
    name     TEXT,
    workflow INTEGER,
    status   INTEGER,
    type     TEXT,
    params   TEXT,
    FOREIGN KEY (workflow) REFERENCES workflow (id)
);


CREATE TABLE IF NOT EXISTS port
(
    id       SERIAL PRIMARY KEY,
    name     TEXT,
    workflow INTEGER,
    type     TEXT,
    params   TEXT,
    FOREIGN KEY (workflow) REFERENCES workflow (id)
);


CREATE TABLE IF NOT EXISTS dependency
(
    step INTEGER,
    port INTEGER,
    type INTEGER,
    name TEXT,
    PRIMARY KEY (step, port, type, name),
    FOREIGN KEY (step) REFERENCES step (id),
    FOREIGN KEY (port) REFERENCES port (id)
);


CREATE TABLE IF NOT EXISTS execution
(
    id         SERIAL PRIMARY KEY,
    step       INTEGER,
    job_token  INTEGER,
    cmd        TEXT,
    status     INTEGER,
    start_time BIGINT,
    end_time   BIGINT,
    FOREIGN KEY (step) REFERENCES step (id)
);


CREATE TABLE IF NOT EXISTS token
(
    id    SERIAL PRIMARY KEY,
    port  INTEGER,
    tag   TEXT,
    type  TEXT,
    value BYTEA,
    FOREIGN KEY (port) REFERENCES port (id)
);


CREATE TABLE IF NOT EXISTS recoverable
(
    id    SERIAL PRIMARY KEY,
    FOREIGN KEY (id) REFERENCES token (id)
);


CREATE TABLE IF NOT EXISTS provenance
(
    dependee INTEGER,
    depender INTEGER,
    CONSTRAINT primary_key UNIQUE (dependee, depender),
    FOREIGN KEY (dependee) REFERENCES token (id),
    FOREIGN KEY (depender) REFERENCES token (id)
);


CREATE TABLE IF NOT EXISTS deployment
(
    id                  SERIAL PRIMARY KEY,
    name                TEXT,
    type                TEXT,
    config              TEXT,
    external            BOOLEAN,
    lazy                BOOLEAN,
    scheduling_policy   TEXT,
    workdir             TEXT,
    wraps               TEXT
);


CREATE TABLE IF NOT EXISTS target
(
    id         SERIAL PRIMARY KEY,
    deployment INTEGER,
    type       TEXT,
    locations  INTEGER,
    service    TEXT,
    workdir    TEXT,
    params     TEXT,
    FOREIGN KEY (deployment) REFERENCES deployment (id)
);


CREATE TABLE IF NOT EXISTS filter
(
    id          SERIAL PRIMARY KEY,
    name        TEXT,
    type        TEXT,
    config      TEXT
);