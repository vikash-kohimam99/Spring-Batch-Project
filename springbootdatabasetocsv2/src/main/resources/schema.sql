DROP TABLE IF EXISTS person2;

CREATE TABLE person2  (
    person_id BIGINT  NOT NULL PRIMARY KEY,
    first_name VARCHAR(40),
    last_name VARCHAR(40),
    email VARCHAR(100),
    age INT
);