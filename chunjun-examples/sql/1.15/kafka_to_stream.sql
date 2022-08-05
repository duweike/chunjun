CREATE TABLE source
(
    id        INT,
    name      STRING
) WITH (
      'connector' = 'stream-x',
      'print' = 'true'
      );

CREATE TABLE sink
(
    id          int,
    name        varchar
) WITH (
      'connector' = 'kafka-x'
      ,'topic' = 'duweikeForTest'
      ,'properties.bootstrap.servers' = 'localhost:9092'
      ,'properties.group.id' = 'duweike'
      ,'scan.startup.mode' = 'earliest-offset'
      ,'format' = 'json'
      );

insert into source
select *
from sink;
