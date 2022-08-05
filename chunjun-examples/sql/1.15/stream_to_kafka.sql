CREATE TABLE source
(
    id        INT,
    name      STRING
) WITH (
      'connector' = 'stream-x',
      'number-of-rows' = '1000', -- 输入条数，默认无限
      'rows-per-second' = '1' -- 每秒输入条数，默认不限制
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

insert into sink
select *
from source;
