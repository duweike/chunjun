CREATE TABLE source
(
    id        INT,
    name      STRING
) WITH (
      'connector' = 'stream-x',
      'number-of-rows' = '10', -- 输入条数，默认无限
      'rows-per-second' = '1' -- 每秒输入条数，默认不限制
      );

CREATE TABLE sink
(
    id          int,
    name        varchar
) WITH (
      'connector' = 'mysql-x',
      'url' = 'jdbc:mysql://localhost:3306/mysql',
      'table-name' = 'test',
      'username' = 'root',
      'password' = '123456',

      'sink.buffer-flush.max-rows' = '1024', -- 批量写数据条数，默认：1024
      'sink.buffer-flush.interval' = '10000', -- 批量写时间间隔，默认：10000毫秒
      'sink.all-replace' = 'true', -- 解释如下(其他rdb数据库类似)：默认：false。定义了PRIMARY KEY才有效，否则是追加语句
                                  -- sink.all-replace = 'true' 生成如：INSERT INTO `result3`(`mid`, `mbb`, `sid`, `sbb`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `mid`=VALUES(`mid`), `mbb`=VALUES(`mbb`), `sid`=VALUES(`sid`), `sbb`=VALUES(`sbb`) 。会将所有的数据都替换。
                                  -- sink.all-replace = 'false' 生成如：INSERT INTO `result3`(`mid`, `mbb`, `sid`, `sbb`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE `mid`=IFNULL(VALUES(`mid`),`mid`), `mbb`=IFNULL(VALUES(`mbb`),`mbb`), `sid`=IFNULL(VALUES(`sid`),`sid`), `sbb`=IFNULL(VALUES(`sbb`),`sbb`) 。如果新值为null，数据库中的旧值不为null，则不会覆盖。
      'sink.parallelism' = '1'    -- 写入结果的并行度，默认：null
      );

insert into sink
select *
from source;
