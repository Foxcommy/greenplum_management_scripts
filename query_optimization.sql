/* EXPLAIN */
-- Демо --
create table tx1 (id int, descr text) with (appendonly=true, orientation=row) distributed by (id);
create table tx2 (id int, descr text) with (appendonly=true, orientation=row) distributed by (id);

explain select * from table1 JOIN table2 using (id1);

-- Демо 1 --
-- Создаём таблицы и умышленно для второй таблицы задаем плохой ключ распределения
drop table if exists lab6;
create table lab6 (id int, descr text) distributed by (id);

drop table if exists lab7;
create table lab7 (id int, descr text) distributed by (descr);

-- Вставляем строки в таблицы
insert into lab6 SELECT generate_series(1,20) AS id, md5(random()::text) AS descr;
insert into lab7 SELECT generate_series(1,2000000) AS id, md5(random()::text) AS descr;

-- Проводим анализ
analyze lab6;
analyze lab7;

SELECT gp_segment_id, count(*) FROM lab6 GROUP BY 1;
SELECT gp_segment_id, count(*) FROM lab7 GROUP BY 1;

-- Выполняем джойн и смотрим план
explain select * from lab6 JOIN lab7 using (id);

-- Выполняем джойн и смотрим план
explain analyze select * from lab6 JOIN lab7 using (id);

-- GUCs
set optimizer=off; -- Заставляет систему всегда использовать оптимизатор Postgres
--set gp_resgroup_print_operator_memory_limits = on;  -- отображает память, выделенную каждому оператору
--set explain_memory_verbosity = DETAIL; -- очень подробный план по выполнению запроса  

-- Выполняем джойн и смотрим план c Постгрес Оптимайзером
explain analyze select * from lab6 JOIN lab7 using (id);


--==================================================================================================
-- SKEW --
-- Создаём таблицы и умышленно для второй таблицы задаем плохой ключ распределения
drop table if exists lab6;
create table lab6 (id int, descr text) distributed by (id);

drop table if exists lab7;
create table lab7 (id int, descr text) distributed by (descr);

-- Вставляем строки в таблицы
insert into lab6 SELECT generate_series(1,20) AS id, md5(random()::text) AS descr;
insert into lab7 SELECT generate_series(1,2000000) AS id, md5(random()::text) AS descr;

-- Смотрим распределение строк по сегментам
select gp_segment_id, count(*) from lab6 group by 1;
select gp_segment_id, count(*) from lab7 group by 1;

-- Выполняем джойн и смотрим, что для второй таблицы кол-во ожидаемых строк на один сегмент (
-- значительно меньше, чем актуальных
explain analyze select * from lab6 JOIN lab7 using (id);

-- Меняем ключ распределения для второй таблицы по полю id
ALTER TABLE lab7 SET DISTRIBUTED BY (id);

-- Выполняем запрос и видим, что теперь кол-во ожидаемых строк = кол-ву актуальных по таблице lab7

--/* SPILL-файлы  */
-- Демо
drop table if exists lab1;
CREATE TABLE lab1 (id int, descr text) WITH (appendonly=true) DISTRIBUTED BY (id);
INSERT INTO lab1 SELECT generate_series(1,2000000) AS id, md5(random()::text) AS descr; -- вставить два раза

explain analyze select id, row_number() over (order by id desc) from lab1 t1 join lab1 t2 using(id);

INSERT INTO lab1 SELECT generate_series(1,2000000) AS id, md5(random()::text) AS descr; -- вставить два раза

explain analyze select id, row_number() over (order by id desc) from lab1 t1 join lab1 t2 using(id);

ANALYZE lab1;

-- ограничиваем запрос по памяти на время сессии, чтобы он писал в спилы 
set statement_mem = 1000;

explain analyze select id, row_number() over (order by id desc) from lab1 t1 join lab1 t2 using(id);

--======================================================================================================
--ПАРТИЦИОНИРОВАНИЕ
--======================================================================================================

/* Partitioning */

--1. Создаем таблицу:
create table lab8 (id int, descr text) WITH (appendonly=true) distributed by (id);

--2. Вставляем данные:
INSERT INTO lab8 SELECT generate_series(1,100) AS id, md5(random()::text) AS descr;

--4. Создаем партиционированную таблицу:
create table lab8p (like lab8) WITH (appendonly=true)
DISTRIBUTED BY (id, descr)
PARTITION BY RANGE (id)
( START (1) END (101) EVERY (20),
DEFAULT PARTITION extra_id ); 

--5. Добавляем даннные из обычной в партиционированную:
INSERT INTO lab8p SELECT * from lab8;

--6. Сравниваем планы запросов, смотрим на partition elimination:

EXPLAIN ANALYZE SELECT * FROM lab8 WHERE id = 15;

EXPLAIN ANALYZE SELECT * FROM lab8p WHERE id = 15;

EXPLAIN ANALYZE SELECT * FROM lab8p WHERE id < 30;

--7. Смотрим, когда partition elimination не срабатывает:
EXPLAIN ANALYZE SELECT * FROM lab8p WHERE id::text = '15';

-- Разрезать партицию
--8. Показываем партиции:
select * from pg_catalog.pg_partitions where tablename = 'lab8p';

--9. Смотрим информацию по партиции одной (CHECK CONSTANT)
\d+ lab8p_1_prt_2

--10. Разрезаем партицию:
ALTER TABLE lab8p SPLIT PARTITION FOR (RANK(1))
AT ('10')
INTO (PARTITION part_2_1, PARTITION part_2_2);

--11. Смотрим, что констрейнты применились:
\d+ lab8p_1_prt_part_2_1
\d+ lab8p_1_prt_part_2_2

-- Добавляем партицию:
ALTER TABLE lab8p ADD PARTITION
START (101) INCLUSIVE
END (121) EXCLUSIVE;

--Разрезаем дефолтную партицию:
ALTER TABLE lab8p SPLIT DEFAULT PARTITION 
START (101) INCLUSIVE
END (121) EXCLUSIVE
INTO (PARTITION part_30, default partition);

-- Exchange partitions ------
-- Проверяем наличие (отсутствие) данных в новой партиции)
select * from lab8p_1_prt_part_30;

--Создаем стейджинговую таблицу, которую подставим вместо партиции(без наследования типа хранения):
create table lab8_stg (like lab8p);

--Вставляем в нее данные
INSERT INTO lab8_stg SELECT generate_series(101,120) AS id, md5(random()::text) AS descr;

--Номер партиции (ранк):
select partitionrank from pg_catalog.pg_partitions where partitiontablename = 'lab8p_1_prt_part_30';

--Заменим партицию на стейджинговую таблицу:
alter table lab8p exchange partition for (RANK(7)) with table lab8_stg2 with validation;

--Можно другим способом (по имени):
alter table lab8p exchange partition part_30 with table lab8_stg with validation;

--Проверим данные:
select * from lab8p_1_prt_part_30;
select * from lab8_stg;

---------------------------------------------------------------------------------------------------------------------
--- Лабораторная работа №5 (Partitioning)
drop table if exists table3;
--1
create table table3 (dttm timestamp, id int)
partition by range (dttm) (
start (date '2016-01-01') inclusive
end (date '2019-01-01') exclusive
every (interval '1 year'),
default partition default_p);


drop table if exists tmp1;

create table tmp1 (like table3) with (appendonly =true, orientation=column, compresstype=zstd , compresslevel=5);
alter table table3 exchange partition for (date '2016-01-01') with table tmp1 with validation;

drop table if exists tmp1;

create table tmp1 (like table3) with (appendonly =true, orientation=column, compresstype=zstd , compresslevel=1);
alter table table3 exchange partition for (date '2017-01-01') with table tmp1 with validation;

drop table if exists tmp1;

alter table table3 split default partition
start ('2015-01-01') inclusive
end ('2016-01-01') exclusive
into (partition year_2015, default partition);

drop table if exists tmp1;
create table tmp1 (like table3) with (appendonly =true, compresstype=zstd , compresslevel=19);
alter table table3 exchange partition for (date '2015-01-01') with table tmp1 with validation;

alter table table3 rename partition for (date '2015-01-01') to old_one;

select * from pg_catalog.pg_partitions where tablename = 'table3';

/* Index */
1. Создаём 2 таблицы. Одна без индекса, другая с Bitmap индексом:
create table lab10 (id int, descr text)
WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=1) 
distributed by (id);

другая с Bitmap индексом:
create table lab10i (id int, descr text)
WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=1)  
distributed by (id);

2. Создаём индекс:
CREATE INDEX lab10i_idx ON lab10i USING bitmap (descr);

3. Включаем timing для замера времени выполнения запроса:
\timing

4. Выполняем загрузку данных в таблицу без индекса и с индексом. Сравниваем результаты:
insert into lab10 SELECT generate_series(1,3000000) AS id, 'test index' AS descr;
insert into lab10i SELECT generate_series(1,3000000) AS id, 'test index' AS descr;

5. Выполняем обновление данных в таблицах с индексом и без. Сравниваем результаты:
update lab10 set descr = 'update index' where descr = 'test index';
update lab10i set descr = 'update index' where descr = 'test index';

6. Выполняем обновление данных с предварительным удалением индекса и с последующим его созданием:
drop index lab10i_idx;
update lab10i set descr = 'update index 2' where descr = 'update index';
CREATE INDEX lab10i_idx ON lab10i USING bitmap (descr);


7. Проверим использование индекса:
EXPLAIN ANALYZE SELECT * FROM lab10i WHERE descr = 'update index 2';

--- Лабораторная работа №6 (INDEXES)
create table table4 (
id1 int,
id2 int,
gen1 text,
gen2 text) with (appendonly =true, orientation=column, compresstype=zstd , compresslevel=1);

insert into table4 select gen, gen, gen::text || 'text1', gen::text || 'text2' from generate_series (1,2000000) gen;

time psql -d adb -Atc 'create index btree_int on table4 using btree (id1);'
time psql -d adb -Atc 'create index btree_text on table4 using btree(gen1);'
time psql -d adb -Atc 'create index bitmap_text on table4 using bitmap (gen2);'
time psql -d adb -Atc 'create index bitmap_int on table4 using bitmap (id2);'

--- Лабораторная работа №7 (TRANSACTIONS)
create table table5 (id int , state text);
insert into table5 values (1,'insert 1'),(2, 'insert 2');
1. BEGIN;
2. BEGIN;
1. select * from table5;
2. update table5 set state = 'update 1 transaction 2' where id=1;
1. select * from table5; -- Не видим строки с апдейтом
2. commit;
1. select * from table5; -- Видим апдейт строки
1. COMMIT;

-- part 2
1. BEGIN;
2. BEGIN;
1. SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ WRITE;
1. select * from table5;
2. update table5 set state = 'update 2 transaction 2' where id=1;
2. COMMIT;
1. select * from table5;  -- Не видим строки с апдейтом
1. commit;
1. select * from table5; -- Видим апдейт строки уже в рамках другой транзакции

/* MVCC */

drop table if exists foo;

SET gp_select_invisible = TRUE;

create table foo (id int , state text);

insert into foo values (1,'insert 1'),(2, 'insert 2');

select xmin,xmax,* from foo;

update foo set state = 'update 1' where id=1;

select xmin,xmax,* from foo;

vacuum foo;

--- Лабораторная работа №8 (MVCC)
create table table6 (id int , state text);
insert into table6 values (1,'insert 1'),(2, 'insert 2');
1. BEGIN;
2. BEGIN;
1. update table6 set state='update 1 transaction 1' where id=1;
1. update table6 set state='update 2 transaction 1' where id=1;
1. SET gp_select_invisible = TRUE;
1. select xmin, xmax, * from table6; -- Видим несколько версий строки
2. select xmin, xmax, * from table6;  -- Не видим изменений, так как транзакция не закомичена
1. ROLLBACK; \q
2. ROLLBACK; \q
psql adb
select xmin, xmax, * from table6; -- Видно только  две первоначальные строки
SET gp_select_invisible = TRUE;
select xmin, xmax, * from table6; -- Видно 3 версии строк. Не смотря на то, что xmax не проставлен, строк с апдейтом не видно
update table6 set state='update 3 transaction 1' where id=1;
select xmin, xmax, * from table6; -- Видим 4 версии строки с id=1. Последняя строка является актуальной версией.
SET gp_select_invisible = false;
select xmin, xmax, * from table6; -- Видим 2 строки и строка с id=1 содержит последний апдейт.

/*  Блокировки */
/*  Пример с покупкой товаров в интернет-магазине Alice и Bob */


-- создаем таблицу
create table toys (
  id serial not null,
  name text,
  usage int not null default 0
);

-- наполняем данными
insert into toys(name) values('car'),('ball'),('train');

-- проверяем блокировки. Их не должно быть для таблицы toys
select
lock.locktype,
lock.relation::regclass,
lock.mode,
lock.transactionid as tid,
lock.virtualtransaction as vtid,
lock.pid,
lock.granted,
lock.gp_segment_id
from pg_catalog.pg_locks lock
where lock.pid != pg_backend_pid()
and lock.relation = 'toys'::regclass
order by lock.pid;

-- Alice смотрит на сайте, какие игрушки есть в магазине
   begin;
   select * from toys;
   
-- Проверяем блокировки и видим, что на таблицу есть блокировки AccessShare на каждом сегменте и мастер-сервере
   
-- Alice запрашивает себе игрушку, но не уверена ещё, что купит её (транзакция ещё не в статусе commit)
   update toys set usage = usage + 1 where id = 2;
   
-- Проверяем блокировки. Наблюдаем, что для таблицы toys есть блокировка RowExclusive и AccessShare на сегментах.
-- А на мастер-сервере есть блокировка ExclusiveLock. Это нужно для того, чтобы контролировать взаимные блокировки (deadlock).
   
-- Bob в параллельной транзакции запрашивает себе другую игрушку. Но пока Alice не определится, Боб не сможет взять игрушку.
-- Транзакция переходит в статус ожидания и ждёт, пока не завершится другая транзакция(покупка Alice) 
   update toys set usage = usage + 1 where id = 1;  
   
-- Проверяем блокировки. ExclusiveLock для транзакции от Боба будет  иметь значение granted = f.

-- С помощью запроса смотрим информацию по блокировкам - кто кого блокирует.
SELECT blocked_locks.pid AS blocked_pid,
blocked_activity.usename  AS blocked_user,
blocking_locks.pid AS blocking_pid,
blocking_activity.usename AS blocking_user,
blocked_locks.mode AS lock_type,
blocked_activity.query AS blocked_statement,
blocking_activity.query AS current_statement_in_blocking_process
FROM  pg_catalog.pg_locks  blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
AND blocking_locks.pid != blocked_locks.pid 
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.GRANTED;

    -- или можно использовать запрос к функции:
    SELECT * FROM pg_catalog.gp_dist_wait_status();

   
-- Alice опредилилась с игрушкой и решила её купить (производим commit транзакции)
   COMMIT;
   
-- Как только Alice опредилилась с покупкой, Бобу тоже становится доступна игрушка.
   
-- Если включить параметр Global Deadlock Detector и повторить операции с Alice и Бобом, 
-- то Боб сможет параллельно с Alice купить игрушку, не дожидаясь пока Alice определится с покупкой.
gpconfig -c gp_enable_global_deadlock_detector -v on --masteronly
   
--- Лабораторная работа №9 (Блокировка)
1. BEGIN;
2. BEGIN;
1. update: update table5 set state='lock 1 transaction 1' where id=1;
2. select: select * from table5  -- команда выполняется
2. update: update table5 set state='lock 2 transaction 1' wh ere id=1;  -- команда зависла и ждёт
1. COMMIT;  -- во второй транзакции операция сразу выполнилась, так как освободилась блокировка

  -- взаимная блокировка --
1. BEGIN;
2. BEGIN;
1. update table6 set state= 'lock 1 transaction 1' where id=1; 
2. update table5 set state='lock 1 transaction 2' where id=1;
1. update table5 set state='lock 2 transaction 1' where id=1;
2. update table6 set state='lock 2 transaction 2' where id=1; 
-- сработал механизм разрешения взаимных блокировок
===============================================================
/* Analyze */

default_statistics_target -- влияет на точность сбора статистики.

create table lab11 (id int, descr text) distributed by (id);
create table lab12 (id int, descr text) distributed by (id);

insert into lab11 SELECT generate_series(1,4) AS id, md5(random()::text) AS descr;
insert into lab12 SELECT generate_series(1,200000) AS id, md5(random()::text) AS descr;

-- Хэш строится по маленькой таблице
explain analyze select * from lab11 JOIN lab12 using (id);

-- Добавляем записи, общее кол-во которых привышает ко-во строк в lab2
insert into lab11 SELECT generate_series(1,500000) AS id, md5(random()::text) AS descr;

-- Хэш должен строится по меньшей таблице lab12. Но он строится по таблице lab11, так как нет актуальной статистики.
explain analyze select * from lab11 JOIN lab12 using (id);

-- Собираем статистику
analyze lab11;

--- Проверяем запрос. Теперь хэш строится по таблице lab12
explain analyze select * from lab11 JOIN lab12 using (id);

--- (Статистика) -----------------------------------------------------------------------------------
 \d+ table1
 \d+ table2
 
set statement_mem = 20000;
 
insert into table1 select gen, gen, gen::text || 'text1', gen::text || 'text2' from generate_series (1000000,4000000) gen;

EXPLAIN ANALYZE select * from table1 t1 join table2 t2 on t1.id1 = t2.id1;

ANALYZE table1;

EXPLAIN ANALYZE select * from table1 t1 join table2 t2 on t1.id1 = t2.id1;