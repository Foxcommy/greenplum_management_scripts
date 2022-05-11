-- Подключение к СУБД из cli
sudo su - gpadmin
psql adb 

select * from pg_catalog.pg_class where relname = 'pg_class';
\d+ gp_toolkit.__gp_log_master_ext

-- Служебные таблицы и схемы
select oid, relname, relnamespace, relstorage, relpersistance, reltuples from pg_catalog.pg_class;
select * from pg_catalog.gp_segment_configuration;
select * from pg_catalog.gp_configuration_history;
select * from pg_catalog.pg_stat_activity;
select * from pg_catalog.pg_settings;
select rsgname from gp_toolkit.gp_resgroup_status;

--- Управление доступам на основе ролей
create role adb_super superuser login password 'superpassword' resource group admin_group;
create role adb_group nologin;
create role adb_1 login in role adb_group;
create role adb_2 login;
grant adb_group to adb_2;
grant all on protocol pxf to adb_group;
grant select on table arenadata_toolkit.db_files_current to adb_group;
grant usage on schema arenadata_toolkit to adb_group;
alter role adb_1 with password 'password_1';

--Демо для распределения:
create table table_foo (a int, b text) distributed by (a);
insert into table_foo values (1, 'raz'), (2, 'dva');
select gp_segment_id, * from table_foo;

-- Таблицы
/* Демонстрация размера таблиц с разным типом хранения данных */
create table lab4_heap (id int, descr text) with (appendonly=false) distributed by (id);
create table lab4_ao_row (id int, descr text) with (appendonly=true, orientation=row) distributed by (id);
create table lab4_ao_row_comp (id int, descr text) with (appendonly=true, orientation=row, compresstype=zlib, compresslevel=1) distributed by (id);
create table lab4_ao_column (id int, descr text) with (appendonly=true, orientation=column) distributed by (id);
create table lab4_ao_column_comp (id int, descr text) with (appendonly=true, orientation=column, compresstype=zlib, compresslevel=1) distributed by (id);

insert into lab4_heap SELECT generate_series(1,3000000) AS id, md5(random()::text) AS descr;
insert into lab4_ao_row SELECT generate_series(1,3000000) AS id, md5(random()::text) AS descr;
insert into lab4_ao_row_comp SELECT generate_series(1,3000000) AS id, md5(random()::text) AS descr;
insert into lab4_ao_column SELECT generate_series(1,3000000) AS id, md5(random()::text) AS descr;
insert into lab4_ao_column_comp SELECT generate_series(1,3000000) AS id, md5(random()::text) AS descr;

SELECT pg_size_pretty(pg_total_relation_size('public.lab4_heap'));
SELECT pg_size_pretty(pg_total_relation_size('public.lab4_ao_row'));
SELECT pg_size_pretty(pg_total_relation_size('public.lab4_ao_row_comp'));
SELECT pg_size_pretty(pg_total_relation_size('public.lab4_ao_column'));
SELECT pg_size_pretty(pg_total_relation_size('public.lab4_ao_column_comp'));

select * from lab4_heap where id<1000 limit 5;

--/* Distribution */
--Дополнительно
-- Хорошее распределение
CREATE TABLE lab5 AS SELECT generate_series(1,100000) AS id, md5(random()::text) AS descr DISTRIBUTED BY (id);
SELECT gp_segment_id, count(*) FROM lab5 GROUP BY 1;

-- Плохое распределение
ALTER TABLE lab5 SET DISTRIBUTED BY (descr);
SELECT gp_segment_id, count(*) FROM lab5 GROUP BY 1;

--/* Diskquota */
-- Дополнительно
create schema stg2;
create table stg2.test_quota(id int, descr text) distributed by (id);

SELECT diskquota.init_table_size_table();
SELECT diskquota.set_schema_quota('stg2', '10MB');
SELECT * FROM diskquota.show_fast_schema_quota_view;
insert into stg2.test_quota select generate_series(1,2000000) AS id, md5(random()::text) AS descr;
SELECT pg_size_pretty( pg_total_relation_size('stg.test_quota') );
insert into stg2.test_quota select generate_series(1,1000000) AS id, md5(random()::text) AS descr;