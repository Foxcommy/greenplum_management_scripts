//==================================================================================================================

/* Внешние таблицы */

-- Внешняя web-таблица для записи, которая использует EXECUTE
DROP external web table IF EXISTS lab_web_ext_write; 
CREATE WRITABLE EXTERNAL WEB TABLE lab_web_ext_write (like table1) EXECUTE 'cat > /tmp/web_out_$GP_SEGMENT_ID.csv' FORMAT 'CSV' (DELIMITER',');

select * from table1;

insert into lab_web_ext_write select * from table1;

\q
ssh sdw1
cd /tmp/.
ls -ltr
head web_out_0.csv
head web_out_1.csv

-- Внешняя web-таблица для чтения, которая использует http-протокол
DROP external web table IF EXISTS lab_web_ext_http; 
CREATE EXTERNAL WEB TABLE lab_web_ext_http (parent text, student text) LOCATION ( 'http://insight.dev.schoolwires.com/HelpAssets/C2Assets/C2Files/C2ImportFamRelSample.csv') FORMAT 'CSV' (HEADER);

-- Внешняя таблица для чтения, которая использует file-протокол
DROP external table IF EXISTS lab_file_ext; 
CREATE EXTERNAL TABLE lab_file_ext (like table1) LOCATION ('file://sdw1/tmp/web_out_0.csv','file://sdw1/tmp/web_out_1.csv','file://sdw2/tmp/web_out_2.csv','file://sdw2/tmp/web_out_3.csv') FORMAT 'CSV' (DELIMITER ',');



--- (External tables)
DROP external table IF EXISTS hosts;
CREATE EXTERNAL WEB TABLE hosts (config text)
EXECUTE 'cat /etc/hosts' ON HOST
FORMAT 'TEXT' (DELIMITER 'OFF');

DROP external table IF EXISTS hosts2;
CREATE EXTERNAL TABLE hosts2 (config text)
LOCATION('file://sdw1/etc/hosts','file://sdw2/etc/hosts')
FORMAT 'TEXT' (DELIMITER 'OFF');


/* GPFDIST */
cd /adb/files
ls -lh
head file_load_1.csv

-- Создаём внутреннюю таблицу для загрузки в неё даннх.
drop table if exists test_load_csv_int;
create table test_load_csv_int (Region text, Country text, "Item Type" text, "Sales Channel" text, "Order Priority" text, "Order Date" date, "Order ID" int, "Ship Date"  date, "Units Sold" numeric, "Unit Price" numeric,"Unit Cost" numeric,"Total Revenue" numeric,"Total Cost" numeric,"Total Profit" numeric) with (appendonly =true, orientation=column, compresstype=zstd , compresslevel=1) distributed by ("Order ID");

-- Создаём внешнюю таблицу с протоколом gpfdist. Таблица копирует название колонок и их тип из внутренней таблицы.
drop external table if exists test_load_csv_ext;
CREATE  EXTERNAL TABLE test_load_csv_ext (like test_load_csv_int)
LOCATION ('gpfdist://10.129.0.32:8081/file_load_1.csv',
          'gpfdist://10.129.0.32:8081/file_load_2.csv')
FORMAT 'CSV' (HEADER DELIMITER AS ',' FILL MISSING FIELDS);

-- не сработает
INSERT INTO test_load_csv_int SELECT * FROM test_load_csv_ext;

-- Запускаем gpfdist на сервере, где расположены файлы file_load_1.csv и file_load_2.csv
gpfdist -p 8081 -d /adb/files

-- Включаем timing и загружаем данные
\timing
INSERT INTO test_load_csv_int SELECT * FROM test_load_csv_ext;

-----------------------------------------------------------

-- Пример, когда утилита gpfdist читает данные из stdin

-- Создаём внешнюю таблицу
drop external table if exists sel

CREATE  EXTERNAL TABLE lab15_stdin (id int, descr text)
LOCATION ('gpfdist://10.129.0.32:8080/*.csv')
FORMAT 'CSV' (DELIMITER AS ',');

-- На сервере, где будем передавать данные в stdin, создаём файл для тестов
for i in $(seq 1 10); do echo "$i,Text-$i"; done > /home/gpadmin/gp_example.csv

-- На этом же сервере запускаем команду, которая считывает файл и передаёт значения из файла на stdin утилите gpfdist
cat /home/gpadmin/gp_example.csv | gpfdist -p 8080 -f /dev/stdin -v

------------------------------------------------------------


-- Пример, когда с помощью протокола gpfdist можно выгрузить данные из GP

-- создаём внешнюю таблицу для записи
drop external table if exists lab16;
CREATE  WRITABLE EXTERNAL TABLE lab16 (like test_load_csv_int)
LOCATION ('gpfdist://10.129.0.32:8083/lab16.csv')
FORMAT 'CSV' (DELIMITER AS ',');

-- на сервере, куда хотим выгрузить данные в файл, запускаем утилиту gpfdist
gpfdist -p 5555 -d /home/gpadmin/ -v
gpfdist -p 8083 -d /home/gpadmin/ -v

-- выполняем запрос
INSERT INTO lab16 SELECT * FROM test_load_csv_int;

-- проверяем, что данные выгрузились в файл lab16.csv
head /home/gpadmin/lab16.csv

-- Пример, когда с помощью протокола gpfdist можно выгрузить данные из GP


----------------------------------------------------------
--- (GPFDIST)

mkdir /tmp/gpfdist_test

for i in $(seq 1 10000); do echo "$i,foo$i"; done > /tmp/gpfdist_test/sample_1.csv
for i in $(seq 10001 20000); do echo "$i,foo$i"; done > /tmp/gpfdist_test/sample_2.csv

gpfdist -p 5555 -d /tmp/gpfdist_test &

drop external table if exists gpfdist_ext_1;
CREATE  EXTERNAL TABLE gpfdist_ext_1 (id int, gen text)
LOCATION ('gpfdist://mdw:5555/sample*')
FORMAT 'CSV' (DELIMITER AS ',');

SELECT * FROM gpfdist_ext_1;




  -- part 2 -----------------------------------------
echo "cat \$1 |sed 's/foo/bar/g'" > /tmp/gpfdist_test/foobar.sh
  
vi /tmp/gpfdist_test/config.yaml

=====================================  
---
VERSION: 1.0.0.1
TRANSFORMATIONS:
  foobar:
    TYPE:     input
    COMMAND:  /bin/bash /tmp/gpfdist_test/foobar.sh %filename%
=======================================
	
gpfdist -p 8087 -d /tmp/gpfdist_test -c /tmp/gpfdist_test/config.yaml &

drop external table if exists gpfdist_ext_2;
CREATE  EXTERNAL TABLE gpfdist_ext_2 (id int, gen text)
LOCATION ('gpfdist://mdw:8087/sample*#transform=foobar')
FORMAT 'CSV' (DELIMITER AS ','); 

SELECT * FROM gpfdist_ext_2;

---------------------------------------------------------------------------------------------------------
/*gpload*/
cat /adb/examples/gpload_config.yaml

drop table if exists table10;
create table table10 (id int, gen text);

gpload -f /adb/examples/gpload_config.yaml

SELECT * FROM table10;


--- Лабораторная работа №13 (GPLOAD)

create table table10 (id int, gen text) DISTRIBUTED RANDOMLY;

vi /tmp/gpfdist_test/gpload_config.yaml
==========================================
--- 
VERSION: 1.0.0.1
DATABASE: adb
USER: gpadmin
HOST: mdw
PORT: 5432
GPLOAD:
   INPUT:
    - SOURCE:
         LOCAL_HOSTNAME:
           - mdw
         PORT: 5566
         FILE:
           - /tmp/gpfdist_test/sample*
    - COLUMNS:
           - id: int
           - gen: text 
    - FORMAT: CSV
    - DELIMITER: ','
    - ERROR_LIMIT: 25
    - LOG_ERRORS: true
   OUTPUT:
    - TABLE: table10
    - MODE: INSERT
   PRELOAD:
    - REUSE_TABLES: true

==========================================

gpload -f /tmp/gpfdist_test/gpload_config.yaml

------------------------------------------------------------------------------------------------


/*PXF*/

--- (PXF)

 --- part 1----

CREATE TABLE table11 (id1 int, id2 int, gen text, now timestamp without time zone)
WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=1)
DISTRIBUTED BY (id1);

create index table11_btree_id1 on table11 using btree (id1);

insert into table11 select gen, gen, 'text' || gen::text, now () from generate_series (1,4000000) gen;

DROP EXTERNAL TABLE if exists table_11_pxf_read;
CREATE EXTERNAL TABLE table_11_pxf_read(like table11)
LOCATION ('pxf://public.table11?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://mdw:5432/adb&USER=gpadmin')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

EXPLAIN ANALYZE SELECT COUNT(*) FROM table_11_pxf_read;

 --- part 2 ----
 
DROP EXTERNAL TABLE if exists table_11_pxf_read_parallel;
CREATE EXTERNAL TABLE table_11_pxf_read_parallel(like table11)
LOCATION ('pxf://public.table11?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://mdw:5432/adb&USER=gpadmin&PARTITION_BY=id1:int&RANGE=1:4000001&INTERVAL=500000')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

EXPLAIN ANALYZE SELECT COUNT(*) FROM table_11_pxf_read_parallel;

 --write---
create table table12 (like table11) DISTRIBUTED RANDOMLY;

DROP EXTERNAL TABLE if exists table_12_pxf_write;
CREATE WRITABLE EXTERNAL TABLE table_12_pxf_write(like table11)
LOCATION ('pxf://public.table12?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://mdw:5432/adb&USER=gpadmin&BATCH_SIZE=25&POOL_SIZE=2')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')
DISTRIBUTED BY (id1);

insert into table_12_pxf_write select * from table11 limit 100;


/* COPY */

-- Обычные пользователи могут выгружать данные только в stdout, а потом записать данные в файл
psql -d adb -U user1 -c "copy lab1 to stdout DELIMITER ',';" > copy_out.csv


-- Superusers могут выгружать данные в файл на сегмент хосты, либо на мастер 
copy table1 to '/tmp/lab1_<SEGID>.csv' on segment DELIMITER ',';



-- Пример загрузки данных из файла, который находится на вашем компьютере
-- с помощью psql-клиента установленного на Windows

-- создаём таблицу для загрузки данных
create table lab17 (like lab8);

-- создаем пользователя, кто будет загружать данные в таблицу и даём права на таблицу
CREATE ROLE copy_user login password 'copy_user';
GRANT ALL ON TABLE lab17 TO copy_user;

-- на компьютере, откуда нужно загрузить данные, создаём тестовый файл test_copy.csv
1,COPY-1
2,COPY-2
3,COPY-3
4,COPY-4
5,COPY-5

-- запускаем команду для загрузки данных из файла
type test_copy.csv | psql -d adb -h 10.129.0.32 -U copy_user -c "copy lab17 from stdin DELIMITER ',';"



------------------------------------
--- (COPY)
create table table7 (id int, state text) distributed randomly; 

insert into table7 select gen, 'text ' || gen::text from generate_series(1,200000) gen;

gpssh -f /home/gpadmin/arenadata_configs/arenadata_all_hosts.hosts 'mkdir /tmp/copy_out'

copy table7 to '/tmp/copy_out/table7_<SEGID>.csv' on segment DELIMITER ',';

head /tmp/copy_out/table7_1.csv

truncate table table7;

copy table7 from '/tmp/copy_out/table7_<SEGID>.csv' on segment DELIMITER ',';

truncate table table7;

sdw1: echo 'wrong string' >> /tmp/copy_out/table7_0.csv

copy table7 from '/tmp/copy_out/table7_<SEGID>.csv' on segment DELIMITER ',' LOG ERRORS SEGMENT REJECT LIMIT 2;

select gp_read_error_log('public.table7'); 

select gp_truncate_error_log('public.table7');


-------------------------------------------------------------------------------
/* UDF */

-- Пример stable-функции
begin;
select now();
commit;

-- Пример VOLATILE-функции
begin;
select timeofday();
commit;

-- Таблица для демонстрации работы функций
DROP TABLE IF EXISTS lab18;	
CREATE TABLE lab18 (f1 text, f2 int, f3 int) DISTRIBUTED BY (f2);
INSERT INTO lab18 VALUES ('A', 2, 5),('A', 3, 5),('A', 4, 5),('B', 6, 10), ('C', 2, 20);


-- Пример VOLATILE-функции. Может ли работать на сегментах или только на мастере
CREATE OR REPLACE FUNCTION t1_calc( name text) RETURNS integer
AS $$
DECLARE
t1_row lab18%ROWTYPE;
calc_int lab18.f3%TYPE;
BEGIN
SELECT * INTO t1_row FROM lab18 WHERE lab18.f1 = $1;
calc_int = (t1_row.f2 * t1_row.f3)::integer;
RETURN calc_int;
END;
$$ LANGUAGE plpgsql VOLATILE;

explain (analyze, verbose) select t1_calc('A');  -- работает только на мастере (в плане есть только slice0)
explain (analyze, verbose) select t1_calc('A') from lab18; -- должна запуститься на сегментах, но не может из-за того, что обращаемся к данным непосредственно на сегментах


------- Пример функции с указанием места выполнения ----------------
CREATE OR REPLACE FUNCTION where_am_i_running() RETURNS SETOF text 
AS $$
  BEGIN 
    RETURN NEXT ('Current time - ' || timeofday()::text); 
  END;
 $$ LANGUAGE plpgsql VOLATILE EXECUTE ON ALL SEGMENTS;
 
explain (analyze, verbose) select where_am_i_running(); -- работает на сегментах, так как slice1 обрабатывается на нескольких worker-ах
select where_am_i_running(); -- показывает одно значение на каждый сегмент 
 
CREATE OR REPLACE FUNCTION where_am_i_running() RETURNS SETOF text 
AS $$
  BEGIN 
    RETURN NEXT ('Current time - ' || timeofday()::text); 
  END;
 $$ LANGUAGE plpgsql VOLATILE EXECUTE ON MASTER;
 
explain (analyze, verbose) select where_am_i_running(); -- работает только на мастере (в плане есть только slice0)
select where_am_i_running(); -- показывает одно значение, так как отрабатывает только на мастер-сервере
 
 
CREATE OR REPLACE FUNCTION where_am_i_running() RETURNS SETOF text 
AS $$
  BEGIN 
    RETURN NEXT ('Current time - ' || timeofday()::text); 
  END;
 $$ LANGUAGE plpgsql VOLATILE EXECUTE ON ANY;
 
explain (analyze, verbose) select where_am_i_running();  -- работает только на мастере (в плане есть только slice0)
explain (analyze, verbose) select where_am_i_running() from lab18; -- работает на сегментах, так как slice1 обрабатывается на нескольких worker-ах
select where_am_i_running() from lab18; -- показывает результат для каждой строки в таблице lab18. Для каждой строки была применена функция.


-- Пример функции, которая возвращает больше, чем одна строка для REPLICATED таблиц 
CREATE OR REPLACE FUNCTION test_func_seg(name text) 
RETURNS SETOF lab18
AS $$
DECLARE
    q4 lab18;
BEGIN
    FOR q4 in
        SELECT * FROM lab18 WHERE f1 = $1 and f2=3
    LOOP
        RETURN NEXT q4;
    END LOOP;
END;
$$ LANGUAGE plpgsql VOLATILE EXECUTE ON ALL SEGMENTS;


-- Выполняем запрос к функции
select test_func_seg('A'); -- функция не отработает, так как lab18 распределена по полю f2, функция обращается к распредённой таблицы и возвращает одну или несколько строк.

-- Меняем распределение таблицы lab18 на DISTRIBUTED REPLICATED
ALTER TABLE lab18 set DISTRIBUTED REPLICATED;

-- Выполняем тотже самый запрос к функции
explain (analyze, verbose) select test_func_seg('A'); -- функция отрабатывает на сегментах, так как slice1 обрабатывается на нескольких worker-ах
select test_func_seg('A'); -- для каждого сегмента, функция возвращает найденную строку

-- Пример функции, которая выполняет UPDATE

-- создаём таблицу и вставляем данные
DROP TABLE IF EXISTS lab19;
CREATE TABLE lab19 (id int, descr text);
INSERT INTO lab19 VALUES (1,'raz'),(2,'dva');

-- создаем функцию, которая будет запускаться на сегментах
CREATE OR REPLACE FUNCTION update_data()
RETURNS NUMERIC AS $$
DECLARE
    v_cnt numeric;
BEGIN
    v_cnt := 0;
	    UPDATE lab19 SET descr='update by function' WHERE id=1;
        GET DIAGNOSTICS v_cnt = ROW_COUNT;
    RETURN v_cnt;
END;
$$ LANGUAGE plpgsql EXECUTE ON ALL SEGMENTS;

-- запускаем функцию 
SELECT update_data(); -- запрос не выполнится, так как функция пытается обновить данные непосредственно на сегменте. Мастер-сервер не контролирует этот процесс.

-- меняем функцию, чтобы функция запускалась на мастере
CREATE OR REPLACE FUNCTION update_data()
RETURNS NUMERIC AS $$
DECLARE
    v_cnt numeric;
BEGIN
    v_cnt := 0;
	    UPDATE lab19 SET descr='update by function' WHERE id=1;
        GET DIAGNOSTICS v_cnt = ROW_COUNT;
    RETURN v_cnt;
END;
$$ LANGUAGE plpgsql EXECUTE ON MASTER;

-- запускаем функцию 
SELECT update_data(); -- функция отработает и обновит данные на сегментах. Как если бы мы просто запустили команду UPDATE в консоли

-- Пример использования локального словаря

-- создаддим две одинаковых функции с запуском на сегментах
CREATE OR REPLACE FUNCTION local_dict_1() RETURNS text AS $$
import socket
hostname = socket.gethostname()
if SD.has_key("plan"):
    plan = SD["plan"]
    a = "Key is present! Host: " + hostname
    return a
else:
    plan = plpy.prepare("SELECT 1")
    SD["plan"] = plan
    a = "Key doesn't present. Add key to the dictionary. Host: " + hostname
    return a
$$ LANGUAGE plpythonu EXECUTE ON ALL SEGMENTS;

CREATE OR REPLACE FUNCTION local_dict_2() RETURNS text AS $$
import socket
hostname = socket.gethostname()
if SD.has_key("plan"):
    plan = SD["plan"]
    a = "Key is present! Host: " + hostname
    return a
else:
    plan = plpy.prepare("SELECT 1")
    SD["plan"] = plan
    a = "Key doesn't present. Add key to the dictionary. Host: " + hostname
    return a
$$ LANGUAGE plpythonu EXECUTE ON ALL SEGMENTS;

-- запустим функию local_dict_1() два раза. 
-- В первый раз будет сообщение, что ключа не существует, но во второй раз - ключ уже будет присутствовать в словаре
SELECT local_dict_1();

-- запустим функию local_dict_2()
-- При первом запуске будет сообщение, что ключа не существует. 
-- Так как ключ локальный, то он не может быть расшарен между двумя разными функциями
SELECT local_dict_2();


-- Пример использования глобального словаря
CREATE OR REPLACE FUNCTION global_dict_1() RETURNS text as $$ 
if 'socket' not in GD:
    import socket
    GD['socket'] = socket
    return "Import module 'socket'. Host: " + GD['socket'].gethostname()
else:
    return "Module 'socket' is already in global dictionary. Host: " + GD['socket'].gethostname()
$$ LANGUAGE plpythonu EXECUTE ON ALL SEGMENTS;

CREATE OR REPLACE FUNCTION global_dict_2() RETURNS text as $$ 
if 'socket' not in GD:
    import socket
    GD['socket'] = socket
    return "Import module 'socket'. Host: " + GD['socket'].gethostname()
else:
    return "Module 'socket' is already in global dictionary. Host: " + GD['socket'].gethostname()
$$ LANGUAGE plpythonu EXECUTE ON ALL SEGMENTS;

-- запустим функию global_dict_1() два раза. 
-- В первый раз будет сообщение, что был импортирован модуль. Второй раз - модуль уже существует в глобальном словаре
SELECT global_dict_1();

-- запустим функию global_dict_2()
-- При первом же запуске будет сообщение, что модуль уже существует в глобальном словаре. 
-- Так как ключ глобальный, то он может быть расшарен между двумя разными функциями.
SELECT global_dict_2();


-------------------------------------------------------------
--- (UDF)

 -- part 1 --
create or replace function get_host_pyt() returns text  
as $$
    import socket
    return 'I am running on host: ' + socket.gethostname()
$$
volatile
language plpythonu execute on all segments;

select get_host_pyt();


create or replace function get_host_pyt3() returns text  
as $$
	import socket
	return 'I am running on host: ' + socket.gethostname()
$$
volatile
language plpythonu execute on any;

select get_host_pyt3();

 -- part 2 ------------------
create or replace function get_host_cont() returns text  
as $$
# container: plc_py
	import socket
	return 'I am running on host: ' + socket.gethostname()
$$
volatile
language plcontainer execute on all segments;

select get_host_cont();

create or replace function get_host_cont2() returns text  
as $$
# container: plc_py
	import socket
	return 'I am running on host: ' + socket.gethostname()
$$
volatile
language plcontainer execute on master;

select get_host_cont2();


------------------------------------------------
/* MADLib */

-- Пример со слайдов

CREATE TABLE test_data (trans_id INT, product text);

INSERT INTO test_data VALUES
(1, 'beer'),
(1, 'diapers'),
(1, 'chips'),
(2, 'beer'),
(2, 'diapers'),
(3, 'beer'),
(3, 'diapers'),
(4, 'beer'),
(4, 'chips'),
(5, 'beer'),
(6, 'beer'),
(6, 'diapers'),
(6, 'chips'),
(7, 'beer'),
(7, 'diapers');


SELECT * FROM madlib.assoc_rules (.40, .75, 'trans_id', 'product', 'test_data', 'public', true); 

SELECT pre, post, support FROM assoc_rules ORDER BY support DESC;

--- Лабораторная работа №17 (MADlib)------------
CREATE TABLE regr_example (
id int, y int, x1 int, x2 int );

INSERT INTO
regr_example VALUES
(1, 5, 2, 3),
(2, 10, 7, 2),
(3, 6, 4, 1),
(4, 8, 3, 4);

SELECT madlib.linregr_train (
'regr_example',
'regr_example_model',
'y',
'ARRAY[1, x1, x2]'
);

SELECT regr_example.*, madlib.linregr_predict ( ARRAY[1, x1, x2], m.coef ) as predict,
y - madlib.linregr_predict ( ARRAY[1, x1, x2], m.coef ) as residual
FROM regr_example , regr_example_model m;
-------------------------------------------------

/* POST GIS */

-- Создаем таблицу 
CREATE TABLE ski_resorts (
    id INTEGER NOT NULL, 
    name VARCHAR(30) NOT NULL, 
	town VARCHAR(30), 
	country_code CHARACTER(2), 
	location GEOGRAPHY(POINT,4326), 
	CONSTRAINT pk_id PRIMARY KEY(id)
	);

-- Вставляем данные по горнолыжныи курортам
INSERT INTO ski_resorts (id, name, town, country_code, location) VALUES (1,'Rosa Khutor','Sochi','RU',ST_GeogFromText('SRID=4326;POINT(40.2809639 43.6769142)'));
INSERT INTO ski_resorts (id, name, town, country_code, location) VALUES (2,'Sheregesh','Kemerovo','RU',ST_GeogFromText('SRID=4326;POINT(87.955528 52.953466)'));
INSERT INTO ski_resorts (id, name, town, country_code, location) VALUES (3,'Sorochany','Moscow area','RU',ST_GeogFromText('SRID=4326;POINT(37.565472 56.279871)'));
INSERT INTO ski_resorts (id, name, town, country_code, location) VALUES (4,'Chimbulak','Almaty','KZ',ST_GeogFromText('SRID=4326;POINT(77.080578 43.127966)'));


-- Проверяем данные
SELECT * FROM ski_resorts;

-- Запрашиваем координаты по курорту Роза Хутор
SELECT ST_AsText(location) AS Dist_deg FROM ski_resorts WHERE name='Rosa Khutor';

-- Координаты по отдельности
SELECT ST_X(ST_AsText(location)), ST_Y(ST_AsText(location)) FROM ski_resorts WHERE name='Rosa Khutor';

-- Вычисляем расстояние между Роза Хутор и Сорочанами
SELECT ST_Distance(a.location, b.location)/1000 AS dist FROM ski_resorts a, ski_resorts b WHERE a.name='Rosa Khutor' AND b.name='Sorochany';



