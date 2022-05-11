ALTER DATABASE adb SET search_path TO etl, 
public, pg_catalog;

############# Лабораторная работа №1 (Блокировки) #############
create table foo (id int, state text); 
insert into foo values (1,'insert 1'),(2, 'insert 2');

открыть 2 терминала psql, указать параметр \set AUTOCOMMIT off 

В первом окне выполнить delete from foo;
Во втором окне выполнить select * from foo; -- Видим записи, т.к. транзакция в первом окне не завершена

В первом окне выполнить commit;
Во втором окне выполнить select * from foo; -- Записей нет, т.к. транзакция в первом окне завершена, данные удалены.

В первом окне выполнить insert into foo values (1,'insert 1'),(2, 'insert 2');
В первом окне выполнить truncate foo; Операция не выполнилась, т.к. во втором окне не завршена транзакция select;

Во втором окне выполнить commit; -- Операция truncate в первом окне завршается.
Во втором окне выполнить select * from foo; -- операция не выполняется, т.к. ждем завершения транзакции truncate в первом окне



############# Лабораторная работа №2 (table rename) #############
create table foo1 (id int, val text);
create table foo2 (id int, val text);
insert into foo1 values (1,'Insert1');
insert into foo2 values (2,'Insert2');

create view vw_foo as select * from foo1;
select * from vw_foo;

do $$
begin
	alter table foo1 rename to foo_tmp;
	alter table foo2 rename to foo1;
	alter table foo_tmp rename to foo2;
end $$ language plpgsql;

select * from vw_foo;
\d+ vw_foo



############# Лабораторная работа №3 (PXF) #############
create external table etl.stg_foo_ext
(proid integer
 ,operday date
 ,dbacc integer
 ,dbcur varchar(3)
 ,cracc integer
 ,crcur varchar(3)
 ,dbsum decimal(31,10)
 ,crsum decimal(31,10)
 ,purpose varchar(500)
)
LOCATION ('pxf://C##ETL_USER.PROVODKI?PROFILE=JDBC&JDBC_DRIVER=oracle.jdbc.driver.OracleDriver&DB_URL=jdbc:oracle:thin:@//oracle-server:1521/XE&USER=c##etl_user&PASS=c##etl_user')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

create table etl.stg_foo_rnd
(proid integer
 ,operday date
 ,dbacc integer
 ,dbcur varchar(3)
 ,cracc integer
 ,crcur varchar(3)
 ,dbsum decimal(31,10)
 ,crsum decimal(31,10)
 ,purpose varchar(500)
)
with (appendonly=true , orientation=column, compresstype=zstd, compresslevel=1) distributed randomly;

insert into etl.stg_foo_rnd
select * from etl.stg_foo_ext
where operday >= '05.01.2020'::date;

select x.sid
     ,x.serial#
     ,x.username
     ,sql_text
     ,x.sql_id
     ,x.sql_child_number
     ,optimizer_mode
     ,hash_value
     ,address
from   v$sqlarea sqlarea,v$session x
where x.sql_hash_value = sqlarea.hash_value
and x.sql_address = sqlarea.address
and x.username = 'C##ETL_USER';


drop external table etl.stg_foo_ext;
create external table etl.stg_foo_ext
(proid integer
 ,operday date
 ,dbacc integer
 ,dbcur varchar(3)
 ,cracc integer
 ,crcur varchar(3)
 ,dbsum decimal(31,10)
 ,crsum decimal(31,10)
 ,purpose varchar(500)
)
LOCATION ('pxf://C##ETL_USER.PROVODKI?PROFILE=JDBC&JDBC_DRIVER=oracle.jdbc.driver.OracleDriver&DB_URL=jdbc:oracle:thin:@//oracle-server:1521/XE&USER=c##etl_user&PASS=c##etl_user&&PARTITION_BY=operday:date&RANGE=2020-05-01:2020-05-20&INTERVAL=1:day')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')

insert into etl.stg_foo_rnd
select * from etl.stg_foo_ext
where operday >= '05.01.2020'::date;



############# Лабораторная работа №4 (Загрузка Stage-слоя) #############
create table etl.stg_clients_id
(clientcode integer
 ,fio varchar(100)
 ,inn varchar(20)
 ,gender varchar(1)
 ,birthdate date
 ,doc_seria varchar(20)
 ,doc_number varchar(20)
 ,hashdiff uuid
 ,load_date_time timestamp
 ,load_source varchar(20)
 ,part int
)
with (appendonly=true , orientation=column, compresstype=zstd, compresslevel=1) distributed by (clientcode);

create table etl.stg_accounts_id
(acccode integer
 ,accnum varchar(20)
 ,accname varchar(200)
 ,opendate date
 ,closedate date
 ,clicode integer
 ,hashdiff uuid
 ,load_date_time timestamp
 ,load_source varchar(20)
)
with (appendonly=true , orientation=column, compresstype=zstd, compresslevel=1) distributed by (acccode);

create table etl.stg_provodki_id
(proid integer
 ,operday date
 ,dbacc integer
 ,dbcur varchar(3)
 ,cracc integer
 ,crcur varchar(3)
 ,dbsum decimal(31,10)
 ,crsum decimal(31,10)
 ,purpose varchar(500)
 ,hashdiff uuid
 ,load_date_time timestamp
 ,load_source varchar(20)
)
with (appendonly=true , orientation=column, compresstype=zstd, compresslevel=1) distributed by (dbacc);


create function etl.sp_stg_client_id_load() returns void
    language plpgsql
as
$$
declare
    v_function text := 'sp_stg_client_id_load';
    v_location text;

begin
    v_location := 'Truncate stg_clients_id';
    truncate etl.stg_clients_id;
    insert into etl.stg_clients_id
    select *
            ,md5(fio||'|'||inn||'|'||gender||'|'||to_char(birthdate,'ddmmyyyy')||'|'||doc_seria||'|'||doc_number)::uuid
            ,now()
            ,'ABS'
            ,1
    from etl.stg_clients_ext;

exception
    when others then
        raise exception '(%:%:%)', v_function, v_location, sqlerrm;
end;
$$;

create function etl.sp_stg_account_id_load() returns void
    language plpgsql
as
$$
declare
    v_function text := 'sp_stg_account_id_load';
    v_location text;

begin
    v_location := 'Truncate stg_accounts_id';
    truncate etl.stg_accounts_id;
    insert into etl.stg_accounts_id
    select *
            ,md5(acccode||'|'||accnum||'|'||accname||'|'||to_char(opendate,'ddmmyyyy')||'|'||to_char(COALESCE(closedate,'12.31.2100'),'ddmmyyyy')||'|'||clicode)::uuid
            ,now()
            ,'ABS'
    from etl.stg_accounts_ext;

exception
    when others then
        raise exception '(%:%:%)', v_function, v_location, sqlerrm;
end;
$$;

create or replace function etl.sp_stg_provodki_id_load(p_operday date) returns void
    language plpgsql
as
$$
declare
    v_function text := 'sp_stg_provodki_id_load';
    v_location text;

begin
    v_location := 'Truncate stg_provodki_id';
    truncate etl.stg_provodki_id;
    v_location := 'Insert into stg_provodki_id';
    insert into stg_provodki_id
    select proid,
           operday,
           dbacc,
           dbcur,
           cracc,
           crcur,
           dbsum,
           crsum,
           purpose,
           md5(to_char(operday,'ddmmyyyy')||'|'||dbacc||'|'||dbcur||'|'||cracc||'|'||crcur||'|'||dbsum||'|'||crsum||'|'||purpose)::uuid,
           now(),'ABS'
    from stg_provodki_ext
    where operday = p_operday;

exception
    when others then
        raise exception '(%:%:%)', v_function, v_location, sqlerrm;
end;
$$;


do $$
begin
	--perform sp_stg_client_rnd_load();
	--perform sp_stg_client_id_load();
	--perform sp_stg_account_rnd_load();
	--perform sp_stg_account_id_load();
    --perform sp_stg_provodki_rnd_load('05.05.2020');
    perform sp_stg_provodki_id_load('05.05.2020');
end $$ language plpgsql;




############# Лабораторная работа №5 (Обновление справочников) #############
create table etl.dim_clients_id
(clientcode integer
 ,fio varchar(100)
 ,inn varchar(20)
 ,gender varchar(1)
 ,birthdate date
 ,doc_seria varchar(20)
 ,doc_number varchar(20)
 ,hashdiff uuid
 ,load_date_time timestamp
 ,load_source varchar(20)
 ,part int
)
with (appendonly=true , orientation=column, compresstype=zstd, compresslevel=1) distributed by (clientcode)
partition by list (part) (default partition main);

create table dim_accounts_hist_id
(acccode integer
 ,accnum varchar(20)
 ,accname varchar(200)
 ,opendate date
 ,closedate date
 ,clicode integer
 ,hashdiff uuid
 ,load_date_time timestamp
 ,load_source varchar(20)
 ,date_from date
 ,date_to date
)
with (appendonly=true , orientation=column, compresstype=zstd, compresslevel=1) distributed by (acccode)
PARTITION BY RANGE (date_to)
( START (date '2020-04-30') INCLUSIVE
END (date '2020-06-01') EXCLUSIVE
EVERY (INTERVAL '1 day'),
default partition current_value);

create function etl.sp_dim_client_id_full_load(p_load_type text) returns void
    language plpgsql
as
$$
declare
    v_function text := 'sp_stg_client_id_load' || ': load_type - ' || p_load_type;
    v_location text;
begin
    if p_load_type = 'TRUNCATE' then
        -- обновляем целевой справочник через truncate/insert

        v_location := 'Truncate dim_clients_id';
        truncate etl.dim_clients_id;

        v_location := 'insert into etl.dim_clients_id';
        insert into etl.dim_clients_id
        select clientcode, fio, inn, gender, birthdate, doc_seria, doc_number, hashdiff, load_date_time, load_source
        from (select *
                    ,row_number() over (partition by s.clientcode order by s.load_date_time desc) as rn -- ранжируем записи на источнике, чтобы потом исключить дубли
              from etl.stg_clients_id s) s
        where rn=1;
    elseif p_load_type = 'DELETE' then
        -- обновляем целевой справочник через truncate/insert

        v_location := 'delete dim_clients_id';
        delete from etl.dim_clients_id;

        v_location := 'insert into etl.dim_clients_id';
        insert into etl.dim_clients_id
        select clientcode, fio, inn, gender, birthdate, doc_seria, doc_number, hashdiff, load_date_time, load_source
        from (select *
                    ,row_number() over (partition by s.clientcode order by s.load_date_time desc) as rn -- ранжируем записи на источнике, чтобы потом исключить дубли
              from etl.stg_clients_id s) s
        where s.rn=1;
    end if;

exception
    when others then
        raise exception '(%:%:%)', v_function, v_location, sqlerrm;
end
$$;

-- Инициализирующая загрузка
insert into etl.dim_accounts_hist_id
select *
       ,'05.01.2020'::date date_from
       ,'12.31.2100'::date date_to
from etl.stg_accounts_id;


create function etl.sp_dim_account_hist_id_merge() returns void
    language plpgsql
as
$$
declare
    v_function text := 'sp_dim_account_hist_id_merge';
    v_location text;
begin
    v_location := 'create temporary table tmp_accounts';
    create temporary table tmp_accounts(t_acccode integer
     ,t_accnum varchar(20)
     ,t_accname varchar(200)
     ,t_opendate date
     ,t_closedate date
     ,t_clicode integer
     ,t_hashdiff uuid
     ,t_load_date_time timestamp
     ,t_load_source varchar(20)
     ,date_from date
     ,date_to date
     ,s_acccode integer
     ,s_accnum varchar(20)
     ,s_accname varchar(200)
     ,s_opendate date
     ,s_closedate date
     ,s_clicode integer
     ,s_hashdiff uuid
     ,s_load_date_time timestamp
     ,s_load_source varchar(20)
     ,op_type varchar(1),rn int) on commit drop distributed randomly ;

    v_location := 'insert into tmp_accounts';
    insert into tmp_accounts
    select t.*,s.*
            -- определяем тип операции
            ,case
                when s.hashdiff<>t.hashdiff then 'U'
                when t.acccode is null then 'I'
                when s.acccode is null then 'D'
             end op_type
            ,row_number() over (partition by s.acccode order by s.load_date_time desc) as rn -- ранжируем записи на источнике, чтобы потом исключить дубли
    from dim_accounts_hist_id t -- target
    full outer join stg_accounts_new_id s on s.acccode=t.acccode -- stage
    where t.date_to = '12.31.2100' -- партиция по date_to сокращает скан целевой исторической таблицы
    and (t.hashdiff<>s.hashdiff or t.acccode is null or s.acccode is null);

    v_location := 'delete from dim_accounts_hist_id old values';
    delete from dim_accounts_hist_id t
    using tmp_accounts s
    where t.acccode=s.t_acccode and s.op_type in ('U','D')
    and t.date_to = '12.31.2100'; -- скан только по default партиции

    v_location := 'insert into dim_accounts_hist_id update/delete with date_to';
    -- вставляем новые записи + старые измененные/удаленные записи с закрытым интервалом
    insert into dim_accounts_hist_id
    select case
                when op_type = 'U' then t_acccode
                when op_type = 'I' then s_acccode
                when op_type = 'D' then t_acccode
             end id
            ,case
                when op_type = 'U' then t_accnum
                when op_type = 'I' then s_accnum
                when op_type = 'D' then t_accnum
             end
            ,case
                when op_type = 'U' then t_accname
                when op_type = 'I' then s_accname
                when op_type = 'D' then t_accname
             end
            ,case
                when op_type = 'U' then t_opendate
                when op_type = 'I' then s_opendate
                when op_type = 'D' then t_opendate
             end
            ,case
                when op_type = 'U' then t_closedate
                when op_type = 'I' then s_closedate
                when op_type = 'D' then t_closedate
             end
            ,case
                when op_type = 'U' then t_clicode
                when op_type = 'I' then s_clicode
                when op_type = 'D' then t_clicode
             end
            ,case
                when op_type = 'U' then t_hashdiff
                when op_type = 'I' then s_hashdiff
                when op_type = 'D' then t_hashdiff
             end
            ,case
                when op_type = 'U' then t_load_date_time
                when op_type = 'I' then s_load_date_time
                when op_type = 'D' then t_load_date_time
             end
            ,case
                when op_type = 'U' then t_load_source
                when op_type = 'I' then s_load_source
                when op_type = 'D' then t_load_source
             end
            ,case
                when op_type = 'U' then date_from
                when op_type = 'I' then now()
                when op_type = 'D' then date_from
             end
            ,case
                when op_type = 'U' then now() - interval '1' day
                when op_type = 'I' then '12.31.2100'::date
                when op_type = 'D' then now() - interval '1' day
             end
    from tmp_accounts
    where rn=1;

    v_location := 'insert into dim_accounts_hist_id new values';
    --Делаем еще одну вставку - это уже новая запись для U с открытым интервалом
    insert into dim_accounts_hist_id
    select s_acccode,
           s_accnum,
           s_accname,
           s_opendate,
           s_closedate,
           s_clicode,
           s_hashdiff,
           s_load_date_time,
           s_load_source,
           now(),'12.31.2100'::date
    from tmp_accounts
    where op_type='U' and rn=1;

exception
    when others then
        raise exception '(%:%:%)', v_function, v_location, sqlerrm;

end
$$;

do $$
begin
	--perform sp_dim_client_rnd_full_load('DELETE');
	--perform sp_dim_client_id_full_load('DELETE');
	--perform sp_dim_client_rep_full_load('DELETE');
	--perform sp_dim_account_hist_rnd_merge();
	perform sp_dim_account_hist_id_merge();
end $$ language plpgsql;




############# Лабораторная работа №6 (Обновление витрины) #############
create table etl.dm_provodki_id
(proid integer
 ,operday date
 ,dbacc integer
 ,dbcur varchar(3)
 ,cracc integer
 ,crcur varchar(3)
 ,dbsum decimal(31,10)
 ,crsum decimal(31,10)
 ,purpose varchar(500)
 ,hashdiff uuid
 ,load_date_time timestamp
 ,load_source varchar(20)
 ,dbaccnum varchar(20)
 ,craccnum varchar(20)
 ,dbclifio varchar(100)
 ,crclifio varchar(100)
)
with (appendonly=true , orientation=column, compresstype=zstd, compresslevel=1) distributed by (dbacc)
PARTITION BY RANGE (operday)
( START (date '2020-04-01') INCLUSIVE
END (date '2020-06-01') EXCLUSIVE
EVERY (INTERVAL '1 day') );


create or replace function etl.sp_provodki_operday_load_id(p_operday date) returns void
    language plpgsql
as
$$
declare
    v_function text := 'sp_provodki_operday_load_id';
    v_location text;

begin
    v_location := 'truncate stg_dm_provodki_id';
    truncate stg_dm_provodki_id;

    v_location := 'insert into tmp_provodki';
    insert into stg_dm_provodki_id
    select spr.*
            ,dba.accnum,cra.accnum,dbc.fio,crc.fio
    from stg_provodki_id spr
    inner join dim_accounts_hist_id dba on dba.acccode=spr.dbacc and spr.operday between dba.date_from and dba.date_to
    inner join dim_accounts_hist_id cra on cra.acccode=spr.cracc and spr.operday between cra.date_from and cra.date_to
    inner join dim_clients_id dbc on dbc.clientcode=dba.clicode
    inner join dim_clients_id crc on crc.clientcode=cra.clicode
    where spr.operday = p_operday;

    v_location := 'dm_provodki_id exchange partition';
    execute 'alter table dm_provodki_id exchange partition for ('''|| p_operday ||''') with table stg_dm_provodki_id';

exception
    when others then
        raise exception '(%:%:%)', v_function, v_location, sqlerrm;
end;
$$;


create or replace function etl.sp_provodki_operday_load_rep(p_operday date) returns void
    language plpgsql
as
$$
declare
    v_function text := 'sp_provodki_operday_load_id';
    v_location text;

begin
    v_location := 'truncate stg_dm_provodki_id';
    truncate stg_dm_provodki_id;

    v_location := 'insert into tmp_provodki';
    insert into stg_dm_provodki_id
    select spr.*
            ,dba.accnum,cra.accnum,dbc.fio,crc.fio
    from stg_provodki_id spr
    inner join dim_accounts_hist_id dba on dba.acccode=spr.dbacc and spr.operday between dba.date_from and dba.date_to
    inner join dim_accounts_hist_id cra on cra.acccode=spr.cracc and spr.operday between cra.date_from and cra.date_to
    inner join dim_clients_rep dbc on dbc.clientcode=dba.clicode
    inner join dim_clients_rep crc on crc.clientcode=cra.clicode
    where spr.operday = p_operday;

    v_location := 'dm_provodki_id exchange partition';
    execute 'alter table dm_provodki_id exchange partition for ('''|| p_operday ||''') with table stg_dm_provodki_id';

exception
    when others then
        raise exception '(%:%:%)', v_function, v_location, sqlerrm;
end;
$$;



############# Лабораторная работа №6 (Горячий счет) #############
insert into stg_provodki_id
select num*100,'05.05.2020',813855,'810',2878292,'810',100,100,'hot accounts'
     ,md5(num*100 || '|' || '05.05.2020' || '|' || 1|| '|' || '810' || '|' || 10 || '|' || '810' || '|' || 100 || '|' || 100 || '|' || 'hot accounts')::uuid
    ,now(),'test'
from generate_series(1,800000) num;





Create table stage (id int, part int);
Create table target (id int, part int) partition by list (part) (default partition main);
set gp_enable_exchange_default_partition to 'on'; 
alter table target EXCHANGE DEFAULT PARTITION with table stage;
