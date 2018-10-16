--Day 2
truncate table employee 

drop index emp_load_eid_ncidx on emp_load

create index emp_load_ename_idx on emp_load(ename)

set statistics io on
select * from emp_load where ename='a1'

create clustered index emp_load_eid_cidx on
employee(eid)

CREATE DATABASE DB3
USE DB3
GO

CREATE TABLE emp_load (EID INT,  NAME VARCHAR(200), SALARY INT, DEPARTMENT_ID INT)

--composite clustered index
create index emp_load_eid_ename_idx on emp_load(eid, ename)

select * from emp_load where eid = 1 -- index seek + rid lookup
select * from emp_load where eid = 1 and ename = 'a1' -- index seek + rid lookup
select * from emp_load where ename = 'a1' -- index scan + rid lookup
select * from emp_load where eid = 1 or ename = 'a1' -- index seek + index scan + rid lookup

drop table emp_load

CREATE TABLE emp_load
	(eid int,
	ename varchar(100),
	salary int,
	deptid int,
	c1 char(1000),
	c2 char(1000))

create procedure sp_loadEmp1(@rec int)
as
set nocount on
declare @i int
set @i = 1
begin tran
truncate table EMPLOYEE
while (@i <= @rec)
begin
insert into emp_load values(@i, concat(char((@i-1)%26 + 97), @i), rand() * 100000,
 ((@i-1)%3 + 1), 'test', 'test')
if(@i%50000)=0
begin
commit
begin tran
end
set @i = @i+1
end
commit

create clustered index emp_load_eid_cidx
on emp_load(eid)

create index emp_load_ename_idx on emp_load(ename)

--index with inlcude 
drop index emp_load_ename_idx on emp_load
create index emp_load_ename_idx on emp_load(ename) include(salary)

set statistics io on
select eid, ename, salary from emp_load where ename like 'a1234%'

--unique index
--cannot have duplicate values values can be created only on duplicate

drop index emp_load_ename_idx on emp_load

--order by eid, ename
--no sort order by eid desc, ename desc
--sort order by eid desc, ename asc

--view does not optimize performance.
truncate table emp_load

drop index emp_load_eid_cidx on emp_load

set statistics io off

exec sp_loadEmp1 2000000

CREATE TABLE dept (deptid INT,  deptname VARCHAR(200))
INSERT INTO dept VALUES (1, 'HR');
INSERT INTO dept VALUES (2, 'IT');
INSERT INTO dept VALUES (3, 'FINANCE');

set statistics io on
select * from(
select 
deptname, ename, salary,
row_number() over(partition by deptname
order by salary desc) rn
from 
emp_load e join dept d
on e.deptid=d.deptid) as t where rn <= 5

create index emp_load_idx
on emp_load(deptid, salary desc)
include (ename)

--performing join after filter of 15 rows and added include ename as ename in select query
select deptname, t1.* from(
select * from(
select 
deptid, ename, salary,
row_number() over(partition by e.deptid
order by salary desc) rn
from 
emp_load e
) as t where rn <= 5
) as t1
join dept d
on t1.deptid=d.deptid

drop index emp_load_idx on emp_load
set statistics io off
--filtered index 
create index emp_load_eid_idx 
on emp_load
(eid) where deptid=1

select * from emp_load where eid=1 -- table scan
select eid from emp_load where eid=1 -- table scan
set statistics io on 
select * from emp_load where eid=1 and deptid=1 -- index seek

set statistics io off 
create clustered index emp_load_eid_cidx 
on emp_load
(eid)
--page split can occur at all levels
-- optimise inserts due to page split
-- or you can use fill factor
drop index emp_load_eid_cidx on emp_load

USE DB2
GO
truncate table [dbo].[emp_load]
truncate table [dbo].[EMPLOYEE]

create table primkey_test (eid int primary key)

create table primkey_test (eid int primary key nonclustered)

create table unique_test (eid int unique clustered)

drop index emp_load_eid_idx on emp_load
--columnstore index
create clustered columnstore index
emp_load_col_idx on emp_load

drop index emp_load_ename_idx on emp_load

select sum(cast(eid as bigint)) from emp_load

insert into emp_load values (465123, 'asdfsd', 452, 2, 'test', 'test')
select count(eid) from emp_load

--compression
select object_name(object_id),
data_compression_desc
from sys.partitions 



alter table emp_load
rebuild with (data_compression=page)

alter table emp_load
rebuild with (data_compression=row)

alter table emp_load
rebuild with (data_compression=none)

--input parameter
--Procedures
create procedure prime_number(@number int)
as
begin
set nocount on
declare @bound int = @number / 2;
declare @i int = 2
while(@i <= @bound)
begin
 if(@number % @i = 0)
 begin
	print Concat(@number, ' is not a prime number')
	return
 end
 set @i=@i + 1;
end
	print Concat(@number, ' is a prime number')
end

exec prime_number 9

--take eid and return dept name
create procedure sp_getDept(@id int)
as
begin
declare @deptname varchar(100)
select @deptname = deptname
from emp_load join dept
on emp_load.deptid = dept.deptid
where eid=@id
print @deptname
end

exec sp_getDept 1
set statistics io off

truncate table emp_load
exec [dbo].[sp_loadEmp1] 200

alter procedure getempnames(@deptname varchar(100))
as
begin
declare @temp varchar(100) = ''
	select @temp = concat(@temp, ',', ename) from emp_load join dept on emp_load.deptid = dept.deptid where deptname = @deptname
	print @temp
end

exec getempnames 'HR'

select concat(ename) from emp_load join dept on emp_load.deptid = dept.deptid
select * from emp_load

select * from information_schema.tables

alter procedure gettables(@dbname varchar(100))
as
begin
declare @temp varchar(100)
declare @count varchar(100)
select @temp = TABLE_NAME from information_schema.tables where TABLE_CATALOG = @dbname
select @count =  count(*) from @temp
end

exec gettables 'DB3'

drop index emp_load_col_idx on emp_load
-- CURSORS
--Msg 35370, Level 16, State 1, Procedure sp_getname, Line 5 [Batch Start Line 248]
--Cursors are not supported on a table which has a clustered columnstore index.
create procedure sp_getname
as
begin
declare c1 cursor
for select ename from emp_load
declare @ename varchar(100)
open c1
fetch next from c1 into @ename
while(@@FETCH_STATUS =0)
begin
print @ename
fetch next from c1 into @ename
end
close c1
deallocate c1
end

exec sp_getname

alter procedure sp_gettabcount
as
begin
	create table ##tabcount (name varchar(100), cnt int)
	declare c1 cursor
	for
	select table_name from information_schema.tables
	declare @table_name varchar(100) 
	open c1
	fetch next from c1 into @table_name
	while(@@FETCH_STATUS =0)
	begin
	execute('insert into ##tabcount
	select ''' + @table_name + ''', count(*) from ' + @table_name)
	fetch next from c1 into @table_name
	print @table_name
	fetch next from c1 into @table_name
	end
	select * from ##tabcount
	drop table ##tabcount
	close c1
	deallocate c1
end

--use execute to generate dynamic query -> runs on its own child thread
exec sp_gettabcount

create type emptype
	as table (ename varchar(100))

create procedure
sp_getdeptname(@enamelist emptype readonly)
as
begin
select * from @enamelist
end

--table value parameters tvp
declare @emptype emptype
insert into @emptype values('a1'), ('b2')
exec sp_getdeptname @emptype


--default -  scroll is on in cursor, you can disable the cursor
--with encryption - view or procedure
sp_helptext [view_name]


--Functions
--Scalar Function

create function fn_prime_number(@number int)
returns varchar(100)
as
begin
declare @bound int = @number / 2;
declare @i int = 2
while(@i <= @bound)
begin
 if(@number % @i = 0)
 begin
	return (Concat(@number, ' is not a prime number'))
 end
 set @i=@i + 1;
end
	return (Concat(@number, ' is a prime number'))
end

select dbo.fn_prime_number(9)

alter function fn_getempnames(@deptname varchar(100))
returns varchar(100)
as
begin
declare @temp varchar(100) = ''
	select @temp = concat(@temp, ',', ename) from emp_load join dept on emp_load.deptid = dept.deptid where deptname = @deptname
	return  (substring(@temp, 2, 999))
end

select deptname, dbo.fn_getempnames(deptname) from dept

--write a function that takes params, word and sentence, find occurrence of a word in a sentence
create function fn_findwordoccurrence(@word varchar(100), @sentence varchar(1000))
returns int
as
begin
	
end

--scalar function - get return one value

--table valued function

create function sf_returntab(@deptname varchar(100))
returns table
as 
return(select ename, salary, deptname
from emp_load join dept
on emp_load.deptid=dept.deptid
where deptname=@deptname)

select * from dbo.sf_returntab('HR')

--multi valued function
create function sf_multitab(@deptname varchar(100))
returns @tab table (
ename varchar(100),
salary int,
deptname varchar(100))
as
begin
	insert into @tab
		select ename, salary, deptname
	from emp_load join dept
	on emp_load.deptid=dept.deptid
	where deptname=@deptname
return
end

select * from dbo.sf_multitab('HR')

--split sentence into words
create function sf_splitsentence(@sentence varchar(100), @delimeter varchar(5))
returns @tab table (
word varchar(100))
as
begin
declare @tempword varchar(100) = '', @i int=1
	select @tempword='', @i = 1
	while(@i<=len(@sentence))
	begin
		if SUBSTRING(@sentence, @i, 1) = @delimeter
		begin
			insert into @tab values (@tempword)
		end
		else
		set 
	set @i = @i +1
	end
	return
end

--TRIGGERS

select * from emp_load

create table emp_load_audit
(update_time dateTime2,
update_by varchar(100),
command varchar(100),
old_eid int,
new_eid int,
old_ename varchar(100),
new_ename varchar(100),
old_salary int,
new_salary int,
old_deptid int,
new_deptid int,
old_c1 char(100),
new_c1 char(100),
old_c2 char(100),
new_c2 char(100))

--ctrl + shift + R
--
GO

create trigger trigger_emp_load_ins
on emp_load 
after insert
as
begin
INSERT INTO [dbo].[emp_load_audit]
           ([update_time]
           ,[update_by]
           ,[command]
           ,[old_eid]
           ,[new_eid]
           ,[old_ename]
           ,[new_ename]
           ,[old_salary]
           ,[new_salary]
           ,[old_deptid]
           ,[new_deptid]
           ,[old_c1]
           ,[new_c1]
           ,[old_c2]
           ,[new_c2])
     
	 (select 
	 GETDATE(),
	SUSER_SNAME(),
	'INSERT',
	null,
	eid,
	null,
	ename,
	null,
	salary,
	null,
	deptid,
	null, 
	c1,
	null,
	c2 from inserted) 
end

exec sp_loadEmp1 100


begin tran

select * from [dbo].[emp_load_audit]

rollback

--In memory tables

create table test_data(id int, name varchar(100))

begin tran
update test_data set name = 'y' where id = 1

--blocking - check blocking
select * from sys.sysprocesses
where blocked != 0

dbcc inputbuffer(59)

select * from test_data with (nolock)
--get isoloation level
dbcc useroptions

-- change the isolation leve to snapshot to get last cimmited ldata

alter database db3 
set allow_snapshot_isolation on

alter database db3 
set read_committed_snapshot on

--when dead lock occurs one process gets rolled back
begin tran
update test_data set name = 'x' where id=2
update test_data set name = 'x' where id=1

rollback

create table 
--take locks in same order to avoid dead locks
--enable trace flag  
dbcc traceon(-1, 1222) --global set
dbcc tracestatus
--tools sql server profiler

--Dead lock

create table dl1(id int)
create table dl2(id int)

insert into dl2 (id) values (1)
insert into dl1 (id) values (2)
insert into dl1 (id) values (1)
insert into dl2 (id) values (2)

begin tran
update dl1 set id = 10 where id = 1
update dl2 set id = 20 where id = 20

rollback

GO
--In Memory Table
create table [customer]
([customerId] int not null
primary key nonclustered
hash with (bucket_count = 1000000),
[name] nvarchar(250) not null,
[customerSince] datetime null
)
with 
(memory_optimized = on, durability = Schema_only);--schema_and_data

insert into customer values (1, 'a', getdate())

select * from customer

CREATE TABLE emp (eid INT,  ename VARCHAR(200), salary INT, deptid INT)

alter procedure sp_loademp_normal(@rec int)
as
begin
set nocount on
declare @i int
truncate table emp
set @i = 1
while(@i <= @rec)
begin
insert into emp
values(@i, concat('a', @i), rand() * 10000, ((@i-1)%3)+1)
set @i=@i+1
end
commit
end

create table emp_mem
(eid int primary key nonclustered hash with
(bucket_count = 5000000),
ename varchar(100),
salary int,
deptid int)
with (memory_optimized = on, durability = schema_and_data);--schema_and_data

create procedure sp_loademp_mem(@rec int)
with native_compilation, schemabinding, execute as owner
as
begin atomic with (transaction isolation level = snapshot, language = 'us_english')
declare @i int
set @i = 1
while(@i <= @rec)
begin
insert into dbo.emp_mem
values(@i, 'a' + cast(@i as varchar(100)), rand() * 10000, ((@i-1)%3)+1)
set @i=@i+1
end
end

exec sp_loademp_mem 1000000

exec sp_loademp_normal 1000000

--limitations of native compilation and in memory tables

USE master ;  
GO  
DROP DATABASE DB2
GO  

--CTE common table expression
with tab as(select ename from emp)
select * from tab

with tab(id)
as
(select 1 id
union all
select id +1 from tab
where id<100)
select * from tab

create table email
	(email varchar(100))

	insert into email(email) values ('demo1@demo.com');
	insert into email(email) values ('demo1@demo.com');
	insert into email(email) values ('demo1@demo.com');
	insert into email(email) values ('demo2@demo.com');
	insert into email(email) values ('demo2@demo.com');

	select * from email

with CTE
as(Select email, row_number() over (partition by email order by email) 
as emailNumber from email)
--Select * from CTE
delete from CTE where emailNumber >1

select * from email

create table emp_pk (eid int primary key, ename varchar(100))

alter procedure sp_insemp(@eid int, @ename varchar(10))
as
begin try
insert into emp_pk values(@eid, @ename)
end try
begin catch
print 'cannot insert duplicate values'
select ERROR_MESSAGE()
end catch

exec sp_insemp 1, 'a'
exec sp_insemp 1, 'a'

select ISNUMERIC('123')

select isDate('123')

select ISJSON('{"name":}')

select * from (select top 10 * from emp) as t for json auto
[{"eid":1,"ename":"a1","salary":1452,"deptid":1},{"eid":2,"ename":"a2","salary":4975,"deptid":2},{"eid":3,"ename":"a3","salary":8041,"deptid":3},{"eid":4,"ename":"a4","salary":1680,"deptid":1},{"eid":5,"ename":"a5","salary":507,"deptid":2},{"eid":6,"ename":"a6","salary":2940,"deptid":3},{"eid":7,"ename":"a7","salary":6759,"deptid":1},{"eid":8,"ename":"a8","salary":2735,"deptid":2},{"eid":9,"ename":"a9","salary":3222,"deptid":3},{"eid":10,"ename":"a10","salary":3210,"deptid":1}]

select ISJSON('[{"eid":1,"ename":"a1","salary":1452,"deptid":1},{"eid":2,"ename":"a2","salary":4975,"deptid":2},{"eid":3,"ename":"a3","salary":8041,"deptid":3},{"eid":4,"ename":"a4","salary":1680,"deptid":1},{"eid":5,"ename":"a5","salary":507,"deptid":2},{"eid":6,"ename":"a6","salary":2940,"deptid":3},{"eid":7,"ename":"a7","salary":6759,"deptid":1},{"eid":8,"ename":"a8","salary":2735,"deptid":2},{"eid":9,"ename":"a9","salary":3222,"deptid":3},{"eid":10,"ename":"a10","salary":3210,"deptid":1}]')

select json_value('[{"eid":1,"ename":"a1","salary":1452,"deptid":1},{"eid":2,"ename":"a2","salary":4975,"deptid":2},{"eid":3,"ename":"a3","salary":8041,"deptid":3},{"eid":4,"ename":"a4","salary":1680,"deptid":1},{"eid":5,"ename":"a5","salary":507,"deptid":2},{"eid":6,"ename":"a6","salary":2940,"deptid":3},{"eid":7,"ename":"a7","salary":6759,"deptid":1},{"eid":8,"ename":"a8","salary":2735,"deptid":2},{"eid":9,"ename":"a9","salary":3222,"deptid":3},{"eid":10,"ename":"a10","salary":3210,"deptid":1}]', '$[1].ename')

--modes lax, strict
select json_value('{"eid":1,"ename":"a1","salary":1452,"deptid":1}', 'strict $.name')

select * from openrowset -- eimporting excel file

select * from openjson('[{"eid":1,"ename":"a1","salary":1452,"deptid":1},{"eid":2,"ename":"a2","salary":4975,"deptid":2},{"eid":3,"ename":"a3","salary":8041,"deptid":3},{"eid":4,"ename":"a4","salary":1680,"deptid":1},{"eid":5,"ename":"a5","salary":507,"deptid":2},{"eid":6,"ename":"a6","salary":2940,"deptid":3},{"eid":7,"ename":"a7","salary":6759,"deptid":1},{"eid":8,"ename":"a8","salary":2735,"deptid":2},{"eid":9,"ename":"a9","salary":3222,"deptid":3},{"eid":10,"ename":"a10","salary":3210,"deptid":1}]')
with
(eid int '$.eid',
ename varchar(100) '$.ename',
salary int '$.salary',
deptid int '$.deptid')

select JSON_QUERY('[{"eid":1,"ename":"a1","salary":1452,"deptid":1},{"eid":2,"ename":"a2","salary":4975,"deptid":2},{"eid":3,"ename":"a3","salary":8041,"deptid":3},{"eid":4,"ename":"a4","salary":1680,"deptid":1},{"eid":5,"ename":"a5","salary":507,"deptid":2},{"eid":6,"ename":"a6","salary":2940,"deptid":3},{"eid":7,"ename":"a7","salary":6759,"deptid":1},{"eid":8,"ename":"a8","salary":2735,"deptid":2},{"eid":9,"ename":"a9","salary":3222,"deptid":3},{"eid":10,"ename":"a10","salary":3210,"deptid":1}]', '$[0]')

select JSON_MODIFY('{"name": "ABC"}', '$.name', 'xyz')

select JSON_MODIFY('["name"]', 'append $', 'xyz')

create table t_json (id int, data nvarchar(max) check (isjson(data)=1))
--blob - 2gb

openrowset
sp_configure 'show advanced '


