USE DB2
CREATE TABLE DEPAERMENT (ID INT,  NAME VARCHAR(200))
INSERT INTO DEPAERMENT VALUES (1, 'HR');
INSERT INTO DEPAERMENT VALUES (2, 'IT');
INSERT INTO DEPAERMENT VALUES (3, 'FINANCE');

--Multi row insert statement
insert into DEPAERMENT values (1, 'HR'), (2, 'IT'), (3, 'FINANCE')

CREATE TABLE EMPLOYEE (EID INT,  NAME VARCHAR(200), SALARY INT, DEPARTMENT_ID INT)

create procedure sp_loadEmp(@rec int)
as
set nocount on
declare @i int
set @i = 1
begin tran
truncate table EMPLOYEE
while (@i <= @rec)
begin
insert into EMPLOYEE values(@i, concat(char((@i-1)%26 + 97), @i), rand() * 100000, ((@i-1)%3 + 1))
if(@i%50000)=0
begin
commit
begin tran
end
set @i = @i+1
end
commit


exec sp_loadEmp 10000

SELECT * FROM EMPLOYEE

--SELECT @@TRANCOUNT

--UNIQUE
CREATE TABLE T1_UQ (C1 INT, C2 INT, C3 varchar(50), UNIQUE (C1, C2))

INSERT INTO T1_UQ VALUES (NULL, NULL, 'a'), (1, NULL, 'b'), (2, NULL, 'c')

INSERT INTO T1_UQ VALUES (NULL, NULL, 'a')

SELECT * FROM T1_UQ

--CHECK CONSTRAINT

create table t_sal
(id int, sal int check(sal>0))

insert into t_sal values (1, NULL)

create table t_pan
(id int, pan char(10) check(pan like '[a-z][a-z][a-z][a-z][a-z][0-9][0-9][0-9][0-9][a-z]'))

insert into t_pan values (1, 'sdfsd1234r')


create table t_parent(c1 int unique,  c2 int)
create table t_child(c1 int references t_parent(c1),  c3 int)

insert into t_child values (null, 1)
insert into t_child values (1, 1)

create table t_parent_comp(c1 int,  c2 int, c3 int, primary key(c1, c2))

create table t_child_comp(c1 int, c2 int, c4 int, foreign key (c1, c2) references t_parent_comp(c1, c2))

dept name, emp name, salary of top 5

SELECT top 5 e.salary, d.NAME, e.name from employee e, DEPAERMENT d where e.DEPARTMENT_ID = d.id ORDER by salary desc
-- TOP 5 Salaries
SELECT top 5 e.salary, d.NAME, e.name 
	from employee e join DEPAERMENT d 
	on e.DEPARTMENT_ID = d.id 
	ORDER by salary desc

	SELECT top 5 e.salary, DISTINCT d.NAME, e.name 
	from employee e join DEPAERMENT d 
	on e.DEPARTMENT_ID = d.id 
	ORDER by salary desc
	GROUP BY d.NAME


	SELECT TOP ROW_NUMBER() OVER ()

	--find duplicate salaries
	SELECT salary, count(*)
	from employee e join DEPAERMENT d 
	on e.DEPARTMENT_ID = d.id 
	having count

	--
	-- TOP 5 Salaries of each department
	select DEPAERMENT.name, EMPLOYEE.name, salary,
	row_number() over(order by salary desc) rn,
	rank() over(order by salary desc) rnk,
	dense_rank() over(order by salary desc) drnk
	from EMPLOYEE join DEPAERMENT on EMPLOYEE.DEPARTMENT_ID =  DEPAERMENT.ID

	select * from (select DEPAERMENT.name, EMPLOYEE.name, salary,
	row_number() over(partition by DEPAERMENT.name order by salary desc) rn
	from EMPLOYEE join DEPAERMENT on EMPLOYEE.DEPARTMENT_ID =  DEPAERMENT.ID) as t where rn < 6

	SELECT name, salary, row_number() over(order by eid desc) as rn, sum(salary, 
	select salary from employee where ROW_NUMBER = rn + 1)from employee order by salary desc 


	select *,t.salary + (select isnull(sum(t.salary),0) from employee where eid>t.eid  ) as employee from employee t

	select name, salary
	(select sum(cast(salary as bigint)) from emp i)

	--Running TOTAL
	select name, salary,
	(select sum(cast(salary as bigint)) from employee i
	where i.salary > o.salary or (i.SALARY = o.salary and i.eid <= o.eid))
	runnin_total
	from employee o
	order by salary desc, eid


	select name, salary, sum(cast(salary as bigint)) over(
	order by salary desc
	row between unbounded preceding and current row)
	 runing_total
	from EMPLOYEE

	select name, salary, sum(cast(salary as bigint)) over(
	order by salary desc
	rows between 1 following and 1 following)
	 next_high
	from EMPLOYEE

	select name, salary,
	lead(salary, 1)
	over(order by salary desc)
	from EMPLOYEE

	select name, salary,
	lead(salary, 1, 0)
	over(order by salary desc),
	lag(salary, 1, 0)
	over(order by salary desc)
	from EMPLOYEE

	select DISTINCT DEPAERMENT.name, sum(salary) over (partition by DEPAERMENT.name) 
	from EMPLOYEE join DEPAERMENT on EMPLOYEE.DEPARTMENT_ID = DEPAERMENT.ID

	select
	sum(case
	when DEPAERMENT.name ='HR' then cast(salary as bigint)
	else null end) HR
	sum(case
	when DEPAERMENT.name ='FINANCE' then cast(salary as bigint)
	else null end) FINANCE
	sum(case
	when DEPAERMENT.name ='IT' then cast(salary as bigint)
	else null end) HR
	from EMPLOYEE join DEPAERMENT on EMPLOYEE.DEPARTMENT_ID = DEPAERMENT.ID


	select * from(
	select DEPAERMENT.NAME, cast(salary as bigint) salary
	from
	employee join DEPAERMENT
	on employee.DEPARTMENT_ID = DEPAERMENT.ID)
	as t pivot (sum(salary) for DEPAERMENT.NAME in (HR, Finance, IT)) as p 

	select * from(select DEPAERMENT.name, sum(salary) over (partition by DEPAERMENT.name) 
	from EMPLOYEE join DEPAERMENT on EMPLOYEE.DEPARTMENT_ID = DEPAERMENT.ID)

	as t pivot (sum(salary) for DEPAERMENT.NAME in (HR, Finance, IT)) as p 

	--temp tables local and global

	create table ##tmp (id int, name varchar(200))

	insert into #tmp values(1, 'a'), (3, 'b')

	SELECT * FROM #tmp

	select @@SPID

	--Temporal tables
	create table dbo.testtemporal
	(id int primary key,
	a int,
	b int,
	systemstarttime datetime2 generated
	always as row start not null,
	systemendtime datetime2 generated
	always as row end not null,
	period for system_time(
	systemstarttime, systemendtime))
	with (system_versioning=on)
	--history_table=dbo.testtemporal_history

	insert into dbo.testtemporal (id, a, b) values (1, 1, 1)
	update dbo.testtemporal set a = 2

	SELECT * FROM [dbo].[MSSQL_TemporalHistoryFor_1477580302]

	alter table  dbo.testtemporal
	set(system_versioning=off)

	drop table dbo.testtemporal

	create table dbo.testtemporal
	(id int primary key,
	a int,
	b int,
	systemstarttime datetime2 generated
	always as row start not null,
	systemendtime datetime2 generated
	always as row end not null,
	period for system_time(
	systemstarttime, systemendtime))
	with (system_versioning=on
	(history_table=dbo.testtemporal_history))

	insert into dbo.testtemporal (id, a, b) values (1, 1, 1)

	truncate table dbo.testtemporal

	--Views CODD
	create view view_ename_deptname 
	as
	select EMPLOYEE.name as ename, DEPAERMENT.name dname
	from EMPLOYEE join DEPAERMENT
	on EMPLOYEE.DEPARTMENT_ID = DEPAERMENT.ID

	select * from view_ename_deptname
	update view_ename_deptname set ename = 'aa1' where ename = 'a1'
	delete from view_ename_deptname

	--Schema binding with view
	create view view_ename_deptname 
	with schemabinding
	as
	select EMPLOYEE.name as ename, DEPAERMENT.name dname
	from EMPLOYEE join DEPAERMENT
	on EMPLOYEE.DEPARTMENT_ID = DEPAERMENT.ID
	


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

exec sp_loadEmp1 5000000

set statistics io on

select * from  emp_load where eid = 99123 option(maxdop 1)

select eid from  emp_load where eid = 99123 -- no rid lookup

select * from  emp_load where eid in (1, 30, 31)

select * from  emp_load where eid <= 100

select eid from  emp_load --index scan not table scan

--index seek, index scan, table scan

--selectivity & cardinality

CREATE NONCLUSTERED INDEX IX_EID   
    ON emp_load (eid);   
GO  

CREATE INDEX emp_load_eid_ncdx   
    ON emp_load (eid);   
GO  
