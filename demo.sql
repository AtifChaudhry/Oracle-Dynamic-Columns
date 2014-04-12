REM
REM Dynamic Columns (DC) Demo
REM
REM Last Modified: 3/23/2012 12:15 PM
REM 
REM Requires: utldyc.sql
REM

spool demo.log

set echo       on
set pagesize 1000
set linesize  200

col json      for a80
col xml       for a80
col dyncol    for a50
col blob      for a80
col "Has C2?" for a10
col list      for a30
col food      for a15
col color     for a15
col telephone for a15

REM
REM Create an eXtensible EMP table with a blob to hold DC
REM
drop table xEMP;

begin
    execute immediate 
       'create table xEMP ('
    || ' empno             number(4) not null,'
    || ' ename             varchar2(10),'
    || ' job               varchar2(9),'
    || ' mgr               number(4),'
    || ' hiredate          date,'
    || ' sal               number(7,2),'
    || ' comm              number(7,2),'
    || ' deptno            number(2),'
  $if dbms_db_version.ver_le_11_2 $then
    || ' dyncol            varchar2(4000)         /* DC storage */'
  $else
    || ' dyncol            varchar2(4000)        /* DC storage */'
  $end
    || ')';
end;
/

REM
REM Populate the xEMP table with standard EMP rows and 4 DC
REM
insert into xEMP 
  select EMP.*, 
         case when ename != 'SCOTT' then
          COLUMN_CREATE (c1 => rownum,
                         c2 => decode(mod(empno,3), 
                                      0, 'Pancake', 
                                      1, 'Muffin'), 
                         c3 => decode(mod(empno,4), 
                                      0, 'Blue', 
                                      1, 'Green', 
                                      2, 'Yellow'), 
                         c4 => '415-209-' || empno)
         else null end
  from EMP;

REM
REM List the DC in an JSON wrapper
REM
select ename, COLUMN_JSON(dyncol) json from xEMP;

REM
REM Selete the DC using the accessor.
REM
select ename, deptno, 
       COLUMN_GET(dyncol, 3) color, 
       COLUMN_GET(dyncol, 2) food,
       COLUMN_GET(dyncol, 4) telephone
from xEMP;

REM
REM Show all the DC per row and determine which rows have column C2. 
REM
select deptno, ename, 
       COLUMN_LIST(dyncol) list,
       decode(COLUMN_EXISTS(dyncol, 2), 1, 'Yes', 'No') "Has C2?"
from xEMP order by deptno;

REM
REM Remove the DC 2 for employees in department 10
REM
update xEMP set dyncol = COLUMN_DELETE(dyncol, 2) 
where deptno = 10;

select deptno, ename, COLUMN_LIST(dyncol) list,
       decode(COLUMN_EXISTS(dyncol, 2), 1, 'Yes', 'No') "Has C2?"
from xEMP order by deptno;
commit;

REM
REM Adding a DC will insert one if column not present else update existing value
REM
update xEMP set dyncol = COLUMN_ADD(dyncol, 2, 'Pizza') 
where deptno in (10, 20);

select deptno, ename, COLUMN_GET(dyncol, 2) food
from xEMP order by deptno;
commit;

REM
REM Removing the last DC makes the column null, but adding a DC make it not null.
REM
select deptno, ename, COLUMN_LIST(dyncol) list from xEMP order by deptno;

update xEMP set dyncol = COLUMN_DELETE(dyncol, 2) where ename = 'SCOTT';

select deptno, ename, COLUMN_LIST(dyncol) list from xEMP order by deptno;

update xEMP set dyncol = COLUMN_ADD(dyncol, 27, 'Flying' ) where ename = 'SCOTT';

select deptno, ename, COLUMN_LIST(dyncol) list from xEMP order by deptno;
commit;

REM
REM Use Function Indexes to create secondary indexes on specific DC column
REM
create index xEMP_food on xEMP(COLUMN_GET(dyncol, 2));

explain plan for 
  select /*+ use_index(e) */ ename from xEMP e 
  where COLUMN_GET(dyncol, 2) = 'Pizza';
set echo off
@?/rdbms/admin/utlxpls
set echo on

REM
REM List the DC in an XML wrapper
REM
select ename, COLUMN_XML(dyncol) xml  from xEMP;

REM
REM Show the DC's internal representation (for interpretation see implementation)
REM
select ename, dyncol from xEMP;

spool off
exit
