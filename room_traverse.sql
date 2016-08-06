--追溯old_roomstatus为空的记录，用最近一条new_roomstatus不为空的来填充

--找出new_roomstatus不为空的记录, 入住日前90天
use tmp_htlbidb;
drop table if exists RoomControlModel_dkp_traceroomstatus_01;
create table RoomControlModel_dkp_traceroomstatus_01 as
select room,effectdate,new_roomstatus,old_roomstatus,operatetime,
row_number() over (distribute by room,effectdate sort by operatetime asc) as rn  -- 按照房态操作时间升序
from dw_htldb.factroominforec
where d>='2016-03-01' and d<'2016-06-03'
and operatetime>=date_sub(effectdate,90) and operatetime<d
and new_roomstatus is not null;
--920355088, 46.4G

-- 找出old_roomstatus is NULL的记录，这部分需要追溯old_roomstatus; 并找出在它之前new_roomstatus不为空的记录
drop table if exists RoomControlModel_dkp_traceroomstatus_02;
create table RoomControlModel_dkp_traceroomstatus_02 as
select oldnull.room,
oldnull.effectdate,
oldnull.new_roomstatus,
oldnull.old_roomstatus,
oldnull.operatetime,
newnotnull.new_roomstatus as new_roomstatus_before,
newnotnull.old_roomstatus as old_roomstatus_before,
newnotnull.operatetime as operatetime_before,
newnotnull.rn
from
(select room,effectdate,new_roomstatus,old_roomstatus,operatetime
from RoomControlModel_dkp_traceroomstatus_01
where old_roomstatus is null) oldnull
left join
tmp_htlbidb.RoomControlModel_dkp_traceroomstatus_01 newnotnull
on oldnull.room=newnotnull.room
and oldnull.effectdate=newnotnull.effectdate
where newnotnull.operatetime<oldnull.operatetime;
--


--取出最近一次操作记录
drop table if exists RoomControlModel_dkp_traceroomstatus_03;
create table RoomControlModel_dkp_traceroomstatus_03 as
select room, effectdate, new_roomstatus, old_roomstatus, operatetime, max(rn) as maxrn
from tmp_htlbidb.RoomControlModel_dkp_traceroomstatus_02
group by room, effectdate, new_roomstatus, old_roomstatus, operatetime;


--用其new_roomstatus填充当前的old_roomstatus，其operatetime作为该次操作的时间
drop table if exists RoomControlModel_dkp_traceroomstatus_04;
create table RoomControlModel_dkp_traceroomstatus_04 as
select obj.room,
obj.effectdate,
obj.operatetime,
trc.new_roomstatus_before as old_roomstatus,    --上一个new_roomstatus作为当前的old_roomstatus
obj.new_roomstatus,
trc.operatetime_before    --上一条记录的操作时间
from tmp_htlbidb.RoomControlModel_dkp_traceroomstatus_03 obj    --old为空的记录对应的最近一条new不为空记录
left join tmp_htlbidb.RoomControlModel_dkp_traceroomstatus_02 trc   --old为空的记录对应的所有new不为空记录
on obj.room=trc.room
and obj.effectdate=trc.effectdate
and obj.maxrn=trc.rn;


-- 把oldroomstatus is NULL和 not NULL的数据合并
drop table if exists RoomControlModel_dkp_traceroomstatus_05;
create table RoomControlModel_dkp_traceroomstatus as
select room,effectdate,operatetime,old_roomstatus,new_roomstatus,NULL as operatetime_before
from dw_htldb.factroominforec
where d>='2016-04-01' and d<'2016-06-03'
and operatetime>=date_sub(effectdate,90) and operatetime<d
and old_roomstatus is not null and new_roomstatus is not null
union all
select room,effectdate,operatetime,old_roomstatus,new_roomstatus,operatetime_before
from tmp_htlbidb.RoomControlModel_dkp_traceroomstatus_04;

drop table if exists RoomControlModel_dkp_traceroomstatus;
create table RoomControlModel_dkp_traceroomstatus as
	select * from RoomControlModel_dkp_traceroomstatus_05
	where old_roomstatus = 'N' or new_roomstatus = 'N';

use tmp_htlbidb;
drop table if exists RoomControlModel_dkp_close_room;
create table RoomControlModel_dkp_close_room as
	select tmp.room, tmp.effectdate, count(1) as closetimes
	from (select * from RoomControlModel_dkp_traceroomstatus where new_roomstatus = 'N') tmp
	group by tmp.room, tmp.effectdate;