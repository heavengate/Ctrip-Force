use tmp_htlbidb;
set hive.mapred.mode=nonstrict;
drop table if exists RoomControlModel_dkp_close_hours;
add file /home/hotelbi/dkp/getCloseTime.py;
create table RoomControlModel_dkp_close_hours as
    select transform(room, effectdate, operatetime, ordd, new_roomstatus)
    using 'getCloseTime.py'
    as room, effectdate, closehours
    from (
    	select room, effectdate, operatetime, ordd, new_roomstatus
        from RoomControlModel_dkp_close
        distribute by room, effectdate
        sort by room, effectdate, operatetime
    ) tmp;

-- 只统计当前room+effectdate
use tmp_htlbidb;
drop table if exists RoomControlModel_dkp_close;
create table RoomControlModel_dkp_close as
	select a.orderid, a.room, a.effectdate, a.ordd, b.operatetime, b.old_roomstatus, b.new_roomstatus
    from RoomControlModel_force_target_sp a, RoomControlModel_zzw_traceroomstatus b
    where a.room = b.room and b.effectdate = a.effectdate and b.operatetime < a.ordd;