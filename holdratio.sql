-- 连接FactHtlOrderSnap，获取isholdroom, freesale
use tmp_htlbidb;
drop table if exists RoomControlModel_dkp_orders_hold;
create table RoomControlModel_dkp_orders_hold as
	select a.masterhotelid, a.effectdate, cast(a.ordroomnum as int) as ordroomnum, b.isholdroom, b.freesale
    from RoomControlModel_orders_sp a, DW_HtlDB.FactHtlOrderSnap b
    where a.orderid = cast(b.orderid as string);

-- 统计masterhotelid+effectdate总effectdate订单数
use tmp_htlbidb;
drop table if exists RoomControlModel_dkp_ordernum_mhtl;
create table RoomControlModel_dkp_ordernum_mhtl as
	select masterhotelid, effectdate, count(1) as ordernum, sum(
    from RoomControlModel_dkp_orders_hold
    group by masterhotelid, effectdate;

-- 统计masterhotelid+effectdate保留effectdate订单数
use tmp_htlbidb;
drop table if exists RoomControlModel_dkp_holdnum_mhtl;
create table RoomControlModel_dkp_holdnum_mhtl as
	select masterhotelid, effectdate, count(1) as holdnum
    from RoomControlModel_dkp_orders_hold
    where isholdroom = 'T' or freesale = 'T'
    group by masterhotelid, effectdate;

use tmp_htlbidb;
drop table if exists RoomControlModel_dkp_holdnum_ordernum;
create table RoomControlModel_dkp_holdnum_ordernum as
	select a.masterhotelid, a.effectdate, b.holdnum, a.ordernum
    from RoomControlModel_dkp_ordernum_mhtl a left join RoomControlModel_dkp_holdnum_mhtl b
    on a.masterhotelid = b.masterhotelid and a.effectdate = b.effectdate;

use tmp_htlbidb;
drop table if exists RoomControlModel_dkp_hold_order;
create table RoomControlModel_dkp_hold_order as
	select masterhotelid, effectdate, if(holdnum is null, 0, holdnum) as holdnum, ordernum
    from RoomControlModel_dkp_holdnum_ordernum;
    
-- drop table RoomControlModel_dkp_ordernum_mhtl;
-- drop table RoomControlModel_dkp_holdnum_mhtl;
-- drop table RoomControlModel_dkp_holdnum_ordernum;


-- 统计masterhotel春节平均保留订单比
use tmp_htlbidb;
drop table if exists RoomControlModel_dkp_holdratio_spring;
create table RoomControlModel_dkp_holdratio_spring as
	select masterhotelid, sum(holdnum)/sum(ordernum) as holdratio
    from RoomControlModel_dkp_hold_order
    where effectdate >= "2016-02-06" and effectdate <= "2016-02-12"
    group by masterhotelid;	
    
use tmp_htlbidb;
drop table if exists RoomControlModel_dkp_holdratio_spring_after;
create table RoomControlModel_dkp_holdratio_spring_after as
	select masterhotelid, sum(holdnum) / sum(ordernum) as holdratio
    from RoomControlModel_dkp_hold_order
    where effectdate >= "2016-03-13" and effectdate <= "2016-03-19" 
    group by masterhotelid;

-- 统计masterhotel清明平均保留订单比
use tmp_htlbidb;
drop table if exists RoomControlModel_dkp_holdratio_qingming;
create table RoomControlModel_dkp_holdratio_qingming as
	select masterhotelid, sum(holdnum) / sum(ordernum) as holdratio
    from RoomControlModel_dkp_hold_order
    where effectdate >= "2016-04-01" and effectdate <= "2016-04-03"
    group by masterhotelid;

use tmp_htlbidb;
drop table if exists RoomControlModel_dkp_holdratio_qingming_after;
create table RoomControlModel_dkp_holdratio_qingming_after as
	select masterhotelid, sum(holdnum) / sum(ordernum) as holdratio
    from RoomControlModel_dkp_hold_order
    where effectdate >= "2016-04-09" and effectdate <= "2016-04-16"
    group by masterhotelid;

-- 统计masterhotel五一平均保留订单比
use tmp_htlbidb;
drop table if exists RoomControlModel_dkp_holdratio_wuyi;
create table RoomControlModel_dkp_holdratio_wuyi as
	select masterhotelid, sum(holdnum) / sum(ordernum) as holdratio
    from RoomControlModel_dkp_hold_order
    where effectdate >= "2016-04-29" and effectdate <= "2016-05-01"
    group by masterhotelid;
    
use tmp_htlbidb;
drop table if exists RoomControlModel_dkp_holdratio_wuyi_after;
create table RoomControlModel_dkp_holdratio_wuyi_after as
	select masterhotelid, sum(holdnum) / sum(ordernum) as holdratio
    from RoomControlModel_dkp_hold_order
    where effectdate >= "2016-05-14" and effectdate <= "2016-05-21"
    group by masterhotelid;

-- 统计masterhotel端午平均保留订单比
use tmp_htlbidb;
drop table if exists RoomControlModel_dkp_holdratio_duanwu;
create table RoomControlModel_dkp_holdratio_duanwu as
	select masterhotelid, sum(holdnum) / sum(ordernum) as holdratio
    from RoomControlModel_dkp_hold_order
    where effectdate >= "2016-06-08" and effectdate <= "2016-06-10"
    group by masterhotelid;

-- 汇总保留订单比
use tmp_htlbidb;
--set hive.mapred.mode = nonstrict;
drop table if exists RoomControlModel_dkp_holdratio;
create table RoomControlModel_dkp_holdratio as
	select a.masterhotelid,
    	a.holdratio as spring,
        b.holdratio as spring_after,
        c.holdratio as qingming,
        d.holdratio as qingming_after,
        e.holdratio as wuyi,
        f.holdratio as wuyi_after,
        g.holdratio as duanwu
    from RoomControlModel_dkp_holdratio_spring a,
    	RoomControlModel_dkp_holdratio_spring_after b,
        RoomControlModel_dkp_holdratio_qingming c,
        RoomControlModel_dkp_holdratio_qingming_after d,
        RoomControlModel_dkp_holdratio_wuyi e,
        RoomControlModel_dkp_holdratio_wuyi_after f,
        RoomControlModel_dkp_holdratio_duanwu g
    where a.masterhotelid = b.masterhotelid 
    and b.masterhotelid = c.masterhotelid
    and c.masterhotelid = d.masterhotelid
    and d.masterhotelid = e.masterhotelid
    and e.masterhotelid = f.masterhotelid 
    and f.masterhotelid  = g.masterhotelid;