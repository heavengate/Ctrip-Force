--取1.1~5.15的所有订单
use tmp_htlbidb;
drop table if exists RoomControlModel_dkp_orders_all;
set mapred.reduce.tasks=300;
create table RoomControlModel_dkp_orders_all as
select t3.*,spo.shadowid,spo.isforceorder,spo.agentname,
case when spo.agentname='亿程旅行社' then '预付'
when spo.agentname='叮丁旅行社' then '高返'
when spo.agentname='松果网' then '松果'
when spo.agentname='代理' then '代理'
when spo.agentname is null then null else '现付' end as shadow_type,
ccl.canceltime
from
(
	select t2.*,
		IF(dbr.masterbasicroomid<=0 or dbr.masterbasicroomid is null, dbr.basicroomtypeid, dbr.masterbasicroomid) as masterbasicroomid
	from
	(
		select t1.*,dh.hotelbelongto,
			IF(dh.masterhotelid<=0 or dh.masterhotelid is null, dh.hotel, dh.masterhotelid) as masterhotelid
		from
		(
			select fho.*,dr.hotel,
				IF(dr.basicroomtypeid<=0 or dr.basicroomtypeid is null, dr.room, dr.basicroomtypeid) as basicroomtypeid
			from
			(
				select orderid,
					orderdate,
					room,
					to_date(arrival) as checkindate,
					to_date(etd) as etd,
					ordroomnum,
					ordadvanceday,
					orderstatus,
					cancelreason,
					IF(cancelreason='NOROOM',1,0) as isnoroom,     --是否满房
					d as ordd,
					balancetype,
					datediff(etd,arrival) as ord_days,
					hour(orderdate) as ord_hour,     --预订时刻
					isholdroom,isstraightconnect,freesale
				from DW_HtlDB.FactHtlOrderSnap
				where orderdate>='2016-01-01' and orderdate<'2016-05-16' and arrival<'2016-05-18' and arrival<etd
				and channelid=4 and istestaccount=0 and referencetype<>'M' and cancelreason<>'DBLORDER'
			) fho
			left join Dim_HtlDB.dimroom dr on fho.room=dr.room
		) t1
		left join Dim_HtlDB.dimhtlhotel dh on t1.hotel=dh.hotel
	) t2
	left join Dim_HtlDB.dimbasicroomtype dbr on t2.basicroomtypeid=dbr.basicroomtypeid
) t3
left join (select * from ods_htl_OrderDB.Ord_ShadowPriceOrder where d='2016-05-16') spo
on t3.orderid=spo.orderid
left join (select * from ods_htl_orderdb.ord_cancelorder where d='2016-06-01') ccl
on t3.orderid=ccl.orderid;
--shadowid is not null为马甲订单
--select distinct balancetype from RoomControlModel_dkp_orders_all where orderdate>='2016-03-01';
--PP FG
--select count(1) from RoomControlModel_dkp_orders_all where orderdate>='2016-03-01';
--19107766
--select count(1) from RoomControlModel_dkp_orders_all where orderdate>='2016-03-01' and isforceorder='T';
--609988
--select count(1) from RoomControlModel_dkp_orders_all where etd<=checkindate;  之前有287单，现已增加条件arrival<etd过滤


--房型+支付类型列表，取该房型出现次数最多的支付类型
--每个房型+支付类型出现的次数，并按出现次数排序
drop table if exists RoomControlModel_dkp_room_balancetype;
create table RoomControlModel_dkp_room_balancetype as
select room,balancetype from
(
	select *,
	row_number() over (partition by room order by cnt_ord desc) as rk
	from
	(
		select room,balancetype,count(1) as cnt_ord
		from RoomControlModel_dkp_orders_all
		group by room,balancetype
	) t1
) t2
where rk=1;


--对现改预出现的预付修正为现付
drop table if exists RoomControlModel_dkp_orders_ppmodify;
create table RoomControlModel_dkp_orders_ppmodify as
select a.orderid,a.orderdate,a.ordd,a.room,b.balancetype,a.checkindate,a.etd,a.ordroomnum,a.ordadvanceday,
a.orderstatus,a.cancelreason,a.isnoroom,a.ord_days,a.ord_hour,a.hotel,a.basicroomtypeid,a.hotelbelongto,
a.masterhotelid,a.masterbasicroomid,a.shadowid,a.isforceorder,a.agentname,a.shadow_type,a.canceltime,
a.isholdroom,a.isstraightconnect,a.freesale
from RoomControlModel_dkp_orders_all a
inner join RoomControlModel_dkp_room_balancetype b
on a.room=b.room;
--select count(1) from (select room,count(distinct balancetype) as cnt from RoomControlModel_dkp_orders_ppmodify group by room) tt where cnt>1;


--所有订单按effectdate拆开
drop table if exists RoomControlModel_dkp_orders_sp;
set hive.mapred.mode=nonstrict;
add file /home/hotelbi/dkp/checkindate_split.py;
create table RoomControlModel_dkp_orders_sp as
select transform(orderid,orderdate,ordd,room,hotel,checkindate,etd,ordroomnum,ordadvanceday,ord_days,orderstatus,cancelreason,isnoroom,canceltime,
	balancetype,ord_hour,masterhotelid,basicroomtypeid,masterbasicroomid,hotelbelongto,shadowid,isforceorder,agentname,shadow_type)
USING  'python checkindate_split.py'
as  orderid,effectdate,orderdate,ordd,room,hotel,checkindate,etd,ordroomnum,ordadvanceday,ord_days,orderstatus,cancelreason,isnoroom,canceltime,
	balancetype,ord_hour,masterhotelid,basicroomtypeid,masterbasicroomid,hotelbelongto,shadowid,isforceorder,agentname,shadow_type
from RoomControlModel_dkp_orders_ppmodify;


drop table RoomControlModel_dkp_force_resttime;
create table RoomControlModel_dkp_force_resttime as
select *,(unix_timestamp(deadline)-unix_timestamp(orderdate))/3600 as rest_time
from
(select orderid,orderdate,room,balancetype,masterbasicroomid,masterhotelid,isnoroom,
shadowid,isforceorder,checkindate,
from_unixtime(unix_timestamp(checkindate,'yyyy-MM-dd')+86399) as deadline
from RoomControlModel_dkp_orders_ppmodify
) tt;


--强下订单，这是预测目标
drop table if exists RoomControlModel_dkp_force_target_sp;
create table RoomControlModel_dkp_force_target_sp as
select * from RoomControlModel_dkp_orders_sp
where isforceorder='T' and orderdate>='2016-03-01' and ord_days=1
and effectdate>=ordd and effectdate<date_add(ordd,3);


--目标强下订单最近30天同母酒店的所有订单(当前下单时刻之前, 拆分)
drop table if exists RoomControlModel_dkp_mhotel_orders_efdt;
create table RoomControlModel_dkp_mhotel_orders_efdt as
select /*+ mapjoin(a)*/ a.orderid,a.masterhotelid,a.isnoroom,
b.orderid as orderid_before,
a.effectdate,b.effectdate as effectdate_before,
a.orderdate,b.orderdate as orderdate_before,
a.room,b.room as room_before,
a.hotel,b.hotel as hotel_before,
a.masterbasicroomid,b.masterbasicroomid as masterbasicroomid_before,
a.balancetype,b.balancetype as balancetype_before,
b.ordroomnum as ordroomnum_before,
b.isnoroom as isnoroom_before,
b.isforceorder as isforceorder_before
from
RoomControlModel_dkp_force_target_sp a,
RoomControlModel_dkp_orders_sp b
where a.masterhotelid=b.masterhotelid
and b.orderdate>=date_sub(a.ordd,30) and b.orderdate<a.orderdate;


--最近30天同effectdate的订单量和预订房间量（子酒店）
drop table if exists RoomControlModel_dkp_htl_succeed_30d;
create table RoomControlModel_dkp_htl_succeed_30d as
select orderid,
count(orderid_before) as htl_ordnum_efdt30d,
sum(isnoroom_before) as htl_full_ordnum_efdt30d,
sum(if(isnoroom_before=0,ordroomnum_before,0)) as htl_succeed_rmqty_efdt30d,
sum(if(isforceorder_before='T',1,0)) as htl_force_ordnum_efdt30d,
sum(if(isforceorder_before='T',isnoroom_before,0)) as htl_force_full_ordnum_efdt30d,
sum(if(isforceorder_before='T' and isnoroom_before=0,ordroomnum_before,0)) as htl_force_succeed_rmqty_efdt30d
from RoomControlModel_dkp_mhotel_orders_efdt
where hotel=hotel_before
and effectdate=effectdate_before
group by orderid;


--最近30天同effectdate的订单量和预订房间量（母基础房型+支付类型）
drop table if exists RoomControlModel_dkp_mbasicroom_succeed_30d;
create table RoomControlModel_dkp_mbasicroom_succeed_30d as
select orderid,
count(orderid_before) as mbr_ordnum_efdt30d,
sum(isnoroom_before) as mbr_full_ordnum_efdt30d,
sum(if(isnoroom_before=0,ordroomnum_before,0)) as mbr_succeed_rmqty_efdt30d,
sum(if(isforceorder_before='T',1,0)) as mbr_force_ordnum_efdt30d,
sum(if(isforceorder_before='T',isnoroom_before,0)) as mbr_force_full_ordnum_efdt30d,
sum(if(isforceorder_before='T' and isnoroom_before=0,ordroomnum_before,0)) as mbr_force_succeed_rmqty_efdt30d
from RoomControlModel_dkp_mhotel_orders_efdt
where masterbasicroomid=masterbasicroomid_before
and effectdate=effectdate_before
group by orderid;


--最近30天同effectdate的订单量和预订房间量（母酒店+支付类型）
drop table if exists RoomControlModel_dkp_mhotel_succeed_30d;
create table RoomControlModel_dkp_mhotel_succeed_30d as
select orderid,
count(orderid_before) as mhtl_ordnum_efdt30d,
sum(isnoroom_before) as mhtl_full_ordnum_efdt30d,
sum(if(isnoroom_before=0,ordroomnum_before,0)) as mhtl_succeed_rmqty_efdt30d,
sum(if(isforceorder_before='T',1,0)) as mhtl_force_ordnum_efdt30d,
sum(if(isforceorder_before='T',isnoroom_before,0)) as mhtl_force_full_ordnum_efdt30d,
sum(if(isforceorder_before='T' and isnoroom_before=0,ordroomnum_before,0)) as mhtl_force_succeed_rmqty_efdt30d
from RoomControlModel_dkp_mhotel_orders_efdt
where effectdate=effectdate_before
group by orderid;


-----------以下不分effectdate---------------
--目标强下订单最近30天同母酒店的所有订单(T+1)
drop table if exists RoomControlModel_dkp_mhotel_orders;
create table RoomControlModel_dkp_mhotel_orders as
select /*+ mapjoin(a)*/ a.orderid,a.masterbasicroomid,a.masterhotelid,
b.orderid as orderid_before,
a.orderdate,b.orderdate as orderdate_before,
a.room,b.room as room_before,
a.hotel,b.hotel as hotel_before,
b.masterbasicroomid as masterbasicroomid_before,
a.balancetype,b.balancetype as balancetype_before,
b.ordroomnum as ordroomnum_before,
b.isnoroom as isnoroom_before,
b.isforceorder as isforceorder_before,
b.isholdroom,b.isstraightconnect
from
RoomControlModel_dkp_force_target_sp a,
RoomControlModel_dkp_orders_all b
where a.masterhotelid=b.masterhotelid
and b.orderdate>=date_sub(a.ordd,30) and b.orderdate<a.ordd;


--最近30天的订单量（子酒店，不分effectdate）
drop table if exists RoomControlModel_dkp_htl_submit_30d;
create table RoomControlModel_dkp_htl_submit_30d as
select orderid,
count(orderid_before) as htl_ordnum_30d,
sum(isnoroom_before) as htl_full_ordnum_30d,
sum(if(isforceorder_before='T',1,0)) as htl_force_ordnum_30d,
sum(if(isforceorder_before='T',isnoroom_before,0)) as htl_force_full_ordnum_30d
from RoomControlModel_dkp_mhotel_orders
where hotel=hotel_before
group by orderid;


--最近30天的订单量（母基础房型+支付类型，不分effectdate）
drop table if exists RoomControlModel_dkp_mbasicroom_submit_30d;
create table RoomControlModel_dkp_mbasicroom_submit_30d as
select orderid,
count(orderid_before) as mbr_ordnum_30d,
sum(isnoroom_before) as mbr_full_ordnum_30d,
sum(if(isforceorder_before='T',1,0)) as mbr_force_ordnum_30d,
sum(if(isforceorder_before='T',isnoroom_before,0)) as mbr_force_full_ordnum_30d
from RoomControlModel_dkp_mhotel_orders
where masterbasicroomid=masterbasicroomid_before
group by orderid;


--最近30天的订单量（母酒店+支付类型，不分effectdate）
drop table if exists RoomControlModel_dkp_mhotel_submit_30d;
create table RoomControlModel_dkp_mhotel_submit_30d as
select orderid,
count(orderid_before) as mhtl_ordnum_30d,
sum(isnoroom_before) as mhtl_full_ordnum_30d,
sum(if(isforceorder_before='T',1,0)) as mhtl_force_ordnum_30d,
sum(if(isforceorder_before='T',isnoroom_before,0)) as mhtl_force_full_ordnum_30d
from RoomControlModel_dkp_mhotel_orders
group by orderid;


--------------------------------
--该子房型距现在最后一次关房
drop table if exists RoomControlModel_dkp_force_roominforec;
create table RoomControlModel_dkp_force_roominforec as
select * from
(
	select *,
	row_number() over (partition by orderid order by operatetime desc) as rk
	from
	(
		select /*+ mapjoin(a)*/ a.orderid,a.orderdate,a.room,a.effectdate,
		a.balancetype,a.masterbasicroomid,a.masterhotelid,a.isnoroom,
		(case when b.eid like ('%DHS&%') then '大户室与业务'
		when b.eid like ('%EBK&%') then 'EBK'
		when b.eid like ('%SiteMinder%')
		or b.eid like ('%JEYN%')
		or b.eid like ('%Adapter%')
		or b.eid like ('%TL_Lincoln%')
		or b.eid like ('%CheckRoomAvailPoly%')
		or b.eid like ('%Manager%')
		or b.eid like ('%接口平台%')
		or b.eid like ('%IHG%') then '直连'
		when b.eid like ('%BI_%') then '自动开房'
		when b.eid like ('%ORD%') then '订单组'
		when b.eid like ('%JobwsAuto%') then 'JobAuto'
		when b.eid like ('%RoomInfoByPriceChangeJob%') then 'PriceChange'
		when b.eid like ('%HtlRoomStatusSyncJob_良好同步%') then '良好同步'
		when b.eid is null then '超出范围'
		else '其它'  end) as eid_source,
		b.operatetime,b.old_roomstatus,b.new_roomstatus
		from RoomControlModel_dkp_force_target_sp a,
		(select * from DW_HtlDB.FactRoomInfoRec where d>='2016-03-01' and d<'2016-05-18'
			and (new_roomstatus='N' or (old_roomstatus='N' and new_roomstatus is null))) b
		where cast(a.room as int)=b.room and a.effectdate=b.d
		and b.operatetime<=a.orderdate
	) t1
) t2
where rk=1;

--select count(1) from RoomControlModel_dkp_force_roominforec;  451486  97.5%的强下都有关房记录

--该母酒店最近30天保留房订单（占所有订单的比）
drop table if exists RoomControlModel_dkp_mhtl_holdord;
create table RoomControlModel_dkp_mhtl_holdord as
select orderid,sum(if(isholdroom='T',1,0)) as mhtl_hold_ordnum
from RoomControlModel_dkp_mhotel_orders
group by orderid;


-------------------------------
--计算最大房间量
--找出该母基础房型最近30天的拆分订单，每个effectdate按orderdate倒序排序，找到该effectdate的第一个满房订单
drop table if exists RoomControlModel_dkp_mbr_maxnum_30d_p1;
create table RoomControlModel_dkp_mbr_maxnum_30d_p1 as
select orderid,masterbasicroomid,effectdate_before,
orderid_before as orderid_1stnoroom,
orderdate_before as orderdate_1stnoroom
from
(
	select *,
	row_number() over (distribute by orderid,effectdate_before sort by orderdate_before asc) as rn
	from RoomControlModel_dkp_mhotel_orders_efdt
	where masterbasicroomid=masterbasicroomid_before
	and effectdate<>effectdate_before and isnoroom_before=1
) tt
where rn=1;


--找到在这个满房单之前的同effectdate所有订单
drop table if exists RoomControlModel_dkp_mbr_maxnum_30d_p2;
create table RoomControlModel_dkp_mbr_maxnum_30d_p2 as
select a.orderid,a.effectdate_before,a.orderid_1stnoroom,a.orderdate_1stnoroom,
b.orderid_before as orderid_before2,b.orderdate_before as orderdate_before2,
b.ordroomnum_before as ordroomnum_before2
from RoomControlModel_dkp_mbr_maxnum_30d_p1 a,
(select * from RoomControlModel_dkp_mhotel_orders_efdt
	where masterbasicroomid=masterbasicroomid_before) b
where a.orderid=b.orderid and a.effectdate_before=b.effectdate_before
and b.orderdate_before<a.orderdate_1stnoroom;


--找出没有满房订单的effectdate, 以及该effectdate的所有订单
drop table if exists RoomControlModel_dkp_mbr_maxnum_30d_p3;
create table RoomControlModel_dkp_mbr_maxnum_30d_p3 as
select a.orderid,a.effectdate_before,a.orderid_before,a.ordroomnum_before
from
(select * from RoomControlModel_dkp_mhotel_orders_efdt
where masterbasicroomid=masterbasicroomid_before and effectdate<>effectdate_before) a
left join RoomControlModel_dkp_mbr_maxnum_30d_p1 b
on a.orderid=b.orderid and a.effectdate_before=b.effectdate_before
where b.orderid is null or b.effectdate_before is null;


--最大房间量
drop table if exists RoomControlModel_dkp_mbr_maxnum_30d_p4;
create table RoomControlModel_dkp_mbr_maxnum_30d_p4 as
select orderid,max(roomnum) as maxroomnum
from
(
	select orderid,effectdate_before,
		sum(ordroomnum_before2) as roomnum
	from RoomControlModel_dkp_mbr_maxnum_30d_p2
	group by orderid,effectdate_before
	union all
	select orderid,effectdate_before,
		sum(ordroomnum_before) as roomnum
	from RoomControlModel_dkp_mbr_maxnum_30d_p3
	group by orderid,effectdate_before
) t1
group by orderid;


--母酒店	
drop table if exists RoomControlModel_dkp_mhtl_maxnum_30d_p1;
create table RoomControlModel_dkp_mhtl_maxnum_30d_p1 as
select orderid,effectdate_before,
orderid_before as orderid_1stnoroom,
orderdate_before as orderdate_1stnoroom
from
(
	select *,
	row_number() over (distribute by orderid,effectdate_before sort by orderdate_before asc) as rn
	from RoomControlModel_dkp_mhotel_orders_efdt
	where effectdate<>effectdate_before and isnoroom_before=1
) tt
where rn=1;


drop table if exists RoomControlModel_dkp_mhtl_maxnum_30d_p2;
create table RoomControlModel_dkp_mhtl_maxnum_30d_p2 as
select a.orderid,a.effectdate_before,a.orderid_1stnoroom,a.orderdate_1stnoroom,
b.orderid_before as orderid_before2,b.orderdate_before as orderdate_before2,
b.ordroomnum_before as ordroomnum_before2
from RoomControlModel_dkp_mhtl_maxnum_30d_p1 a,
RoomControlModel_dkp_mhotel_orders_efdt b
where a.orderid=b.orderid and a.effectdate_before=b.effectdate_before
and b.orderdate_before<a.orderdate_1stnoroom;


drop table if exists RoomControlModel_dkp_mhtl_maxnum_30d_p3;
create table RoomControlModel_dkp_mhtl_maxnum_30d_p3 as
select a.orderid,a.effectdate_before,a.orderid_before,a.ordroomnum_before
from
(select * from RoomControlModel_dkp_mhotel_orders_efdt
where effectdate<>effectdate_before) a
left join RoomControlModel_dkp_mhtl_maxnum_30d_p1 b
on a.orderid=b.orderid and a.effectdate_before=b.effectdate_before
where b.orderid is null or b.effectdate_before is null;


--最大房间量
drop table if exists RoomControlModel_dkp_mhtl_maxnum_30d_p4;
create table RoomControlModel_dkp_mhtl_maxnum_30d_p4 as
select orderid,max(roomnum) as maxroomnum
from
(
	select orderid,effectdate_before,
		sum(ordroomnum_before2) as roomnum
	from RoomControlModel_dkp_mhtl_maxnum_30d_p2
	group by orderid,effectdate_before
	union all
	select orderid,effectdate_before,
		sum(ordroomnum_before) as roomnum
	from RoomControlModel_dkp_mhtl_maxnum_30d_p3
	group by orderid,effectdate_before
) t1
group by orderid;


--子酒店
drop table if exists RoomControlModel_dkp_htl_maxnum_30d_p1;
create table RoomControlModel_dkp_htl_maxnum_30d_p1 as
select orderid,hotel,effectdate_before,
orderid_before as orderid_1stnoroom,
orderdate_before as orderdate_1stnoroom
from
(
	select *,
	row_number() over (distribute by orderid,effectdate_before sort by orderdate_before asc) as rn
	from RoomControlModel_dkp_mhotel_orders_efdt
	where hotel=hotel_before
	and effectdate<>effectdate_before and isnoroom_before=1
) tt
where rn=1;


drop table if exists RoomControlModel_dkp_htl_maxnum_30d_p2;
create table RoomControlModel_dkp_htl_maxnum_30d_p2 as
select a.orderid,a.effectdate_before,a.orderid_1stnoroom,a.orderdate_1stnoroom,
b.orderid_before as orderid_before2,b.orderdate_before as orderdate_before2,
b.ordroomnum_before as ordroomnum_before2
from RoomControlModel_dkp_htl_maxnum_30d_p1 a,
(select * from RoomControlModel_dkp_mhotel_orders_efdt
	where hotel=hotel_before) b
where a.orderid=b.orderid and a.effectdate_before=b.effectdate_before
and b.orderdate_before<a.orderdate_1stnoroom;


drop table if exists RoomControlModel_dkp_htl_maxnum_30d_p3;
create table RoomControlModel_dkp_htl_maxnum_30d_p3 as
select a.orderid,a.effectdate_before,a.orderid_before,a.ordroomnum_before
from
(select * from RoomControlModel_dkp_mhotel_orders_efdt
where hotel=hotel_before and effectdate<>effectdate_before) a
left join RoomControlModel_dkp_htl_maxnum_30d_p1 b
on a.orderid=b.orderid and a.effectdate_before=b.effectdate_before
where b.orderid is null or b.effectdate_before is null;


drop table if exists RoomControlModel_dkp_htl_maxnum_30d_p4;
create table RoomControlModel_dkp_htl_maxnum_30d_p4 as
select orderid,max(roomnum) as maxroomnum
from
(
	select orderid,effectdate_before,
		sum(ordroomnum_before2) as roomnum
	from RoomControlModel_dkp_htl_maxnum_30d_p2
	group by orderid,effectdate_before
	union all
	select orderid,effectdate_before,
		sum(ordroomnum_before) as roomnum
	from RoomControlModel_dkp_htl_maxnum_30d_p3
	group by orderid,effectdate_before
) t1
group by orderid;


--汇总
use tmp_htlbidb;
drop table if exists RoomControlModel_dkp_data_set01;
create table RoomControlModel_dkp_data_set01 as
select a.*,
pmod(datediff(a.effectdate,'2016-02-28'), 7) as dayofweek,   --入住日星期几
datediff(a.effectdate,a.orderdate) as ordadvanceday2,
b.city,b.zone,b.star,b.cprflag,b.goldstar,
b.roomquantity as mhtl_roomquantity,c.defrecommend
from RoomControlModel_dkp_force_target_sp a
left join
dim_htldb.dimhtlhotel b on a.hotel=b.hotel
left join
dim_htldb.dimroom c on a.room=c.room;


drop table if exists RoomControlModel_dkp_data_set02;
create table RoomControlModel_dkp_data_set02 as
select a.orderid,a.effectdate,
b.mbr_ordnum_30d,   --该masterbasicroom最近30天提交的订单（不分effectdate）
b.mbr_full_ordnum_30d,   --该masterbasicroom最近30天的满房订单
b.mbr_force_ordnum_30d,    --该masterbasicroom最近30天的强下订单量
b.mbr_force_full_ordnum_30d,   --该masterbasicroom最近30天的强下满房订单量
c.mhtl_ordnum_30d,   --该masterhotel最近30天提交的订单（不分effectdate）
c.mhtl_full_ordnum_30d,   --该masterhotel最近30天的满房订单
c.mhtl_force_ordnum_30d,    --该masterhotel最近30天的强下订单量
c.mhtl_force_full_ordnum_30d,  --该masterhotel最近30天的强下满房订单量
d.htl_ordnum_30d,   --该hotel最近30天提交的订单（不分effectdate）
d.htl_full_ordnum_30d,   --该hotel最近30天的满房订单
d.htl_force_ordnum_30d,    --该hotel最近30天的强下订单量
d.htl_force_full_ordnum_30d    --该hotel最近30天的强下满房订单量
from RoomControlModel_dkp_data_set01 a
left join RoomControlModel_dkp_mbasicroom_submit_30d b
on a.orderid=b.orderid
left join RoomControlModel_dkp_mhotel_submit_30d c
on a.orderid=c.orderid
left join RoomControlModel_dkp_htl_submit_30d d
on a.orderid=d.orderid;


drop table if exists RoomControlModel_dkp_data_set03;
create table RoomControlModel_dkp_data_set03 as
select a.orderid,a.effectdate,
b.mbr_ordnum_efdt30d,  --该masterbasicroom+effectdate最近30天提交的订单
b.mbr_full_ordnum_efdt30d,   --该masterbasicroom+effectdate最近30天的满房订单
b.mbr_succeed_rmqty_efdt30d,   --该masterbasicroom+effectdate最近30天已预订的房间量
b.mbr_force_ordnum_efdt30d,   --该masterbasicroom+effectdate最近30天的强下订单量
b.mbr_force_full_ordnum_efdt30d,   --该masterbasicroom+effectdate最近30天的强下满房订单量
b.mbr_force_succeed_rmqty_efdt30d,    --该masterbasicroom+effectdate最近30天强下已订的房间量
c.mhtl_ordnum_efdt30d,  --该masterhotel+effectdate最近30天提交的订单
c.mhtl_full_ordnum_efdt30d,   --该masterhotel+effectdate最近30天的满房订单
c.mhtl_succeed_rmqty_efdt30d,   --该masterhotel+effectdate最近30天已预订的房间量
c.mhtl_force_ordnum_efdt30d,   --该masterhotel+effectdate最近30天的强下订单量
c.mhtl_force_full_ordnum_efdt30d,   --该masterhotel+effectdate最近30天的强下满房订单量
c.mhtl_force_succeed_rmqty_efdt30d,    --该masterhotel+effectdate最近30天强下已订的房间量
d.htl_ordnum_efdt30d,  --该hotel+effectdate最近30天提交的订单
d.htl_full_ordnum_efdt30d,   --该hotel+effectdate最近30天的满房订单
d.htl_succeed_rmqty_efdt30d,   --该hotel+effectdate最近30天已预订的房间量
d.htl_force_ordnum_efdt30d,   --该hotel+effectdate最近30天的强下订单量
d.htl_force_full_ordnum_efdt30d,   --该hotel+effectdate最近30天的强下满房订单量
d.htl_force_succeed_rmqty_efdt30d    --该hotel+effectdate最近30天强下已订的房间量
from RoomControlModel_dkp_data_set01 a
left join RoomControlModel_dkp_mbasicroom_succeed_30d b
on a.orderid=b.orderid
left join RoomControlModel_dkp_mhotel_succeed_30d c
on a.orderid=c.orderid
left join RoomControlModel_dkp_htl_succeed_30d d
on a.orderid=d.orderid;


drop table if exists RoomControlModel_dkp_data_set05;
create table RoomControlModel_dkp_data_set05 as
select a.orderid,a.balancetype,a.hotel,a.masterbasicroomid,a.masterhotelid,
b.maxroomnum as mbr_max_rmqty,c.maxroomnum as mhtl_max_rmqty,
d.maxroomnum as htl_max_rmqty,e.rest_time
from RoomControlModel_dkp_data_set01 a
left join RoomControlModel_dkp_mbr_maxnum_30d_p4 b
on a.orderid=b.orderid
left join RoomControlModel_dkp_mhtl_maxnum_30d_p4 c
on a.orderid=c.orderid
left join RoomControlModel_dkp_htl_maxnum_30d_p4 d
on a.orderid=d.orderid
left join RoomControlModel_dkp_force_resttime e
on a.orderid=e.orderid;


--orderdate在当前批次updatime之后的，取当前批次
drop table if exists RoomControlModel_dkp_data_set18_p1;
create table RoomControlModel_dkp_data_set18_p1 as
select a.orderid,a.effectdate,r.city,r.zone,r.citystar,
r.cityi,r.zonei,r.zonestari
from RoomControlModel_dkp_data_set01 a
inner join
(select * from dw_htlmaindb.citytensity
	where d>='2016-03-01' and d<='2016-05-15' and hour>=0) r
on (a.city=r.city and a.zone=r.zone and a.star=r.citystar and a.effectdate=r.effectdate and a.ordd=r.d)
where a.ord_hour>=r.hour and a.ord_hour<r.hour+2 and a.orderdate>r.updatime;


--orderdate在当前批次updatime之前的（2点之后的订单）,取当天前一批次
drop table if exists RoomControlModel_dkp_data_set18_p2;
create table RoomControlModel_dkp_data_set18_p2 as
select b.orderid,b.effectdate,s.city,s.zone,s.citystar,
s.cityi,s.zonei,s.zonestari
from
(select a.* from
	(select * from RoomControlModel_dkp_data_set01 where ord_hour>=2) a
	inner join
	(select * from dw_htlmaindb.citytensity
		where d>='2016-03-01' and d<='2016-05-15' and hour>=0) r
	on (a.city=r.city and a.zone=r.zone and a.star=r.citystar and a.effectdate=r.effectdate	and a.ordd=r.d)
	where a.ord_hour>=r.hour and a.ord_hour<r.hour+2 and a.orderdate<r.updatime
) b
inner join
(select * from dw_htlmaindb.citytensity
	where d>='2016-03-01' and d<='2016-05-15' and hour>=0) s
on (b.city=s.city and b.zone=s.zone and b.star=s.citystar and b.effectdate=s.effectdate and b.ordd=s.d)
where b.ord_hour>=s.hour+2 and b.ord_hour<s.hour+4;


--orderdate在当前批次updatime之前的（2点之前的订单）,取前一天22点的批次
drop table if exists RoomControlModel_dkp_data_set18_p3;
create table RoomControlModel_dkp_data_set18_p3 as
select b.orderid,b.effectdate,s.city,s.zone,s.citystar,
s.cityi,s.zonei,s.zonestari
from
(select a.* from
	(select * from RoomControlModel_dkp_data_set01 where ord_hour<2) a
	inner join
	(select * from dw_htlmaindb.citytensity
		where d>='2016-03-01' and d<='2016-05-15' and hour=0) r
	on (a.city=r.city and a.zone=r.zone and a.star=r.citystar and a.effectdate=r.effectdate	and a.ordd=r.d)
	where a.orderdate<r.updatime
) b
inner join
(select * from dw_htlmaindb.citytensity
	where d>='2016-02-29' and d<='2016-05-14' and hour=22) s
on (b.city=s.city and b.zone=s.zone and b.star=s.citystar and b.effectdate=s.effectdate
	and b.ordd=date_add(s.d,1));


drop table if exists RoomControlModel_dkp_data_comb01;
create table RoomControlModel_dkp_data_comb01 as
select a.*,b.htl_ordnum_efdt30d,b.htl_full_ordnum_efdt30d,b.htl_succeed_rmqty_efdt30d,
b.htl_force_ordnum_efdt30d,b.htl_force_full_ordnum_efdt30d,b.htl_force_succeed_rmqty_efdt30d,
b.mbr_ordnum_efdt30d,b.mbr_full_ordnum_efdt30d,b.mbr_succeed_rmqty_efdt30d,
b.mbr_force_ordnum_efdt30d,b.mbr_force_full_ordnum_efdt30d,b.mbr_force_succeed_rmqty_efdt30d,
b.mhtl_ordnum_efdt30d,b.mhtl_full_ordnum_efdt30d,b.mhtl_succeed_rmqty_efdt30d,
b.mhtl_force_ordnum_efdt30d,b.mhtl_force_full_ordnum_efdt30d,b.mhtl_force_succeed_rmqty_efdt30d,
c.htl_max_rmqty,c.mbr_max_rmqty,c.mhtl_max_rmqty,c.rest_time,h.eid_source
from RoomControlModel_dkp_data_set02 a
left join RoomControlModel_dkp_data_set03 b
on a.orderid=b.orderid
left join RoomControlModel_dkp_data_set05 c
on a.orderid=c.orderid
left join RoomControlModel_dkp_force_roominforec h
on a.orderid=h.orderid;


drop table if exists RoomControlModel_dkp_data_comb02;
create table RoomControlModel_dkp_data_comb02 as
select t1.*,t2.cityi,t2.zonei,t2.zonestari
from
RoomControlModel_dkp_data_comb01 t1
left join
(
	select a.*,b.cityi,b.zonei,b.zonestari
	from RoomControlModel_dkp_data_set01 a
	inner join RoomControlModel_dkp_data_set18_p1 b
	on a.orderid=b.orderid
	union all
	select a.*,b.cityi,b.zonei,b.zonestari
	from RoomControlModel_dkp_data_set01 a
	inner join RoomControlModel_dkp_data_set18_p2 b
	on a.orderid=b.orderid
	union all
	select a.*,b.cityi,b.zonei,b.zonestari
	from RoomControlModel_dkp_data_set01 a
	inner join RoomControlModel_dkp_data_set18_p3 b
	on a.orderid=b.orderid
) t2
on t1.orderid=t2.orderid;

use tmp_htlbidb;
drop table if exists RoomControlModel_dkp_data_final_3;
create table RoomControlModel_dkp_data_final_3 as
select a.*,
b.htl_ordnum_30d,   --该hotel最近30天提交的订单（不分effectdate）
b.htl_full_ordnum_30d,   --该hotel最近30天的满房订单（不分effectdate）
b.htl_force_ordnum_30d,   --该hotel最近30天的强下订单量
b.htl_force_full_ordnum_30d,  --该hotel最近30天的强下满房订单量
b.mbr_ordnum_30d,   --该masterbasicroom最近30天提交的订单（不分effectdate）
b.mbr_full_ordnum_30d,   --该masterbasicroom最近30天的满房订单（不分effectdate）
b.mbr_force_ordnum_30d,   --该masterbasicroom最近30天的强下订单量
b.mbr_force_full_ordnum_30d,  --该masterbasicroom最近30天的强下满房订单量
b.mhtl_ordnum_30d,   --该masterhotel最近30天提交的订单（不分effectdate）
b.mhtl_full_ordnum_30d,   --该masterhotel最近30天的满房订单（不分effectdate）
b.mhtl_force_ordnum_30d,   --该masterhotel最近30天的强下订单量
b.mhtl_force_full_ordnum_30d,  --该masterhotel最近30天的强下满房订单量
b.htl_ordnum_efdt30d,  --该hotel+effectdate最近30天提交的订单量
b.htl_full_ordnum_efdt30d,   --该hotel+effectdate最近30天的满房订单
b.htl_succeed_rmqty_efdt30d,   --该hotel+effectdate最近30天已预订的房间量
b.htl_force_ordnum_efdt30d,   --该hotel+effectdate最近30天强下的订单量
b.htl_force_full_ordnum_efdt30d,     --该hotel+effectdate最近30天强下满房订单量
b.htl_force_succeed_rmqty_efdt30d,    --该hotel+effectdate最近30天强下已预订的房间量
b.mbr_ordnum_efdt30d,  --该masterbasicroom+effectdate最近30天提交的订单量
b.mbr_full_ordnum_efdt30d,   --该masterbasicroom+effectdate最近30天的满房订单
b.mbr_succeed_rmqty_efdt30d,   --该masterbasicroom+effectdate最近30天已预订的房间量
b.mbr_force_ordnum_efdt30d,   --该masterbasicroom+effectdate最近30天强下的订单量
b.mbr_force_full_ordnum_efdt30d,     --该masterbasicroom+effectdate最近30天强下满房订单量
b.mbr_force_succeed_rmqty_efdt30d,    --该masterbasicroom+effectdate最近30天强下已预订的房间量
b.mhtl_ordnum_efdt30d,  --该masterhotel+effectdate最近30天提交的订单量
b.mhtl_full_ordnum_efdt30d,   --该masterhotel+effectdate最近30天的满房订单
b.mhtl_succeed_rmqty_efdt30d,   --该masterhotel+effectdate最近30天已预订的房间量
b.mhtl_force_ordnum_efdt30d,   --该masterhotel+effectdate最近30天强下的订单量
b.mhtl_force_full_ordnum_efdt30d,     --该masterhotel+effectdate最近30天强下满房订单量
b.mhtl_force_succeed_rmqty_efdt30d,    --该masterhotel+effectdate最近30天强下已预订的房间量
b.htl_max_rmqty,   --该hotel最近30天满房时的最大房量
b.mbr_max_rmqty,   --该masterbasicroom最近30天满房时的最大房量
b.mhtl_max_rmqty,   --该masterhotel最近30天满房时的最大房量
b.rest_time,     --剩余售卖时间
b.eid_source,   --关房操作部门
b.cityi,b.zonei,b.zonestari,
c.mhtl_hold_ordnum
from
RoomControlModel_dkp_data_set01 a
left join RoomControlModel_dkp_data_comb02 b
on a.orderid=b.orderid
left join RoomControlModel_dkp_mhtl_holdord c
on a.orderid=c.orderid;


-- 1.0     9210
-- 2.0     12835
-- 3.0     15395
-- 4.0     15983
-- 5.0     15984
-- 6.0     17533
-- 7.0     18027
-- 8.0     18892
-- 9.0     17489
-- 10.0    16637
-- 11.0    14543
-- 12.0    14152
-- 13.0    12766
-- 14.0    12165
-- 15.0    10983
-- 16.0    12359
-- 17.0    12609
-- 18.0    9674
-- 19.0    9060
-- 20.0    11613
-- 21.0    11734
-- 22.0    9647
-- 23.0    7709
-- 24.0    7693
-- 25.0    7981
-- 26.0    7196
-- 27.0    8110
-- 28.0    7618
-- 29.0    7172
-- 30.0    7174
-- 31.0    6184
-- 32.0    6190
-- 33.0    6090
-- 34.0    5804
-- 35.0    5451
-- 36.0    5307
-- 37.0    6155
-- 38.0    5064
-- 39.0    5074
-- 40.0    5136
-- 41.0    5609
-- 42.0    6568
-- 43.0    5080
-- 44.0    4441
-- 45.0    4411
-- 46.0    4171
-- 47.0    4995
-- 48.0    4266
-- 49.0    5233
-- 50.0    4105
-- 51.0    3777
-- 52.0    3858
-- 53.0    3597
-- 54.0    3529
-- 55.0    3287
-- 56.0    3177
-- 57.0    3125
-- 58.0    3049
-- 59.0    3377
-- 60.0    2951