# ************************************************************
# Sequel Pro SQL dump
# Version 4541
#
# http://www.sequelpro.com/
# https://github.com/sequelpro/sequelpro
#
# Host: 35.184.93.227 (MySQL 5.7.25-google-log)
# Database: segment
# Generation Time: 2020-07-17 18:43:23 +0000
# ************************************************************


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


# Dump of table dim_billtos
# ------------------------------------------------------------

CREATE TABLE `dim_billtos` (
  `billto_key` char(36) NOT NULL,
  `netsuite_key` varchar(30) DEFAULT NULL,
  `name` varchar(100) DEFAULT NULL,
  `addr1` varchar(100) DEFAULT NULL,
  `addr2` varchar(100) DEFAULT NULL,
  `city` varchar(60) DEFAULT NULL,
  `state` varchar(40) DEFAULT NULL,
  `zip` varchar(50) DEFAULT NULL,
  `country` varchar(30) DEFAULT NULL,
  `phone` varchar(50) DEFAULT NULL,
  KEY `ix_dim_billtos_netsuite_key` (`netsuite_key`),
  KEY `ix_dim_billtos_key` (`billto_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table dim_channels
# ------------------------------------------------------------

CREATE TABLE `dim_channels` (
  `channel_key` bigint(20) NOT NULL,
  `channel_name` varchar(50) DEFAULT NULL,
  `netsuite_id` bigint(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table dim_customers
# ------------------------------------------------------------

CREATE TABLE `dim_customers` (
  `customer_key` char(36) NOT NULL,
  `customer_name` varchar(100) DEFAULT NULL,
  `customer_email` varchar(100) DEFAULT NULL,
  `netsuite_id` bigint(20) DEFAULT NULL,
  KEY `ix_dim_customers_netsuite_id` (`netsuite_id`),
  KEY `ix_dim_customers_key` (`customer_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table dim_customers_test
# ------------------------------------------------------------

CREATE TABLE `dim_customers_test` (
  `customer_key` char(36) NOT NULL,
  `customer_name` varchar(100) DEFAULT NULL,
  `customer_email` varchar(100) DEFAULT NULL,
  `netsuite_id` bigint(20) DEFAULT NULL,
  KEY `ix_dim_customers_netsuite_id` (`netsuite_id`),
  KEY `ix_dim_customers_key` (`customer_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table dim_dates
# ------------------------------------------------------------

CREATE TABLE `dim_dates` (
  `date_key` int(11) NOT NULL DEFAULT '0',
  `date` date NOT NULL,
  `day_suffix` char(2) NOT NULL,
  `day_of_week` smallint(6) NOT NULL,
  `weekday_name` varchar(10) NOT NULL,
  `weekday_name_short` char(3) NOT NULL,
  `day_of_month` smallint(6) NOT NULL,
  `day_of_year` smallint(6) NOT NULL,
  `week_of_month` smallint(6) NOT NULL,
  `week_of_year` smallint(6) NOT NULL,
  `month` smallint(6) NOT NULL,
  `month_name` varchar(10) NOT NULL,
  `month_name_short` char(3) NOT NULL,
  `quarter` smallint(6) NOT NULL,
  `quarter_name` varchar(6) NOT NULL,
  `year` int(11) NOT NULL,
  `mmyyyy` char(6) NOT NULL,
  `monthyear` char(7) NOT NULL,
  `is_weekend` tinyint(4) NOT NULL,
  `first_date_of_year` date NOT NULL,
  `last_date_of_year` date NOT NULL,
  `first_date_of_quarter` date NOT NULL,
  `last_date_of_quarter` date NOT NULL,
  `first_date_of_month` date NOT NULL,
  `last_date_of_month` date NOT NULL,
  `first_date_of_week` date NOT NULL,
  `last_date_of_week` date NOT NULL,
  `is_holiday` tinyint(4) NOT NULL,
  `holiday_name` varchar(50) DEFAULT NULL,
  KEY `ix_dim_dates_key` (`date_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table dim_desttypes
# ------------------------------------------------------------

CREATE TABLE `dim_desttypes` (
  `desttype_key` bigint(20) NOT NULL,
  `desttype_name` varchar(40) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table dim_lobs
# ------------------------------------------------------------

CREATE TABLE `dim_lobs` (
  `lob_key` bigint(20) NOT NULL,
  `lob_name` varchar(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table dim_people
# ------------------------------------------------------------

CREATE TABLE `dim_people` (
  `wm_people_id` char(36) NOT NULL,
  `city` varchar(50) DEFAULT NULL,
  `state` varchar(50) DEFAULT NULL,
  `zip` varchar(50) DEFAULT NULL,
  `ltv` decimal(16,2) NOT NULL DEFAULT '0.00'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table dim_products
# ------------------------------------------------------------

CREATE TABLE `dim_products` (
  `product_key` bigint(20) NOT NULL AUTO_INCREMENT,
  `sku` varchar(100) NOT NULL,
  `title` varchar(500) NOT NULL,
  `type` varchar(50) DEFAULT NULL,
  `lob_key` bigint(20) DEFAULT NULL,
  `avg_cost` float DEFAULT NULL,
  `netsuite_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`product_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table dim_schedules
# ------------------------------------------------------------

CREATE TABLE `dim_schedules` (
  `schedule_key` bigint(20) NOT NULL,
  `schedule_name` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table dim_schools
# ------------------------------------------------------------

CREATE TABLE `dim_schools` (
  `school_key` bigint(20) NOT NULL AUTO_INCREMENT,
  `school_code` varchar(10) DEFAULT NULL,
  `school_name` varchar(100) DEFAULT NULL,
  `netsuite_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`school_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table dim_shiptos
# ------------------------------------------------------------

CREATE TABLE `dim_shiptos` (
  `shipto_key` char(36) NOT NULL,
  `netsuite_key` varchar(30) DEFAULT NULL,
  `name` varchar(100) DEFAULT NULL,
  `addr1` varchar(100) DEFAULT NULL,
  `addr2` varchar(100) DEFAULT NULL,
  `city` varchar(60) DEFAULT NULL,
  `state` varchar(40) DEFAULT NULL,
  `zip` varchar(50) DEFAULT NULL,
  `country` varchar(30) DEFAULT NULL,
  `phone` varchar(50) DEFAULT NULL,
  `desttype_key` bigint(20) NOT NULL,
  KEY `ix_dim_shiptos_netsuite_key` (`netsuite_key`),
  KEY `ix_dim_shiptos_key` (`shipto_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table dim_sources
# ------------------------------------------------------------

CREATE TABLE `dim_sources` (
  `source_key` bigint(20) NOT NULL,
  `source_name` varchar(50) DEFAULT NULL,
  `netsuite_id` bigint(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table fact_budget_dsr
# ------------------------------------------------------------

CREATE TABLE `fact_budget_dsr` (
  `schedule_key` bigint(20) NOT NULL,
  `date_key` bigint(20) NOT NULL,
  `channel_key` bigint(20) NOT NULL,
  `lob_key` bigint(20) NOT NULL,
  `is_dropship` tinyint(1) NOT NULL,
  `product` decimal(16,2) NOT NULL,
  `shipping` decimal(16,2) NOT NULL DEFAULT '0.00',
  KEY `ix_fact_dsr_budget_lookup` (`schedule_key`,`date_key`,`channel_key`,`lob_key`,`is_dropship`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table fact_orderlines
# ------------------------------------------------------------

CREATE TABLE `fact_orderlines` (
  `date_key` bigint(20) NOT NULL,
  `channel_key` bigint(20) NOT NULL,
  `source_key` bigint(20) NOT NULL,
  `school_key` bigint(20) NOT NULL,
  `customer_key` char(36) NOT NULL,
  `product_key` bigint(20) NOT NULL,
  `billto_key` char(36) NOT NULL,
  `shipto_key` char(36) NOT NULL,
  `netsuite_order_id` bigint(20) DEFAULT NULL,
  `netsuite_order_number` varchar(20) DEFAULT NULL,
  `ocm_order_id` bigint(20) DEFAULT NULL,
  `ocm_order_number` varchar(20) DEFAULT NULL,
  `shipment_number` varchar(20) DEFAULT NULL,
  `netsuite_line_id` varchar(30) DEFAULT NULL,
  `netsuite_line_key` varchar(30) DEFAULT NULL,
  `lob_key` bigint(20) NOT NULL,
  `desttype_key` bigint(20) NOT NULL,
  `is_dropship` tinyint(1) NOT NULL,
  `total_price` decimal(16,2) DEFAULT NULL,
  `total_tax` decimal(16,2) DEFAULT NULL,
  `total_cost` decimal(16,2) DEFAULT NULL,
  `quantity` int(11) DEFAULT NULL,
  `is_discount` tinyint(1) NOT NULL,
  `is_shipping` tinyint(1) NOT NULL,
  `is_service` tinyint(1) NOT NULL,
  `is_cancelled` tinyint(1) NOT NULL,
  `total_discount` decimal(16,2) DEFAULT '0.00',
  `total_shipping` decimal(16,2) DEFAULT '0.00',
  KEY `ix_fact_orderlines_lookup` (`netsuite_line_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table fact_orders
# ------------------------------------------------------------

CREATE TABLE `fact_orders` (
  `date_key` bigint(20) NOT NULL,
  `channel_key` bigint(20) NOT NULL,
  `source_key` bigint(20) NOT NULL,
  `school_key` bigint(20) NOT NULL,
  `customer_key` char(36) NOT NULL,
  `billto_key` char(36) NOT NULL,
  `netsuite_id` bigint(20) NOT NULL DEFAULT '0',
  `netsuite_number` varchar(20) NOT NULL DEFAULT '0',
  `ocm_id` bigint(20) DEFAULT '0',
  `ocm_number` varchar(20) DEFAULT '',
  `merchandise_cost` decimal(16,2) NOT NULL DEFAULT '0.00',
  `merchandise_total` decimal(16,2) NOT NULL DEFAULT '0.00',
  `merchandise_tax` decimal(16,2) NOT NULL DEFAULT '0.00',
  `shipping` decimal(16,2) NOT NULL DEFAULT '0.00',
  `shipping_tax` decimal(16,2) NOT NULL DEFAULT '0.00',
  `discount` decimal(16,2) NOT NULL DEFAULT '0.00',
  `service` decimal(16,2) NOT NULL DEFAULT '0.00',
  `service_tax` decimal(16,2) NOT NULL DEFAULT '0.00',
  `total` decimal(16,2) NOT NULL DEFAULT '0.00',
  KEY `ix_fact_orders_lookup` (`netsuite_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table fact_orders_dsr
# ------------------------------------------------------------

CREATE TABLE `fact_orders_dsr` (
  `schedule_key` bigint(20) NOT NULL,
  `date_key` bigint(20) NOT NULL,
  `channel_key` bigint(20) NOT NULL,
  `lob_key` bigint(20) NOT NULL,
  `is_dropship` tinyint(1) NOT NULL,
  `price` decimal(16,2) NOT NULL,
  `cost` decimal(16,2) NOT NULL,
  `tax` decimal(16,2) NOT NULL,
  `shipping` decimal(16,2) DEFAULT '0.00',
  `discount` decimal(16,2) DEFAULT '0.00',
  KEY `ix_fact_dsr_lookup` (`schedule_key`,`date_key`,`channel_key`,`lob_key`,`is_dropship`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table fact_plan_dsr
# ------------------------------------------------------------

CREATE TABLE `fact_plan_dsr` (
  `schedule_key` bigint(20) NOT NULL,
  `date_key` bigint(20) NOT NULL,
  `channel_key` bigint(20) NOT NULL,
  `lob_key` bigint(20) NOT NULL,
  `is_dropship` tinyint(1) NOT NULL,
  `plan` decimal(16,2) NOT NULL,
  KEY `ix_fact_dsr_budget_lookup` (`schedule_key`,`date_key`,`channel_key`,`lob_key`,`is_dropship`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table link_people
# ------------------------------------------------------------

CREATE TABLE `link_people` (
  `wm_people_id` char(36) NOT NULL,
  `person_id` char(36) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table list_dateandtime_holidays
# ------------------------------------------------------------

CREATE TABLE `list_dateandtime_holidays` (
  `date` date NOT NULL,
  `name` varchar(100) NOT NULL,
  `type` varchar(50) NOT NULL,
  `detail` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



# Dump of table load_budget
# ------------------------------------------------------------

CREATE TABLE `load_budget` (
  `demand_type` varchar(100) DEFAULT NULL,
  `channel` varchar(100) DEFAULT NULL,
  `product_type` varchar(100) DEFAULT NULL,
  `lob` varchar(100) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `value` decimal(16,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table load_plan
# ------------------------------------------------------------

CREATE TABLE `load_plan` (
  `demand_type` varchar(100) DEFAULT NULL,
  `channel` varchar(100) DEFAULT NULL,
  `product_type` varchar(100) DEFAULT NULL,
  `lob` varchar(100) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `value` decimal(16,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table vw_dsr
# ------------------------------------------------------------

CREATE TABLE `vw_dsr` (
   `schedule` VARCHAR(20) NULL DEFAULT NULL,
   `date` DATE NOT NULL,
   `channel` VARCHAR(50) NULL DEFAULT NULL,
   `lob` VARCHAR(100) NULL DEFAULT NULL,
   `dropship` VARCHAR(3) NOT NULL DEFAULT '',
   `price` DECIMAL(16) NOT NULL,
   `cost` DECIMAL(16) NOT NULL,
   `tax` DECIMAL(16) NOT NULL,
   `shipping` DECIMAL(16) NULL DEFAULT '0.00',
   `discount` DECIMAL(16) NULL DEFAULT '0.00'
) ENGINE=MyISAM;





# Replace placeholder table for vw_dsr with correct view syntax
# ------------------------------------------------------------

DROP TABLE `vw_dsr`;

CREATE ALGORITHM=UNDEFINED DEFINER=`jyang`@`%` SQL SECURITY DEFINER VIEW `vw_dsr`
AS SELECT
   `x`.`schedule_name` AS `schedule`,
   `d`.`date` AS `date`,
   `c`.`channel_name` AS `channel`,
   `l`.`lob_name` AS `lob`,(case when (`f`.`is_dropship` = 1) then '3P' else 'OCM' end) AS `dropship`,
   `f`.`price` AS `price`,
   `f`.`cost` AS `cost`,
   `f`.`tax` AS `tax`,
   `f`.`shipping` AS `shipping`,
   `f`.`discount` AS `discount`
FROM ((((`fact_orders_dsr` `f` join `dim_schedules` `x`) join `dim_dates` `d`) join `dim_channels` `c`) join `dim_lobs` `l`) where ((`f`.`schedule_key` = `x`.`schedule_key`) and (`f`.`date_key` = `d`.`date_key`) and (`f`.`channel_key` = `c`.`channel_key`) and (`f`.`lob_key` = `l`.`lob_key`));

--
-- Dumping routines (PROCEDURE) for database 'segment'
--
DELIMITER ;;

# Dump of PROCEDURE sp_build_date_dim
# ------------------------------------------------------------

/*!50003 SET SESSION SQL_MODE="STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"*/;;
/*!50003 CREATE*/ /*!50020 DEFINER=`jyang`@`%`*/ /*!50003 PROCEDURE `sp_build_date_dim`(start_date date, end_date date)
begin
declare rundate date;
set rundate = start_date;

label1: LOOP
	if rundate > end_date then
		leave label1;
	end if;
	insert into dim_date
	select null 
	, rundate
	, CASE DAY(rundate) when 11 then 'th' when 12 then 'th' when 13 then 'th' else case DAY(rundate) % 11 when 1 then 'st' when 2 then 'nd' when '3' then 'rd' else 'th' end end
	, DAYOFWEEK(rundate)
	, DAYNAME(rundate)
	, LEFT(DAYNAME(rundate), 3)
	, DAYOFMONTH(rundate)
	, DAYOFYEAR(rundate)
	, WEEK(rundate,5) - WEEK(DATE_SUB(rundate, INTERVAL DAYOFMONTH(rundate) - 1 DAY),5) + 1
	, WEEKOFYEAR(rundate)
	, MONTH(rundate)
	, MONTHNAME(rundate)
	, LEFT(MONTHNAME(rundate), 3)
	, QUARTER(rundate)
	, CASE QUARTER(rundate) when 1 then 'First' when 2 then 'Second' when 3 then 'Third' when 4 then 'Fourth' end
	, YEAR(rundate)
	, DATE_FORMAT(rundate, '%m%Y')
	, LEFT(DATE_FORMAT(rundate, '%Y%M'), 7)
	, CASE DAYOFWEEK(rundate) when 1 then 1 when 7 then 1 else 0 end
	, MAKEDATE(YEAR(rundate), 1)
	, DATE(CONCAT(YEAR(rundate),"-12-31"))
	, MAKEDATE(YEAR(rundate), 1) + INTERVAL QUARTER(rundate) QUARTER - INTERVAL 1 QUARTER
	, MAKEDATE(YEAR(rundate), 1) + INTERVAL QUARTER(rundate) QUARTER - INTERVAL 1 DAY
	, rundate - INTERVAL (DAY(rundate)-1) DAY
	, LAST_DAY(rundate)
	, rundate + INTERVAL 1 - DAYOFWEEK(rundate) DAY
	, rundate + INTERVAL 7 - DAYOFWEEK(rundate) DAY
	, (select count(*) from list_dateandtime_holidays where type like 'Federal%' and `date` = rundate)
	, (select name from list_dateandtime_holidays where type like 'Federal%' and `date` = rundate)
	;

	
	set rundate = date_add(rundate, interval 1 day);
	iterate label1;
end loop label1;

select rundate;
end */;;

/*!50003 SET SESSION SQL_MODE=@OLD_SQL_MODE */;;
# Dump of PROCEDURE sp_load_budget
# ------------------------------------------------------------

/*!50003 SET SESSION SQL_MODE="ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"*/;;
/*!50003 CREATE*/ /*!50020 DEFINER=`jyang`@`%`*/ /*!50003 PROCEDURE `sp_load_budget`()
begin
insert into fact_orders_dsr (schedule_key, date_key, channel_key, lob_key, `is_dropship`, price, cost, discount, shipping, tax)
select (select schedule_key from dim_schedules where schedule_name = 'Budget'),
d.date_key,
c.channel_key,
l.lob_key,
case product_type when '3P Demand' then 1 else 0 end as is_dropship,
b.value,0,0,0, 0
from load_budget b
join dim_dates d on b.date = d.date
left outer join dim_lobs l on b.lob = l.lob_name
left outer join dim_channels c on case b.channel when 'Endorsed Product' then 'OCM' when 'OAP Product' then 'Wholesale' when 'Carepackages.com' then 'Carepackage' when 'Dormroom.com' then 'Dormroom' when 'MyCollege.com' then 'MyCollege' else b.channel end  = c.channel_name
where b.demand_type = 'Product Demand'
;
end */;;

/*!50003 SET SESSION SQL_MODE=@OLD_SQL_MODE */;;
# Dump of PROCEDURE sp_load_plan
# ------------------------------------------------------------

/*!50003 SET SESSION SQL_MODE="ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"*/;;
/*!50003 CREATE*/ /*!50020 DEFINER=`jyang`@`%`*/ /*!50003 PROCEDURE `sp_load_plan`()
begin
insert into fact_orders_dsr (schedule_key, date_key, channel_key, lob_key, `is_dropship`, price, cost, discount, shipping, tax)
select (select schedule_key from dim_schedules where schedule_name = 'Plan'),
d.date_key,
c.channel_key,
l.lob_key,
case product_type when '3P Demand' then 1 else 0 end as is_dropship,
b.value,0,0,0, 0
from load_plan b
join dim_dates d on b.date = d.date
left outer join dim_lobs l on case b.lob when 'Other' then 'Apparel' else b.lob end = l.lob_name
left outer join dim_channels c on case b.channel when 'Endorsed Product' then 'OCM' when 'OAP Product' then 'Wholesale' when 'Carepackages.com' then 'Carepackage' when 'Dormroom.com' then 'Dormroom' when 'MyCollege.com' then 'MyCollege' else b.channel end  = c.channel_name
where b.demand_type = 'Product Demand'
;
END */;;

/*!50003 SET SESSION SQL_MODE=@OLD_SQL_MODE */;;
# Dump of PROCEDURE sp_upsert_billtos
# ------------------------------------------------------------

/*!50003 SET SESSION SQL_MODE="ANSI_QUOTES,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"*/;;
/*!50003 CREATE*/ /*!50020 DEFINER=`jyang`@`%`*/ /*!50003 PROCEDURE "sp_upsert_billtos"(
	bk char(36), 
	ns varchar(30), 
	n varchar(100),
  	a1 varchar(100),
  	a2 varchar(100), 
  	c varchar(60),
  	s varchar(40),
  	z varchar(50),  
  	y varchar(30), 
  	p varchar(50) 
)
begin
	if (select count(*) from dim_billtos where netsuite_key = ns) > 0 then
		begin
			select bk as old_key, billto_key as new_key 
			from dim_billtos 
			where netsuite_key = ns;
			
			-- do not update billto_key
			update dim_billtos 
			set name = n, addr1 = a1, addr2 = a2, city = c, state = s, zip = z, country = y, phone = p	
            where netsuite_key = ns;

		end;
	else
		begin
			insert into dim_billtos (
				billto_key, netsuite_key,
				name, addr1, addr2, city, state, zip, country, phone
            ) values (
            	bk, ns,
            	n, a1, a2, c, s, z, y, p
            );
            select bk as old_key, bk as new_key;
		end;
	end if;
end */;;

/*!50003 SET SESSION SQL_MODE=@OLD_SQL_MODE */;;
# Dump of PROCEDURE sp_upsert_customer
# ------------------------------------------------------------

/*!50003 SET SESSION SQL_MODE="ANSI_QUOTES,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"*/;;
/*!50003 CREATE*/ /*!50020 DEFINER=`jyang`@`%`*/ /*!50003 PROCEDURE "sp_upsert_customer"(ck char(36), cn varchar(100), ce varchar(100), id bigint)
begin
	if (select count(*) from dim_customers where netsuite_id = id) > 0 then
		begin
			select ck as old_key, customer_key as new_key 
			from dim_customers 
			where netsuite_id = id;
			update dim_customers 
			set `customer_name` = cn,
                `customer_email` = ce		
            where netsuite_id = id;
		end;
	else
		begin
			insert into dim_customers (
				`customer_key`,
                `customer_name`,
                `customer_email`,
                `netsuite_id`
            ) values (
                ck,
                cn,
                ce,
                id
            );
            select ck as old_key, ck as new_key;
		end;
	end if;
end */;;

/*!50003 SET SESSION SQL_MODE=@OLD_SQL_MODE */;;
# Dump of PROCEDURE sp_upsert_dsr
# ------------------------------------------------------------

/*!50003 SET SESSION SQL_MODE="STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"*/;;
/*!50003 CREATE*/ /*!50020 DEFINER=`jyang`@`%`*/ /*!50003 PROCEDURE `sp_upsert_dsr`(sk bigint, dk bigint, ck bigint, lk bigint, ds tinyint, p decimal(16,2), c decimal(16,2), t decimal(16,2), d decimal(16,2), s decimal(16,2))
begin
	if (select count(*) from fact_orders_dsr where schedule_key = sk and date_key = dk and channel_key = ck and lob_key = lk and is_dropship = ds) > 0 then
		begin
			update fact_orders_dsr set price = price + p, cost = cost + c, tax = tax + t, discount = discount + d, shipping = shipping + s where schedule_key = sk and date_key = dk and channel_key = ck and lob_key = lk and is_dropship = ds;
		end;
	else
		begin
			insert into fact_orders_dsr (schedule_key, date_key, channel_key, lob_key, is_dropship, price, cost, tax, discount, shipping) values (sk, dk, ck, lk, ds, p, c, t,d, s);
		end;
	end if;
end */;;

/*!50003 SET SESSION SQL_MODE=@OLD_SQL_MODE */;;
# Dump of PROCEDURE sp_upsert_orderlines
# ------------------------------------------------------------

/*!50003 SET SESSION SQL_MODE="STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"*/;;
/*!50003 CREATE*/ /*!50020 DEFINER=`jyang`@`%`*/ /*!50003 PROCEDURE `sp_upsert_orderlines`(date bigint, channel bigint, source bigint, school bigint, customer char(36), product bigint, billto char(36), shipto char(36), 
	nsoid bigint, nsonum varchar(20), ocmoid bigint, ocmonum varchar(20), shipment varchar(20), nslid varchar(30), nslkey varchar(30), lob bigint, desttype bigint, ds tinyint,
	price decimal(16,2), tax decimal(16,2), cost decimal(16,2), qty int, discount tinyint, shipping tinyint, service tinyint, cancelled tinyint, discountamt decimal(16,2), shippingamt decimal(16,2)
)
begin
	if (select count(*) from fact_orderlines where netsuite_line_id = nslid) > 0 then
		begin
			select nslid as line_id, 1 as existing, is_cancelled as cancelled, total_price as price, total_tax as tax, total_cost as cost, total_discount as discount, total_shipping as shipping
			from fact_orderlines
			where netsuite_line_id = nslid;
			
			update fact_orderlines 
			set date_key = date, channel_key = channel, source_key = source, school_key = school, customer_key = customer, product_key = product, billto_key = billto, shipto_key = shipto,
			netsuite_order_id = nsoid, netsuite_order_number = nsonum, ocm_order_id = ocmoid, ocm_order_number = ocmonum, shipment_number = shipment, netsuite_line_key = nslkey,
			lob_key = lob, desttype_key = desttype, is_dropship = ds, total_price = price, total_tax = tax, total_cost = cost, quantity = qty, 
			is_discount = discount, is_shipping = shipping, is_service = service, is_cancelled = cancelled, total_discount = discountamt, total_shipping = shippingamt
			where netsuite_line_id = nslid;
			
		end;
	else
		begin
			insert into fact_orderlines (
				`date_key`,
  				`channel_key`,
  				`source_key`,
  				`school_key`,
 				`customer_key`,
  				`product_key`,
  				`billto_key`,
  				`shipto_key`,
  				`netsuite_order_id`,
  				`netsuite_order_number`,
  				`ocm_order_id`,
  				`ocm_order_number`,
  				`shipment_number`,
  				`netsuite_line_id`,
  				`netsuite_line_key`,
  				`lob_key`,
  				`desttype_key`,
  				`is_dropship`,
  				`total_price`,
  				`total_tax`,
  				`total_cost`,
 				`quantity`,
  				`is_discount`,
 				`is_shipping`,
  				`is_service`,
  				`is_cancelled`,
  				total_discount,
  				total_shipping
			) values (
				date, 
				channel, 
				source, 
				school, 
				customer, 
				product, 
				billto, 
				shipto, 
				nsoid, 
				nsonum, 
				ocmoid, 
				ocmonum, 
				shipment,
				nslid, 
				nslkey, 
				lob, 
				desttype, 
				ds,
				price, 
				tax, 
				cost, 
				qty, 
				discount, 
				shipping, 
				service, 
				cancelled,
				discountamt,
				shippingamt
			);
			
			select nslid as line_id, 0 as existing, cancelled, price, tax, cost, discountamt, shippingamt;
		end;
	end if;
end */;;

/*!50003 SET SESSION SQL_MODE=@OLD_SQL_MODE */;;
# Dump of PROCEDURE sp_upsert_orders
# ------------------------------------------------------------

/*!50003 SET SESSION SQL_MODE="STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"*/;;
/*!50003 CREATE*/ /*!50020 DEFINER=`jyang`@`%`*/ /*!50003 PROCEDURE `sp_upsert_orders`(date bigint, channel bigint, source bigint, school bigint, customer char(36), billto char(36),
	nsoid bigint, nsonum varchar(20), ocmoid bigint, ocmonum varchar(20), cost decimal(16,2), prod decimal(16,2), prodtax decimal(16,2),
	ship decimal(16,2), shiptax decimal(16,2), disc decimal(16,2), svc decimal(16,2), svctax decimal(16,2), tot decimal(16,2)
)
begin
	if (select count(*) from fact_orders where netsuite_id = nsoid) > 0 then
		begin
			update fact_orders 
			set `date_key` = date,
                `channel_key` = channel,
                `source_key` = source,
                `school_key` = school,
                `customer_key` = customer,
                `billto_key` = billto,
                `netsuite_id` = nsoid,
                `netsuite_number` = nsonum,
                `ocm_id` = ocmoid,
                `ocm_number` = ocmonum,
                `merchandise_cost` = cost,
                `merchandise_total` = prod,
                `merchandise_tax` = prodtax,
                `shipping` = ship,
                `shipping_tax` = shiptax,
                `discount` = disc,
                `service` = svc,
                `service_tax` = svctax,
                `total` = tot
			where netsuite_id = nsoid;
		end;
	else
		begin
			insert into fact_orders (
				`date_key`,
                `channel_key`,
                `source_key`,
                `school_key`,
                `customer_key`,
                `billto_key`,
                `netsuite_id`,
                `netsuite_number`,
                `ocm_id`,
                `ocm_number`,
                `merchandise_cost`,
                `merchandise_total`,
                `merchandise_tax`,
                `shipping`,
                `shipping_tax`,
                `discount`,
                `service`,
                `service_tax`,
                `total`
			) values (
                date,
                channel,
                source,
                school,
                customer,
                billto,
                nsoid,
                nsonum,
                ocmoid,
                ocmonum,
                cost,
                prod,
                prodtax,
                ship,
                shiptax,
                disc,
                svc,
                svctax,
                tot
			);
		end;
	end if;
end */;;

/*!50003 SET SESSION SQL_MODE=@OLD_SQL_MODE */;;
# Dump of PROCEDURE sp_upsert_product
# ------------------------------------------------------------

/*!50003 SET SESSION SQL_MODE="ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"*/;;
/*!50003 CREATE*/ /*!50020 DEFINER=`jyang`@`%`*/ /*!50003 PROCEDURE `sp_upsert_product`(s varchar(100), t varchar(100), y varchar(100), l varchar(100), c float, id bigint)
begin
    if l is null or l = '' then
    	set l = 'Unassigned';
    end if;
	if (select count(*) from dim_products where netsuite_id = id) > 0 then
		begin
			update dim_products 
			set sku = s, title = t, type = y, lob_key = (select lob_key from dim_lobs where lob_name = l), avg_cost = c
            where netsuite_id = id;
		end;
	else
		begin
			insert into dim_products ( sku, title, type, lob_key, avg_cost, netsuite_id)
            values(s, t, y, (select lob_key from dim_lobs where lob_name = l), c, id);
        end;
	end if;
end */;;

/*!50003 SET SESSION SQL_MODE=@OLD_SQL_MODE */;;
# Dump of PROCEDURE sp_upsert_school
# ------------------------------------------------------------

/*!50003 SET SESSION SQL_MODE="ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"*/;;
/*!50003 CREATE*/ /*!50020 DEFINER=`jyang`@`%`*/ /*!50003 PROCEDURE `sp_upsert_school`(c varchar(100), n varchar(100), id bigint)
begin

	if (select count(*) from dim_schools where netsuite_id = id) > 0 then
		begin
			update dim_schools 
			set school_code = c, school_name = n
			where netsuite_id = id;
		end;
	else
		begin
			insert into dim_schools (school_code, school_name, netsuite_id)
            values(c, n, id);
        end;
	end if;
end */;;

/*!50003 SET SESSION SQL_MODE=@OLD_SQL_MODE */;;
# Dump of PROCEDURE sp_upsert_shiptos
# ------------------------------------------------------------

/*!50003 SET SESSION SQL_MODE="ANSI_QUOTES,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"*/;;
/*!50003 CREATE*/ /*!50020 DEFINER=`jyang`@`%`*/ /*!50003 PROCEDURE "sp_upsert_shiptos"(
	sk char(36), 
	ns varchar(30), 
	n varchar(100),
  	a1 varchar(100),
  	a2 varchar(100), 
  	c varchar(60),
  	s varchar(40),
  	z varchar(50),  
  	y varchar(30), 
  	p varchar(50),
  	dt bigint
)
begin
	if (select count(*) from dim_shiptos where netsuite_key = ns) > 0 then
		begin
			select sk as old_key, shipto_key as new_key 
			from dim_shiptos 
			where netsuite_key = ns;
			
			-- do not update billto_key
			update dim_shiptos 
			set name = n, addr1 = a1, addr2 = a2, city = c, state = s, zip = z, country = y, phone = p, desttype_key = dt
            where netsuite_key = ns;

		end;
	else
		begin
			insert into dim_shiptos (
				shipto_key, netsuite_key,
				name, addr1, addr2, city, state, zip, country, phone, desttype_key
            ) values (
            	sk, ns,
            	n, a1, a2, c, s, z, y, p, dt
            );
            select sk as old_key, sk as new_key;
		end;
	end if;
end */;;

/*!50003 SET SESSION SQL_MODE=@OLD_SQL_MODE */;;
DELIMITER ;

/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;

create table dim_sponsors (
  sponsor_key bigint(20) NOT NULL AUTO_INCREMENT,
  sponsor_code varchar(20) NOT NULL,
  sponsor_name varchar(100) DEFAULT NULL,
  netsuite_id bigint(20) DEFAULT NULL,
  school_key bigint ,
  PRIMARY KEY (`sponsor_key`),
  index ix_dim_sponsors_netsuite_id (netsuite_id),
  index ix_dim_sponsors_school_key (school_key)
);


DELIMITER ;;
CREATE PROCEDURE `sp_upsert_sponsor` (c varchar(100), n varchar(100), id bigint, sid bigint)
begin

	if (select count(*) from dim_schools where netsuite_id = id) > 0 then
		begin
			update dim_sponsors 
			set sponsor_code = c, sponsor_name = n, school_key = (select school_key from dim_schools where netsuite_id = sid)
			where netsuite_id = id;
		end;
	else
		begin
			insert into dim_sponsors (sponsor_code, sponsor_name, netsuite_id, school_key)
            values (c, n, id, (select school_key from dim_schools where netsuite_id = sid));
        end;
	end if;
end;;
DELIMITER ;