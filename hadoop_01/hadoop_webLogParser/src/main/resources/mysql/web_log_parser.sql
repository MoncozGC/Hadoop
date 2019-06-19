/*
SQLyog Ultimate v8.32 
MySQL - 5.6.22-log : Database - web_log_view
*********************************************************************
*/ 
/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
CREATE DATABASE /*!32312 IF NOT EXISTS*/`web_log_parser` /*!40100 DEFAULT CHARACTER SET utf8 */;

USE `web_log_parser`;

/*Table structure for table `t_avgpv_num` */

DROP TABLE IF EXISTS `t_avgpv_num`;

CREATE TABLE `t_avgpv_num` (
  `id` int(11) DEFAULT NULL,
  `dateStr` varchar(255) DEFAULT NULL,
  `avgPvNum` decimal(6,2) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Data for the table `t_avgpv_num` */

insert  into `t_avgpv_num`(`id`,`dateStr`,`avgPvNum`) values (1,'20130919','13.40'),(2,'20130920','17.60'),(3,'20130921','15.20'),(4,'20130922','21.10'),(5,'20130923','16.90'),(6,'20130924','18.10'),(7,'20130925','18.60');

/*Table structure for table `t_flow_num` */

DROP TABLE IF EXISTS `t_flow_num`;

CREATE TABLE `t_flow_num` (
  `id` int(11) DEFAULT NULL,
  `dateStr` varchar(255) DEFAULT NULL,
  `pVNum` int(11) DEFAULT NULL,
  `uVNum` int(11) DEFAULT NULL,
  `iPNum` int(11) DEFAULT NULL,
  `newUvNum` int(11) DEFAULT NULL,
  `visitNum` int(11) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Data for the table `t_flow_num` */

insert  into `t_flow_num`(`id`,`dateStr`,`pVNum`,`uVNum`,`iPNum`,`newUvNum`,`visitNum`) values (1,'20131001',4702,3096,2880,2506,3773),(2,'20131002',7528,4860,4435,4209,5937),(3,'20131003',7286,4741,4409,4026,5817),(4,'20131004',6653,5102,4900,2305,4659),(5,'20131005',5957,4943,4563,3134,3698),(6,'20131006',7978,6567,6063,4417,4560),(7,'20131007',6666,5555,4444,3333,3232);

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
