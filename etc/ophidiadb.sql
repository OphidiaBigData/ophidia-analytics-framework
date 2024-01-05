--
--    Ophidia Analytics Framework
--    Copyright (C) 2012-2023 CMCC Foundation
--
--    This program is free software: you can redistribute it and/or modify
--    it under the terms of the GNU General Public License as published by
--    the Free Software Foundation, either version 3 of the License, or
--    (at your option) any later version.
--
--    This program is distributed in the hope that it will be useful,
--    but WITHOUT ANY WARRANTY; without even the implied warranty of
--    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
--    GNU General Public License for more details.
--
--    You should have received a copy of the GNU General Public License
--    along with this program.  If not, see <http://www.gnu.org/licenses/>.
--

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `country`
--

DROP TABLE IF EXISTS `country`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `country` (
  `idcountry` smallint(5) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(64) NOT NULL,
  PRIMARY KEY (`idcountry`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `country`
--

LOCK TABLES `country` WRITE;
/*!40000 ALTER TABLE `country` DISABLE KEYS */;
INSERT INTO `country` VALUES (245,'Afghanistan, Islamic State of'),(246,'Albania'),(247,'Algeria'),(248,'American Samoa'),(249,'Andorra, Principality of'),(250,'Angola'),(251,'Anguilla'),(252,'Antarctica'),(253,'Antigua and Barbuda'),(254,'Argentina'),(255,'Armenia'),(256,'Aruba'),(257,'Australia'),(258,'Austria'),(259,'Azerbaidjan'),(260,'Bahamas'),(261,'Bahrain'),(262,'Bangladesh'),(263,'Barbados'),(264,'Belarus'),(265,'Belgium'),(266,'Belize'),(267,'Benin'),(268,'Bermuda'),(269,'Bhutan'),(270,'Bolivia'),(271,'Bosnia-Herzegovina'),(272,'Botswana'),(273,'Bouvet Island'),(274,'Brazil'),(275,'British Indian Ocean Territory'),(276,'Brunei Darussalam'),(277,'Bulgaria'),(278,'Burkina Faso'),(279,'Burundi'),(280,'Cambodia, Kingdom of'),(281,'Cameroon'),(282,'Canada'),(283,'Cape Verde'),(284,'Cayman Islands'),(285,'Central African Republic'),(286,'Chad'),(287,'Chile'),(288,'China'),(289,'Christmas Island'),(290,'Cocos (Keeling) Islands'),(291,'Colombia'),(292,'Comoros'),(293,'Congo'),(294,'Congo, The Democratic Republic of the'),(295,'Cook Islands'),(296,'Costa Rica'),(297,'Croatia'),(298,'Cuba'),(299,'Cyprus'),(300,'Czech Republic'),(301,'Denmark'),(302,'Djibouti'),(303,'Dominica'),(304,'Dominican Republic'),(305,'East Timor'),(306,'Ecuador'),(307,'Egypt'),(308,'El Salvador'),(309,'Equatorial Guinea'),(310,'Eritrea'),(311,'Estonia'),(312,'Ethiopia'),(313,'Falkland Islands'),(314,'Faroe Islands'),(315,'Fiji'),(316,'Finland'),(317,'Former Czechoslovakia'),(318,'Former USSR'),(319,'France'),(320,'France (European Territory)'),(321,'French Guyana'),(322,'French Southern Territories'),(323,'Gabon'),(324,'Gambia'),(325,'Georgia'),(326,'Germany'),(327,'Ghana'),(328,'Gibraltar'),(329,'Great Britain'),(330,'Greece'),(331,'Greenland'),(332,'Grenada'),(333,'Guadeloupe (French)'),(334,'Guam (USA)'),(335,'Guatemala'),(336,'Guinea'),(337,'Guinea Bissau'),(338,'Guyana'),(339,'Haiti'),(340,'Heard and McDonald Islands'),(341,'Holy See (Vatican City State)'),(342,'Honduras'),(343,'Hong Kong'),(344,'Hungary'),(345,'Iceland'),(346,'India'),(347,'Indonesia'),(348,'Iran'),(349,'Iraq'),(350,'Ireland'),(351,'Israel'),(352,'Italy'),(353,'Ivory Coast'),(354,'Jamaica'),(355,'Japan'),(356,'Jordan'),(357,'Kazakhstan'),(358,'Kenya'),(359,'Kiribati'),(360,'Kuwait'),(361,'Kyrgyz Republic (Kyrgyzstan)'),(362,'Laos'),(363,'Latvia'),(364,'Lebanon'),(365,'Lesotho'),(366,'Liberia'),(367,'Libya'),(368,'Liechtenstein'),(369,'Lithuania'),(370,'Luxembourg'),(371,'Macau'),(372,'Macedonia'),(373,'Madagascar'),(374,'Malawi'),(375,'Malaysia'),(376,'Maldives'),(377,'Mali'),(378,'Malta'),(379,'Marshall Islands'),(380,'Martinique (French)'),(381,'Mauritania'),(382,'Mauritius'),(383,'Mayotte'),(384,'Mexico'),(385,'Micronesia'),(386,'Moldavia'),(387,'Monaco'),(388,'Mongolia'),(389,'Montserrat'),(390,'Morocco'),(391,'Mozambique'),(392,'Myanmar'),(393,'Namibia'),(394,'Nauru'),(395,'Nepal'),(396,'Netherlands'),(397,'Netherlands Antilles'),(398,'Neutral Zone'),(399,'New Caledonia (French)'),(400,'New Zealand'),(401,'Nicaragua'),(402,'Niger'),(403,'Nigeria'),(404,'Niue'),(405,'Norfolk Island'),(406,'North Korea'),(407,'Northern Mariana Islands'),(408,'Norway'),(409,'Oman'),(410,'Pakistan'),(411,'Palau'),(412,'Panama'),(413,'Papua New Guinea'),(414,'Paraguay'),(415,'Peru'),(416,'Philippines'),(417,'Pitcairn Island'),(418,'Poland'),(419,'Polynesia (French)'),(420,'Portugal'),(421,'Puerto Rico'),(422,'Qatar'),(423,'Reunion (French)'),(424,'Romania'),(425,'Russian Federation'),(426,'Rwanda'),(427,'S. Georgia & S. Sandwich Isls.'),(428,'Saint Helena'),(429,'Saint Kitts & Nevis Anguilla'),(430,'Saint Lucia'),(431,'Saint Pierre and Miquelon'),(432,'Saint Tome (Sao Tome) and Principe'),(433,'Saint Vincent & Grenadines'),(434,'Samoa'),(435,'San Marino'),(436,'Saudi Arabia'),(437,'Senegal'),(438,'Seychelles'),(439,'Sierra Leone'),(440,'Singapore'),(441,'Slovak Republic'),(442,'Slovenia'),(443,'Solomon Islands'),(444,'Somalia'),(445,'South Africa'),(446,'South Korea'),(447,'Spain'),(448,'Sri Lanka'),(449,'Sudan'),(450,'Suriname'),(451,'Svalbard and Jan Mayen Islands'),(452,'Swaziland'),(453,'Sweden'),(454,'Switzerland'),(455,'Syria'),(456,'Tadjikistan'),(457,'Taiwan'),(458,'Tanzania'),(459,'Thailand'),(460,'Togo'),(461,'Tokelau'),(462,'Tonga'),(463,'Trinidad and Tobago'),(464,'Tunisia'),(465,'Turkey'),(466,'Turkmenistan'),(467,'Turks and Caicos Islands'),(468,'Tuvalu'),(469,'Uganda'),(470,'Ukraine'),(471,'United Arab Emirates'),(472,'United Kingdom'),(473,'United States'),(474,'Uruguay'),(475,'USA Minor Outlying Islands'),(476,'Uzbekistan'),(477,'Vanuatu'),(478,'Venezuela'),(479,'Vietnam'),(480,'Virgin Islands (British)'),(481,'Virgin Islands (USA)'),(482,'Wallis and Futuna Islands'),(483,'Western Sahara'),(484,'Yemen'),(485,'Yugoslavia'),(486,'Zaire'),(487,'Zambia'),(488,'Zimbabwe');
/*!40000 ALTER TABLE `country` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dbinstance`
--

DROP TABLE IF EXISTS `dbinstance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dbinstance` (
  `iddbinstance` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `iddbmsinstance` int(10) unsigned NOT NULL,
  `dbname` varchar(256) NOT NULL,
  PRIMARY KEY (`iddbinstance`),
  /* THIS IS A COMMENT - Unique key is a constraint used to check the db name uniqueness generation rule */
  UNIQUE KEY `dbname` (`dbname`),
  KEY `iddbmsinstance` (`iddbmsinstance`),
  CONSTRAINT `iddbmsinstance` FOREIGN KEY (`iddbmsinstance`) REFERENCES `dbmsinstance` (`iddbmsinstance`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dbinstance`
--

LOCK TABLES `dbinstance` WRITE;
/*!40000 ALTER TABLE `dbinstance` DISABLE KEYS */;
/*!40000 ALTER TABLE `dbinstance` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dbmsinstance`
--

DROP TABLE IF EXISTS `dbmsinstance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dbmsinstance` (
  `iddbmsinstance` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `idhost` int(10) unsigned NOT NULL,
  `login` varchar(256) DEFAULT NULL,
  `password` varchar(256) DEFAULT NULL,
  `port` int(11) NOT NULL,
  `ioservertype`  varchar(256) NOT NULL DEFAULT 'mysql_table',
  PRIMARY KEY (`iddbmsinstance`),
  KEY `idhost` (`idhost`),
  UNIQUE KEY `port_host` (`idhost`, `port`),
  CONSTRAINT `idhost` FOREIGN KEY (`idhost`) REFERENCES `host` (`idhost`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dbmsinstance`
--

LOCK TABLES `dbmsinstance` WRITE;
/*!40000 ALTER TABLE `dbmsinstance` DISABLE KEYS */;
/*!40000 ALTER TABLE `dbmsinstance` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `fragment`
--

DROP TABLE IF EXISTS `fragment`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `fragment` (
  `idfragment` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `iddbinstance` int(10) unsigned NOT NULL,
  `iddatacube` int(10) unsigned NOT NULL,
  `fragrelativeindex` int(10) unsigned NOT NULL,
  `fragmentname` varchar(256) NOT NULL,
  `keystart` int(10) unsigned NOT NULL,
  `keyend` int(10) unsigned NOT NULL,
  PRIMARY KEY (`idfragment`),
  /* THIS IS A COMMENT - Unique key is a constraint used to check the fragment name uniqueness generation rule */
  UNIQUE KEY `fragmentname` (`fragmentname`),
  KEY `iddbinstance` (`iddbinstance`),
  CONSTRAINT `iddbinstance` FOREIGN KEY (`iddbinstance`) REFERENCES `dbinstance` (`iddbinstance`) ON DELETE CASCADE ON UPDATE CASCADE,
  KEY `iddatacube` (`iddatacube`),
  CONSTRAINT `iddatacube` FOREIGN KEY (`iddatacube`) REFERENCES `datacube` (`iddatacube`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `fragment`
--

LOCK TABLES `fragment` WRITE;
/*!40000 ALTER TABLE `fragment` DISABLE KEYS */;
/*!40000 ALTER TABLE `fragment` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `host`
--

DROP TABLE IF EXISTS `host`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `host` (
  `idhost` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `hostname` varchar(256) NOT NULL,
  `cores` int(10) unsigned DEFAULT NULL,
  `memory` int(10) unsigned DEFAULT NULL,
  `status` varchar(4) NOT NULL DEFAULT "up",
  `importcount` int(10) unsigned NOT NULL DEFAULT 0,
  `lastupdate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`idhost`),
  KEY `idhost` (`idhost`),
  UNIQUE KEY `hostname` (`hostname`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `host`
--

LOCK TABLES `host` WRITE;
/*!40000 ALTER TABLE `host` DISABLE KEYS */;
/*!40000 ALTER TABLE `host` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `hostpartition`
--

-- Note: the constraint to foreign key `idjob` has been removed in order to enable dynamic clustering

DROP TABLE IF EXISTS `hostpartition`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `hostpartition` (
  `idhostpartition` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `partitionname` varchar(64) NOT NULL,
  `hidden` tinyint(1) NOT NULL DEFAULT 0,
  `reserved` tinyint(1) NOT NULL DEFAULT 0,
  `partitiontype` tinyint(1) NOT NULL DEFAULT 0,
  `creationdate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `hosts` int(10) unsigned NULL DEFAULT 0,
  `iduser` int(10) unsigned NULL DEFAULT NULL,
  `idjob` int(10) unsigned DEFAULT NULL,
  CONSTRAINT `iduser_h` FOREIGN KEY (`iduser`) REFERENCES `user` (`iduser`) ON DELETE CASCADE ON UPDATE CASCADE,
  PRIMARY KEY (`idhostpartition`),
  UNIQUE KEY `user_partition` (`partitionname`, `iduser`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `hostpartition`
--

LOCK TABLES `hostpartition` WRITE;
/*!40000 ALTER TABLE `hostpartition` DISABLE KEYS */;
/*!40000 ALTER TABLE `hostpartition` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `hashost`
--

DROP TABLE IF EXISTS `hashost`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `hashost` (
  `idhostpartition` int(10) unsigned NOT NULL,
  `idhost` int(10) unsigned NOT NULL,
  `importcount` int(10) unsigned NULL DEFAULT 0,
  PRIMARY KEY (`idhostpartition`, `idhost`),
  CONSTRAINT `idhost_hh` FOREIGN KEY (`idhost`) REFERENCES `host` (`idhost`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `idhostpartition_hh` FOREIGN KEY (`idhostpartition`) REFERENCES `hostpartition` (`idhostpartition`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `hashost`
--

LOCK TABLES `hashost` WRITE;
/*!40000 ALTER TABLE `hashost` DISABLE KEYS */;
/*!40000 ALTER TABLE `hashost` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `servicestatus`
--

DROP TABLE IF EXISTS `servicestatus`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `servicestatus` (
  `idservicestatus` bigint(20) NOT NULL AUTO_INCREMENT,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `status` enum('Active','Inactive','Timeout','Error') NOT NULL COMMENT 'Don''t change the enumeration value order',
  `elapsedtime` int(11) NOT NULL,
  `iddbmsinstance` int(10) unsigned NOT NULL,
  PRIMARY KEY (`idservicestatus`),
  KEY `iddbmsinstance` (`iddbmsinstance`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `servicestatus`
--

LOCK TABLES `servicestatus` WRITE;
/*!40000 ALTER TABLE `servicestatus` DISABLE KEYS */;
/*!40000 ALTER TABLE `servicestatus` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `folder`
--

DROP TABLE IF EXISTS `folder`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `folder` (
  `idfolder` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `idparent` int(10) unsigned DEFAULT NULL,
  `foldername` varchar(256) NOT NULL,
  PRIMARY KEY (`idfolder`),
  KEY `idparent` (`idparent`),
  CONSTRAINT `idparent_f` FOREIGN KEY (`idparent`) REFERENCES `folder` (`idfolder`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `folder`
--

LOCK TABLES `folder` WRITE;
/*!40000 ALTER TABLE `folder` DISABLE KEYS */;
INSERT INTO `folder` (`idfolder`, `foldername`) VALUES (1, 'root');
/*!40000 ALTER TABLE `folder` ENABLE KEYS */;
UNLOCK TABLES;


--
-- Table structure for table `user`
--

DROP TABLE IF EXISTS `user`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user` (
  `iduser` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(64) NULL,
  `surname` varchar(64) NULL,
  `mail` varchar(64) NULL,
  `username` varchar(256) NOT NULL,
  `password` varchar(64) NULL,
  `registrationdate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `accountcertified` tinyint(1) NOT NULL DEFAULT '0',
  `idcountry` smallint(5) unsigned DEFAULT NULL,
  `maxhosts` int(10) unsigned NULL DEFAULT 0,
  PRIMARY KEY (`iduser`),
  UNIQUE KEY `username` (`username`),
  KEY `idcountry` (`idcountry`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `user`
--

LOCK TABLES `user` WRITE;
/*!40000 ALTER TABLE `user` DISABLE KEYS */;
INSERT INTO `user` (`accountcertified`, `username`) VALUES (1, 'admin'), (1, 'oph-test');
/*!40000 ALTER TABLE `user` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `source`
--

DROP TABLE IF EXISTS `source`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `source` (
  `idsource` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `uri` varchar(2048) NOT NULL,  
  PRIMARY KEY (`idsource`),
  UNIQUE KEY `uri` (`uri`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `source`
--

LOCK TABLES `source` WRITE;
/*!40000 ALTER TABLE `source` DISABLE KEYS */;
/*!40000 ALTER TABLE `source` ENABLE KEYS */;
UNLOCK TABLES;


--
-- Table structure for table `datacube`
--

DROP TABLE IF EXISTS `datacube`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `datacube` (
  `iddatacube` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `idcontainer` int(10) unsigned NOT NULL,
  `creationdate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `description` varchar(2048) DEFAULT NULL,
  `hostxdatacube` int(10) DEFAULT NULL,
  `fragmentxdb` int(10) DEFAULT NULL,
  `tuplexfragment` int(10) DEFAULT NULL,
  `measure` varchar(256) NOT NULL,
  `measuretype` varchar(64) NOT NULL,
  `fragrelativeindexset` varchar(2048) NOT NULL,
  `csize` bigint(20) DEFAULT NULL,
  `celements` bigint(20) DEFAULT NULL,
  `compress` int(10) DEFAULT 0,
  `level` int(10) NOT NULL,
  `idsource` int(10) unsigned DEFAULT NULL,
  `idmissingvalue` int(10) unsigned NULL DEFAULT NULL,
  PRIMARY KEY (`iddatacube`),
  CONSTRAINT `idsource_d` FOREIGN KEY (`idsource`) REFERENCES `source` (`idsource`) ON DELETE SET NULL ON UPDATE CASCADE,
  KEY `idcontainer` (`idcontainer`),
  CONSTRAINT `idcontainer` FOREIGN KEY (`idcontainer`) REFERENCES `container` (`idcontainer`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`localhost`*/ /*!50003 TRIGGER TRIGGER_before_delete_datacube BEFORE DELETE ON datacube 
FOR EACH ROW BEGIN
UPDATE IGNORE host SET importcount = IF(importcount > 0, importcount - 1, 0) where idhost in (select distinct(idhost) from imported where imported.iddatacube = OLD.iddatacube);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Dumping data for table `datacube`
--

LOCK TABLES `datacube` WRITE;
/*!40000 ALTER TABLE `datacube` DISABLE KEYS */;
/*!40000 ALTER TABLE `datacube` ENABLE KEYS */;
UNLOCK TABLES;


--
-- Table structure for table `session`
--

DROP TABLE IF EXISTS `session`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `session` (
  `idsession` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `iduser` int(10) unsigned NOT NULL,
  `idfolder` int(10) unsigned DEFAULT NULL,
  `sessionid` varchar(1024) NOT NULL,
  `label` varchar(256) DEFAULT NULL,
  `creationdate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`idsession`),
  CONSTRAINT `iduser_s` FOREIGN KEY (`iduser`) REFERENCES `user` (`iduser`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `idfolder_s` FOREIGN KEY (`idfolder`) REFERENCES `folder` (`idfolder`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `session`
--

LOCK TABLES `session` WRITE;
/*!40000 ALTER TABLE `session` DISABLE KEYS */;
/*!40000 ALTER TABLE `session` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `job`
--

DROP TABLE IF EXISTS `job`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `job` (
  `idjob` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `idparent` int(10) unsigned DEFAULT NULL,
  `markerid` int(10) unsigned NOT NULL,
  `workflowid` int(10) unsigned DEFAULT NULL,
  `idsession` int(10) unsigned DEFAULT NULL,
  `iduser` int(10) unsigned NOT NULL,
  `creationdate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `status` varchar(64) NOT NULL,
  `submissionstring` varchar(2048) DEFAULT NULL,
  `timestart` timestamp NULL DEFAULT NULL,
  `timeend` timestamp NULL DEFAULT NULL,
  `nchildrentotal` int(10) unsigned DEFAULT NULL,
  `nchildrencompleted` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`idjob`),
  CONSTRAINT `idparent_j` FOREIGN KEY (`idparent`) REFERENCES `job` (`idjob`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `iduser_j` FOREIGN KEY (`iduser`) REFERENCES `user` (`iduser`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `idsession_j` FOREIGN KEY (`idsession`) REFERENCES `session` (`idsession`) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `job`
--

LOCK TABLES `job` WRITE;
/*!40000 ALTER TABLE `job` DISABLE KEYS */;
/*!40000 ALTER TABLE `job` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `jobaccounting`
--

DROP TABLE IF EXISTS `jobaccounting`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobaccounting` (
  `idjob` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `idparent` int(10) unsigned DEFAULT NULL,
  `markerid` int(10) unsigned NOT NULL,
  `workflowid` int(10) unsigned DEFAULT NULL,
  `idsession` int(10) unsigned DEFAULT NULL,
  `iduser` int(10) unsigned DEFAULT NULL,
  `creationdate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `status` varchar(64) NOT NULL,
  `submissionstring` varchar(2048) DEFAULT NULL,
  `timestart` timestamp NULL DEFAULT NULL,
  `timeend` timestamp NULL DEFAULT NULL,
  `nchildrentotal` int(10) unsigned DEFAULT NULL,
  `nchildrencompleted` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`idjob`),
  CONSTRAINT `idparent_ja` FOREIGN KEY (`idparent`) REFERENCES `jobaccounting` (`idjob`) ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT `iduser_ja` FOREIGN KEY (`iduser`) REFERENCES `user` (`iduser`) ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT `idsession_ja` FOREIGN KEY (`idsession`) REFERENCES `session` (`idsession`) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `jobaccounting`
--

LOCK TABLES `jobaccounting` WRITE;
/*!40000 ALTER TABLE `jobaccounting` DISABLE KEYS */;
/*!40000 ALTER TABLE `jobaccounting` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `task`
--

DROP TABLE IF EXISTS `task`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `task` (
  `idtask` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `idoutputcube` int(10) unsigned NOT NULL,
  `idjob` int(10) unsigned DEFAULT NULL,
  `inputnumber` int(10) unsigned NOT NULL DEFAULT 1,
  `operation` varchar(256) NOT NULL,
  `query` varchar(2048) DEFAULT NULL,
  PRIMARY KEY (`idtask`),
  CONSTRAINT `idjob_j` FOREIGN KEY (`idjob`) REFERENCES `job` (`idjob`) ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT `idouputcube_t` FOREIGN KEY (`idoutputcube`) REFERENCES `datacube` (`iddatacube`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `task`
--

LOCK TABLES `task` WRITE;
/*!40000 ALTER TABLE `task` DISABLE KEYS */;
/*!40000 ALTER TABLE `task` ENABLE KEYS */;
UNLOCK TABLES;


--
-- Table structure for table `hasinput`
--

DROP TABLE IF EXISTS `hasinput`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `hasinput` (
  `idhasinput` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `iddatacube` int(10) unsigned NOT NULL,
  `idtask` int(10) unsigned NOT NULL,
  PRIMARY KEY (`idhasinput`),
  CONSTRAINT `iddatacube_h` FOREIGN KEY (`iddatacube`) REFERENCES `datacube` (`iddatacube`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `idtask_h` FOREIGN KEY (`idtask`) REFERENCES `task` (`idtask`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `hasinput`
--

LOCK TABLES `hasinput` WRITE;
/*!40000 ALTER TABLE `hasinput` DISABLE KEYS */;
/*!40000 ALTER TABLE `hasinput` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `partitioned`
--

DROP TABLE IF EXISTS `partitioned`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `partitioned` (
  `idpartitioned` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `iddbinstance` int(10) unsigned NOT NULL,
  `iddatacube` int(10) unsigned NOT NULL,
  PRIMARY KEY (`idpartitioned`),
  UNIQUE KEY `db_datacube` (`iddbinstance`, `iddatacube`),
  KEY `iddbinstance` (`iddbinstance`),
  CONSTRAINT `iddbinstance_p` FOREIGN KEY (`iddbinstance`) REFERENCES `dbinstance` (`iddbinstance`) ON DELETE CASCADE ON UPDATE CASCADE,
  KEY `iddatacube` (`iddatacube`),
  CONSTRAINT `iddatacube_p` FOREIGN KEY (`iddatacube`) REFERENCES `datacube` (`iddatacube`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `partitioned`
--

LOCK TABLES `partitioned` WRITE;
/*!40000 ALTER TABLE `partitioned` DISABLE KEYS */;
/*!40000 ALTER TABLE `partitioned` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `imported`
--

DROP TABLE IF EXISTS `imported`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `imported` (
  `idimported` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `idhost` int(10) unsigned NOT NULL,
  `iddatacube` int(10) unsigned NOT NULL,
  PRIMARY KEY (`idimported`),
  UNIQUE KEY `host_datacube` (`idhost`, `iddatacube`),
  KEY `idhost` (`idhost`),
  CONSTRAINT `idhost_i` FOREIGN KEY (`idhost`) REFERENCES `host` (`idhost`) ON DELETE CASCADE ON UPDATE CASCADE,
  KEY `iddatacube` (`iddatacube`),
  CONSTRAINT `iddatacube_i` FOREIGN KEY (`iddatacube`) REFERENCES `datacube` (`iddatacube`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `imported`
--

LOCK TABLES `imported` WRITE;
/*!40000 ALTER TABLE `imported` DISABLE KEYS */;
/*!40000 ALTER TABLE `imported` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `hierarchy`
--

DROP TABLE IF EXISTS `hierarchy`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `hierarchy` (
  `idhierarchy` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `hierarchyname` varchar(64) NOT NULL,
  `filename` varchar(256) NOT NULL,  
  `description` varchar(2048) DEFAULT NULL,
  PRIMARY KEY (`idhierarchy`),
  UNIQUE KEY (`hierarchyname`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `hierarchy`
--

LOCK TABLES `hierarchy` WRITE;
/*!40000 ALTER TABLE `hierarchy` DISABLE KEYS */;
INSERT INTO hierarchy (`hierarchyname`, `filename`) VALUES ('oph_base', 'OPH_BASE_hierarchy_1.0.xml'), ('oph_time', 'OPH_TIME_hierarchy_1.0.xml');
/*!40000 ALTER TABLE `hierarchy` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dimension`
--

DROP TABLE IF EXISTS `dimension`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dimension` (
  `iddimension` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `idcontainer` int(10) unsigned NOT NULL,
  `dimensionname` varchar(256) NOT NULL,
  `dimensiontype` varchar(64) NOT NULL,
  `idhierarchy` int(10) unsigned NOT NULL,
  `basetime` datetime NULL DEFAULT NULL,
  `units` varchar(64) NULL DEFAULT NULL,
  `calendar` varchar(64) NULL DEFAULT NULL,
  `monthlengths` varchar(64) NULL DEFAULT NULL,
  `leapyear` int(10) NULL DEFAULT NULL,
  `leapmonth` int(10) unsigned NULL DEFAULT NULL,
  PRIMARY KEY (`iddimension`),
  UNIQUE KEY `container_dimensionname` (`idcontainer`, `dimensionname`), 
  KEY `idcontainer` (`idcontainer`),
  CONSTRAINT `idcontainer_d` FOREIGN KEY (`idcontainer`) REFERENCES `container` (`idcontainer`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `idhierarchy_d` FOREIGN KEY (`idhierarchy`) REFERENCES `hierarchy` (`idhierarchy`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dimension`
--

LOCK TABLES `dimension` WRITE;
/*!40000 ALTER TABLE `dimension` DISABLE KEYS */;
/*!40000 ALTER TABLE `dimension` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `grid`
--

DROP TABLE IF EXISTS `grid`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `grid` (
  `idgrid` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `gridname` varchar(256) NOT NULL,
  `enabled` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`idgrid`),
  UNIQUE KEY (`gridname`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `grid`
--

LOCK TABLES `grid` WRITE;
/*!40000 ALTER TABLE `grid` DISABLE KEYS */;
/*!40000 ALTER TABLE `grid` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dimensioninstance`
--

DROP TABLE IF EXISTS `dimensioninstance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dimensioninstance` (
  `iddimensioninstance` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `iddimension` int(10) unsigned NOT NULL,
  `idgrid` int(10) unsigned default NULL,
  `size` int(10) NOT NULL,
  `fkiddimensionindex` int(10) unsigned NOT NULL,
  `fkiddimensionlabel` int(10) unsigned DEFAULT NULL,
  `conceptlevel` char(1) NOT NULL,
  `unlimited` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`iddimensioninstance`),
  CONSTRAINT `iddimension_di` FOREIGN KEY (`iddimension`) REFERENCES `dimension` (`iddimension`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `idgrid_di` FOREIGN KEY (`idgrid`) REFERENCES `grid` (`idgrid`) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dimensioninstance`
--

LOCK TABLES `dimensioninstance` WRITE;
/*!40000 ALTER TABLE `dimensioninstance` DISABLE KEYS */;
/*!40000 ALTER TABLE `dimensioninstance` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cubehasdim`
--

DROP TABLE IF EXISTS `cubehasdim`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cubehasdim` (
  `idcubehasdim` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `iddimensioninstance` int(10) unsigned NOT NULL,
  `iddatacube` int(10) unsigned NOT NULL,
  `explicit` tinyint(1) NOT NULL,
  `level` int(10) NOT NULL,
  PRIMARY KEY (`idcubehasdim`),
  UNIQUE KEY `datacube_dimensioninstance` (`iddatacube`, `iddimensioninstance`), 
  KEY `iddimensioninstance` (`iddimensioninstance`),
  CONSTRAINT `iddimension_cd` FOREIGN KEY (`iddimensioninstance`) REFERENCES `dimensioninstance` (`iddimensioninstance`) ON DELETE CASCADE ON UPDATE CASCADE,
  KEY `iddatacube` (`iddatacube`),
  CONSTRAINT `iddatacube_cd` FOREIGN KEY (`iddatacube`) REFERENCES `datacube` (`iddatacube`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cubehasdim`
--

LOCK TABLES `cubehasdim` WRITE;
/*!40000 ALTER TABLE `cubehasdim` DISABLE KEYS */;
/*!40000 ALTER TABLE `cubehasdim` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `vocabulary`
--

DROP TABLE IF EXISTS `vocabulary`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vocabulary` (
  `idvocabulary` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(256) NOT NULL,
  `description` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`idvocabulary`),
  UNIQUE KEY (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vocabulary`
--

LOCK TABLES `vocabulary` WRITE;
/*!40000 ALTER TABLE `vocabulary` DISABLE KEYS */;
INSERT INTO `vocabulary` (`idvocabulary`, `name`, `description`) VALUES (1, 'CF', 'CF base vocabulary');
/*!40000 ALTER TABLE `vocabulary` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `container`
--

DROP TABLE IF EXISTS `container`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `container` (
  `idcontainer` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `idfolder` int(10) unsigned NOT NULL,
  `idparent` int(10) unsigned DEFAULT NULL,
  `containername` varchar(256) NOT NULL,
  `operator` varchar(256) DEFAULT NULL,
  `description` varchar(2048) DEFAULT NULL,
  `idvocabulary` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`idcontainer`),
  UNIQUE KEY `folder_containername` (`idfolder`, `containername`),
  KEY `idfolder` (`idfolder`),
  CONSTRAINT `idfolder_c` FOREIGN KEY (`idfolder`) REFERENCES `folder` (`idfolder`) ON DELETE CASCADE ON UPDATE CASCADE,
  KEY `idparent` (`idparent`),
  CONSTRAINT `idparent_c` FOREIGN KEY (`idparent`) REFERENCES `container` (`idcontainer`) ON DELETE CASCADE ON UPDATE CASCADE,
  KEY `idvocabulary` (`idvocabulary`),
  CONSTRAINT `idvocabulary_c` FOREIGN KEY (`idvocabulary`) REFERENCES `vocabulary` (`idvocabulary`) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `container`
--

LOCK TABLES `container` WRITE;
/*!40000 ALTER TABLE `container` DISABLE KEYS */;
/*!40000 ALTER TABLE `container` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `metadatakey`
--

DROP TABLE IF EXISTS `metadatakey`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `metadatakey` (
  `idkey` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `idvocabulary` int(10) unsigned DEFAULT NULL,
  `label` varchar(256) NOT NULL,
  `template` ENUM('time:frequency','time:units','time:calendar','time:month_lengths','time:leap_year','time:leap_month') DEFAULT NULL,
  `description` varchar(256) DEFAULT NULL,
  `variable` varchar(256) DEFAULT NULL,
  `required` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`idkey`),
  FOREIGN KEY (`idvocabulary`) REFERENCES `vocabulary` (`idvocabulary`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `metadatakey`
--

LOCK TABLES `metadatakey` WRITE;
/*!40000 ALTER TABLE `metadatakey` DISABLE KEYS */;
INSERT INTO `metadatakey` (`idkey`, `idvocabulary`, `label`, `template`, `variable`, `required`) VALUES (1, 1, 'frequency', 'time:frequency', NULL, 1), (2, 1, 'units', 'time:units', 'time', 1), (3, 1, 'calendar', 'time:calendar', 'time', 1);
/*!40000 ALTER TABLE `metadatakey` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `metadatatype`
--

DROP TABLE IF EXISTS `metadatatype`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `metadatatype` (
  `idtype` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(256) NOT NULL,
  PRIMARY KEY (`idtype`),
  UNIQUE KEY (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `metadatatype`
--

LOCK TABLES `metadatatype` WRITE;
/*!40000 ALTER TABLE `metadatatype` DISABLE KEYS */;
INSERT INTO `metadatatype` (`idtype`, `name`) VALUES (1, 'text'), (2, 'short'), (3, 'int'), (4, 'long'), (5, 'float'), (6, 'double'), (7, 'image'), (8, 'video'), (9, 'audio'), (10, 'url');
/*!40000 ALTER TABLE `metadatatype` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `metadatainstance`
--

DROP TABLE IF EXISTS `metadatainstance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `metadatainstance` (
  `idmetadatainstance` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `iddatacube` int(10) unsigned NOT NULL,
  `idkey` int(10) unsigned DEFAULT NULL,
  `idtype` int(10) unsigned NOT NULL,
  `label` varchar(256) NOT NULL,
  `variable` varchar(256) DEFAULT NULL,
  `value` LONGBLOB NOT NULL,
  `size` int(10) unsigned NOT NULL DEFAULT 1,
  PRIMARY KEY (`idmetadatainstance`),
  UNIQUE KEY `datacube_metadatakey` (`iddatacube`, `idkey`),
  FOREIGN KEY (`iddatacube`) REFERENCES `datacube` (`iddatacube`) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (`idkey`) REFERENCES `metadatakey` (`idkey`) ON DELETE SET NULL ON UPDATE CASCADE,
  FOREIGN KEY (`idtype`) REFERENCES `metadatatype` (`idtype`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `metadatainstance`
--

LOCK TABLES `metadatainstance` WRITE;
/*!40000 ALTER TABLE `metadatainstance` DISABLE KEYS */;
/*!40000 ALTER TABLE `metadatainstance` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `manage`
--

DROP TABLE IF EXISTS `manage`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `manage` (
  `iduser` int(10) unsigned NOT NULL,
  `idmetadatainstance` int(10) unsigned NOT NULL,
  `managedate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT `idmanage` PRIMARY KEY (`iduser`,`idmetadatainstance`,`managedate`),
  FOREIGN KEY (`iduser`) REFERENCES `user` (`iduser`) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (`idmetadatainstance`) REFERENCES `metadatainstance` (`idmetadatainstance`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `manage`
--

LOCK TABLES `manage` WRITE;
/*!40000 ALTER TABLE `manage` DISABLE KEYS */;
/*!40000 ALTER TABLE `manage` ENABLE KEYS */;
UNLOCK TABLES;

/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2014-13-08 22:09:07
