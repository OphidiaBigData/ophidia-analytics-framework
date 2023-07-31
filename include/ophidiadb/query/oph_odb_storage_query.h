/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2023 CMCC Foundation

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef __OPH_ODB_STGE_QUERY_H__
#define __OPH_ODB_STGE_QUERY_H__

#define MYSQL_QUERY_STGE_RETRIEVE_DBMS 				"SELECT iddbmsinstance, hostname, login, password, port, ioservertype from `dbmsinstance` INNER JOIN host on dbmsinstance.idhost = host.idhost where iddbmsinstance = %d AND status = 'up';"
#define MYSQL_QUERY_STGE_RETRIEVE_FIRST_DBMS 			"SELECT iddbmsinstance from `dbmsinstance` INNER JOIN host on dbmsinstance.idhost = host.idhost WHERE status = 'up' AND dbmsinstance.ioservertype = 'mysql_table' LIMIT 1;"
#define MYSQL_QUERY_STGE_RETRIEVE_DB_CONNECTION 		"SELECT dbmsinstance.iddbmsinstance, login, password, port, hostname, ioservertype, dbinstance.iddbinstance, dbname FROM dbmsinstance INNER JOIN host ON dbmsinstance.idhost=host.idhost INNER JOIN dbinstance on dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance INNER JOIN partitioned on dbinstance.iddbinstance=partitioned.iddbinstance WHERE iddatacube = %d AND status = 'up' ORDER BY host.idhost, dbmsinstance.iddbmsinstance, dbinstance.iddbinstance ASC LIMIT %d, %d;"
#define MYSQL_QUERY_STGE_RETRIEVE_DBMS_CONNECTION 		"SELECT DISTINCT dbmsinstance.iddbmsinstance, login, password, port, hostname, ioservertype FROM dbmsinstance INNER JOIN host ON dbmsinstance.idhost=host.idhost INNER JOIN dbinstance on dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance INNER JOIN partitioned on dbinstance.iddbinstance=partitioned.iddbinstance WHERE iddatacube = %d AND status = 'up' ORDER BY host.idhost, dbmsinstance.iddbmsinstance ASC LIMIT %d, %d;"
#define MYSQL_QUERY_STGE_RETRIEVE_FRAG_CONNECTION 		"SELECT fragmentname, fragment.iddatacube, fragrelativeindex, fragment.idfragment, keystart, keyend, dbmsinstance.iddbmsinstance, login, password, port, hostname,  ioservertype, dbinstance.iddbinstance, dbname FROM dbmsinstance INNER JOIN host ON dbmsinstance.idhost=host.idhost INNER JOIN dbinstance on dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance INNER JOIN fragment on fragment.iddbinstance=dbinstance.iddbinstance WHERE (%s) AND fragment.iddatacube = %d AND status = 'up' ORDER BY host.idhost, dbmsinstance.iddbmsinstance, dbinstance.iddbinstance, fragment.fragrelativeindex ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_FRAG_KEYS 			"SELECT idfragment, fragrelativeindex, keystart, keyend FROM fragment WHERE iddatacube = %d ORDER BY fragrelativeindex ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_FRAG_KEYS2 			"SELECT idfragment, fragrelativeindex, keystart, keyend, idhost, dbmsinstance.iddbmsinstance, dbinstance.iddbinstance FROM dbmsinstance INNER JOIN dbinstance ON dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance INNER JOIN fragment ON fragment.iddbinstance=dbinstance.iddbinstance WHERE iddatacube = %d ORDER BY fragrelativeindex ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_INFO_LIST_2 			"SELECT containername FROM container LEFT JOIN datacube ON container.idcontainer = datacube.idcontainer  WHERE datacube.iddatacube = %d;"
#define MYSQL_QUERY_STGE_RETRIEVE_INFO_LIST_3 			"SELECT DISTINCT containername, hostname, status FROM container INNER JOIN datacube ON container.idcontainer = datacube.idcontainer  INNER JOIN partitioned ON datacube.iddatacube=partitioned.iddatacube INNER JOIN dbinstance ON partitioned.iddbinstance=dbinstance.iddbinstance INNER JOIN dbmsinstance ON dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance INNER JOIN host ON dbmsinstance.idhost=host.idhost WHERE datacube.iddatacube = %d %s ORDER BY host.idhost ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_INFO_LIST_4 			"SELECT DISTINCT containername, hostname, status, dbmsinstance.iddbmsinstance FROM container INNER JOIN datacube ON container.idcontainer = datacube.idcontainer INNER JOIN partitioned ON datacube.iddatacube=partitioned.iddatacube INNER JOIN dbinstance ON partitioned.iddbinstance=dbinstance.iddbinstance INNER JOIN dbmsinstance ON dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance INNER JOIN host ON dbmsinstance.idhost=host.idhost WHERE datacube.iddatacube = %d %s ORDER BY host.idhost, dbmsinstance.iddbmsinstance ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_INFO_LIST_5 			"SELECT containername, hostname, status, dbmsinstance.iddbmsinstance, dbname FROM container INNER JOIN datacube ON container.idcontainer = datacube.idcontainer INNER JOIN partitioned ON datacube.iddatacube=partitioned.iddatacube INNER JOIN dbinstance ON partitioned.iddbinstance=dbinstance.iddbinstance INNER JOIN dbmsinstance ON dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance INNER JOIN host ON dbmsinstance.idhost=host.idhost WHERE datacube.iddatacube = %d %s ORDER BY host.idhost, dbmsinstance.iddbmsinstance, dbinstance.dbname ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_INFO_LIST_6 			"SELECT containername, hostname, status, dbmsinstance.iddbmsinstance, dbname, fragmentname FROM container INNER JOIN datacube ON container.idcontainer = datacube.idcontainer INNER JOIN partitioned ON datacube.iddatacube=partitioned.iddatacube INNER JOIN dbinstance ON partitioned.iddbinstance=dbinstance.iddbinstance INNER JOIN dbmsinstance ON dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance INNER JOIN host ON dbmsinstance.idhost=host.idhost INNER JOIN fragment ON datacube.iddatacube=fragment.iddatacube AND fragment.iddbinstance=dbinstance.iddbinstance WHERE datacube.iddatacube = %d %s ORDER BY host.idhost, dbmsinstance.iddbmsinstance, dbinstance.dbname, fragment.idfragment ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_INSTANCES_LIST_1 		"SELECT idhost,hostname,memory,cores,status FROM host %s ORDER BY hostname ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_INSTANCES_LIST_2 		"SELECT iddbmsinstance, hostname, status, port, ioservertype  FROM dbmsinstance INNER JOIN host ON dbmsinstance.idhost = host.idhost %s ORDER BY hostname, iddbmsinstance ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_INSTANCES_LIST_3 		"SELECT partitionname, hostname, status, reserved FROM hostpartition INNER JOIN hashost ON hashost.idhostpartition = hostpartition.idhostpartition INNER JOIN host ON host.idhost = hashost.idhost WHERE (iduser IS NULL OR iduser = %d) %s ORDER BY hostname ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_INSTANCES_LIST_3_PART 	"SELECT partitionname, iduser, reserved FROM hostpartition WHERE NOT hidden AND (iduser IS NULL OR iduser = %d) ORDER BY partitionname ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_FRAGMENTE_NAME_LIST 		"SELECT fragmentname FROM dbinstance INNER JOIN fragment on fragment.iddbinstance=dbinstance.iddbinstance WHERE iddbmsinstance = %d AND iddatacube = %d ORDER BY iddbmsinstance, dbinstance.iddbinstance, fragment.fragrelativeindex ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_FRAGMENTE_NAME_LIST_LIMIT	"SELECT fragmentname FROM dbinstance INNER JOIN fragment on fragment.iddbinstance=dbinstance.iddbinstance WHERE iddbmsinstance = %d AND iddatacube = %d ORDER BY iddbmsinstance, dbinstance.iddbinstance, fragment.fragrelativeindex ASC LIMIT %d, %d;"
#define MYSQL_QUERY_STGE_RETRIEVE_FRAG 				"SELECT idfragment, iddatacube, iddbinstance, fragrelativeindex, fragmentname, keystart, keyend FROM fragment WHERE fragmentname = '%s';"
#define MYSQL_QUERY_STGE_RETRIEVE_DB 				"SELECT dbmsinstance.iddbmsinstance, login, password, port, hostname, ioservertype, dbinstance.iddbinstance, dbname FROM dbmsinstance INNER JOIN host ON dbmsinstance.idhost=host.idhost INNER JOIN dbinstance on dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance WHERE dbinstance.iddbinstance = %d AND status = 'up';"
#define MYSQL_QUERY_STGE_UPDATE_OPHIDIADB_FRAG 			"INSERT INTO `fragment` (`iddbinstance`, `iddatacube`, `fragrelativeindex`, `fragmentname`, `keystart`, `keyend`) VALUES (%d, %d, %d, '%s', %d, %d)"
#define MYSQL_QUERY_STGE_UPDATE_OPHIDIADB_FRAG2 			"INSERT INTO `fragment` (`iddbinstance`, `iddatacube`, `fragrelativeindex`, `fragmentname`, `keystart`, `keyend`) VALUES %s"
#define MYSQL_QUERY_STGE_RETRIEVE_CONTAINER_FROM_FRAGMENT	 "SELECT idcontainer from fragment INNER JOIN datacube on datacube.iddatacube = fragment.iddatacube where fragmentname = '%s';"
#define MYSQL_QUERY_STGE_UPDATE_OPHIDIADB_DB 			"INSERT INTO `dbinstance` (`iddbmsinstance`, `dbname`) VALUES (%d, '%s')"
#define MYSQL_QUERY_STGE_UPDATE_OPHIDIADB_PART 			"INSERT INTO `partitioned` (`iddbinstance`, `iddatacube`) VALUES (%d, %d)"

#define MYSQL_QUERY_STGE_RETRIEVE_PARTITIONED_DB 		"SELECT iddbinstance from `partitioned` where iddatacube = %d"
#define MYSQL_QUERY_STGE_RETRIEVE_PARTITIONED_DATACUBE 		"SELECT iddatacube from `partitioned` where iddbinstance = %d"
#define MYSQL_STGE_FRAGMENT_RELATIVE_INDEX_COND1 		"fragrelativeindex = %d"
#define MYSQL_STGE_FRAGMENT_RELATIVE_INDEX_COND2 		"fragrelativeindex >= %d AND fragrelativeindex <= %d"

#define MYSQL_QUERY_STGE_RETRIEVE_HOSTPARTITION_FS      	"SELECT hostpartition.idhostpartition, COUNT(host.idhost) AS hosts, SUM(host.importcount) AS importcounts FROM hostpartition INNER JOIN hashost ON hostpartition.idhostpartition = hashost.idhostpartition INNER JOIN host ON host.idhost = hashost.idhost INNER JOIN dbmsinstance ON dbmsinstance.idhost = host.idhost WHERE status = 'up' AND ioservertype = '%s' AND (NOT reserved OR iduser = %d) GROUP BY hostpartition.idhostpartition HAVING hosts >= %d ORDER BY importcounts DESC, hosts LIMIT 1;"

#define MYSQL_QUERY_STGE_RETRIEVE_HOST_DBMS_NUMBER		"SELECT count(iddbmsinstance) FROM hashost INNER JOIN host ON host.idhost = hashost.idhost INNER JOIN dbmsinstance ON dbmsinstance.idhost = host.idhost WHERE idhostpartition = %d AND status = 'up' AND ioservertype = '%s' GROUP BY host.idhost;"

#define MYSQL_QUERY_STGE_RETRIEVE_COUNT_HOST_DBMS		"SELECT count(dbms) FROM (SELECT count(iddbmsinstance) AS dbms FROM hashost INNER JOIN host ON host.idhost = hashost.idhost INNER JOIN dbmsinstance ON dbmsinstance.idhost = host.idhost WHERE idhostpartition = %d AND status = 'up' AND ioservertype = '%s' GROUP BY host.idhost)tmp;"

#define MYSQL_QUERY_STGE_DELETE_OPHIDIADB_DB 			"DELETE FROM `dbinstance` WHERE `iddbinstance` = %d"

#define MYSQL_QUERY_STGE_RETRIEVE_FRAG_ID 			"SELECT idfragment from `fragment` where fragmentname = '%s'"

#define MYSQL_QUERY_STGE_RETRIEVE_DB_ID 			"SELECT iddbinstance from `dbinstance` where dbname = '%s'"

#define MYSQL_QUERY_STGE_RETRIEVE_DATACUBEXDB_NUMBER 		"SELECT COUNT(*) FROM partitioned WHERE iddbinstance = %d;"
#define MYSQL_QUERY_STGE_RETRIEVE_DATACUBEXDBS_NUMBER 		"SELECT COUNT(*) FROM partitioned WHERE iddbinstance IN (%s) GROUP BY iddbinstance;"

#define MYSQL_QUERY_STGE_RETRIEVE_DBMS_LIST 		"SELECT host.idhost, iddbmsinstance FROM hashost INNER JOIN host ON host.idhost = hashost.idhost INNER JOIN dbmsinstance ON dbmsinstance.idhost = host.idhost WHERE idhostpartition = %d AND status = 'up' AND ioservertype = '%s' "
#define MYSQL_STGE_POLICY_RR						"ORDER BY CASE %d WHEN 0 THEN host.importcount ELSE hashost.importcount END, hostname, port LIMIT %d FOR UPDATE;"
#define MYSQL_STGE_POLICY_PORT						"ORDER BY hostname, port LIMIT %d FOR UPDATE;"

#define MYSQL_QUERY_STGE_UPDATE_IMPORT_COUNT 		"UPDATE host SET importcount = importcount + 1 WHERE idhost IN (SELECT imported.idhost AS idhost FROM imported WHERE imported.iddatacube = %d);"

#define MYSQL_QUERY_STGE_CREATE_PARTITION			"INSERT IGNORE INTO hostpartition (partitionname, iduser, reserved, hosts) VALUES ('%s', %d, %d, %d);"
#define MYSQL_QUERY_STGE_ADD_HOST_TO_PARTITION		"INSERT INTO hashost (idhostpartition, idhost) VALUES (%d, %d);"
#define MYSQL_QUERY_STGE_ADD_ALL_HOSTS_TO_PARTITION	"INSERT INTO hashost (idhostpartition, idhost) SELECT %d, idhost FROM host;"
#define MYSQL_QUERY_STGE_ADD_SOME_HOSTS_TO_PARTITION	"INSERT INTO hashost (idhostpartition, idhost) SELECT %d, idhost FROM host ORDER BY importcount ASC LIMIT %d;"
#define MYSQL_QUERY_STGE_DELETE_PARTITION			"DELETE FROM hostpartition WHERE partitionname = '%s' AND iduser = %d %s;"
#define MYSQL_QUERY_STGE_DELETE_PARTITION2			"DELETE FROM hostpartition WHERE idhostpartition = %d;"

#define MYSQL_QUERY_STGE_CHECK_ALL_HOSTS			"SELECT MAX(nhosts) AS mhosts FROM (SELECT COUNT(*) AS nhosts FROM hashost INNER JOIN hostpartition ON hashost.idhostpartition = hostpartition.idhostpartition WHERE NOT hidden GROUP BY idhost) AS chosts;"
#define MYSQL_QUERY_STGE_CHECK_HOST					"SELECT COUNT(*) FROM hashost INNER JOIN hostpartition ON hashost.idhostpartition = hostpartition.idhostpartition WHERE NOT hidden AND idhost = %d;"

#define MYSQL_QUERY_STGE_CONNECT_CUBE_TO_HOST		"INSERT INTO imported (idhost, iddatacube) VALUES %s;"

#define MYSQL_QUERY_STGE_GET_IDPARTITION_FROM_NAME	"SELECT idhostpartition, hidden FROM hostpartition WHERE partitionname = '%s' AND (NOT reserved OR iduser = %d);"

#endif				/* __OPH_ODB_STGE_QUERY_H__ */
