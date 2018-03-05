/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2017 CMCC Foundation

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

#define MYSQL_QUERY_STGE_RETRIEVE_DBMS 				"SELECT iddbmsinstance, hostname, login, password, port, fstype, ioservertype from `dbmsinstance` INNER JOIN host on dbmsinstance.idhost = host.idhost where iddbmsinstance = %d AND host.status = 'up' AND dbmsinstance.status = 'up';"
#define MYSQL_QUERY_STGE_RETRIEVE_FIRST_DBMS 			"SELECT iddbmsinstance from `dbmsinstance` INNER JOIN host on dbmsinstance.idhost = host.idhost where host.status = 'up' AND dbmsinstance.status = 'up' AND dbmsinstance.ioservertype = 'mysql_table' LIMIT 1;"
#define MYSQL_QUERY_STGE_RETRIEVE_DB_CONNECTION 		"SELECT dbmsinstance.iddbmsinstance, login, password, port, hostname, ioservertype, dbinstance.iddbinstance, dbname FROM dbmsinstance INNER JOIN host ON dbmsinstance.idhost=host.idhost INNER JOIN dbinstance on dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance INNER JOIN partitioned on dbinstance.iddbinstance=partitioned.iddbinstance WHERE iddatacube = %d AND host.status = 'up' AND dbmsinstance.status = 'up' ORDER BY host.idhost, dbmsinstance.iddbmsinstance, dbinstance.iddbinstance ASC LIMIT %d, %d;"
#define MYSQL_QUERY_STGE_RETRIEVE_DBMS_CONNECTION 		"SELECT DISTINCT dbmsinstance.iddbmsinstance, login, password, port, hostname, ioservertype FROM dbmsinstance INNER JOIN host ON dbmsinstance.idhost=host.idhost INNER JOIN dbinstance on dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance INNER JOIN partitioned on dbinstance.iddbinstance=partitioned.iddbinstance WHERE iddatacube = %d AND host.status = 'up' AND dbmsinstance.status = 'up' ORDER BY host.idhost, dbmsinstance.iddbmsinstance ASC LIMIT %d, %d;"
#define MYSQL_QUERY_STGE_RETRIEVE_FRAG_CONNECTION 		"SELECT fragmentname, fragment.iddatacube, fragrelativeindex, fragment.idfragment, keystart, keyend, dbmsinstance.iddbmsinstance, login, password, port, hostname,  ioservertype, dbinstance.iddbinstance, dbname FROM dbmsinstance INNER JOIN host ON dbmsinstance.idhost=host.idhost INNER JOIN dbinstance on dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance INNER JOIN fragment on fragment.iddbinstance=dbinstance.iddbinstance WHERE (%s) AND fragment.iddatacube = %d AND host.status = 'up' AND dbmsinstance.status = 'up' ORDER BY host.idhost, dbmsinstance.iddbmsinstance, dbinstance.iddbinstance, fragment.fragrelativeindex ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_FRAG_KEYS 			"SELECT idfragment, fragrelativeindex, keystart, keyend FROM fragment WHERE iddatacube = %d ORDER BY fragrelativeindex ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_FRAG_KEYS2 			"SELECT idfragment, fragrelativeindex, keystart, keyend, idhost, dbmsinstance.iddbmsinstance, dbinstance.iddbinstance FROM dbmsinstance INNER JOIN dbinstance ON dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance INNER JOIN fragment ON fragment.iddbinstance=dbinstance.iddbinstance WHERE iddatacube = %d ORDER BY fragrelativeindex ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_INFO_LIST_2 			"SELECT containername, hidden FROM container LEFT JOIN datacube ON container.idcontainer = datacube.idcontainer  WHERE datacube.iddatacube = %d;"
#define MYSQL_QUERY_STGE_RETRIEVE_INFO_LIST_3 			"SELECT DISTINCT containername, hostname, host.status, hidden FROM container INNER JOIN datacube ON container.idcontainer = datacube.idcontainer  INNER JOIN partitioned ON datacube.iddatacube=partitioned.iddatacube INNER JOIN dbinstance ON partitioned.iddbinstance=dbinstance.iddbinstance INNER JOIN dbmsinstance ON dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance INNER JOIN host ON dbmsinstance.idhost=host.idhost WHERE datacube.iddatacube = %d %s ORDER BY host.idhost ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_INFO_LIST_4 			"SELECT DISTINCT containername, hostname, host.status, dbmsinstance.iddbmsinstance, dbmsinstance.status, hidden FROM container INNER JOIN datacube ON container.idcontainer = datacube.idcontainer INNER JOIN partitioned ON datacube.iddatacube=partitioned.iddatacube INNER JOIN dbinstance ON partitioned.iddbinstance=dbinstance.iddbinstance INNER JOIN dbmsinstance ON dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance INNER JOIN host ON dbmsinstance.idhost=host.idhost WHERE datacube.iddatacube = %d %s ORDER BY host.idhost, dbmsinstance.iddbmsinstance ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_INFO_LIST_5 			"SELECT containername, hostname, host.status, dbmsinstance.iddbmsinstance, dbmsinstance.status, dbname, hidden FROM container INNER JOIN datacube ON container.idcontainer = datacube.idcontainer INNER JOIN partitioned ON datacube.iddatacube=partitioned.iddatacube INNER JOIN dbinstance ON partitioned.iddbinstance=dbinstance.iddbinstance INNER JOIN dbmsinstance ON dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance INNER JOIN host ON dbmsinstance.idhost=host.idhost WHERE datacube.iddatacube = %d %s ORDER BY host.idhost, dbmsinstance.iddbmsinstance, dbinstance.dbname ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_INFO_LIST_6 			"SELECT containername, hostname, host.status, dbmsinstance.iddbmsinstance, dbmsinstance.status, dbname, fragmentname, hidden FROM container INNER JOIN datacube ON container.idcontainer = datacube.idcontainer INNER JOIN partitioned ON datacube.iddatacube=partitioned.iddatacube INNER JOIN dbinstance ON partitioned.iddbinstance=dbinstance.iddbinstance INNER JOIN dbmsinstance ON dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance INNER JOIN host ON dbmsinstance.idhost=host.idhost INNER JOIN fragment ON datacube.iddatacube=fragment.iddatacube AND fragment.iddbinstance=dbinstance.iddbinstance WHERE datacube.iddatacube = %d %s ORDER BY host.idhost, dbmsinstance.iddbmsinstance, dbinstance.dbname, fragment.idfragment ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_INSTANCES_LIST_1 		"SELECT idhost,hostname,memory,cores,status FROM host %s ORDER BY hostname ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_INSTANCES_LIST_2 		"SELECT iddbmsinstance, hostname, host.status, port, fstype, ioservertype, dbmsinstance.status  FROM dbmsinstance INNER JOIN host ON dbmsinstance.idhost = host.idhost %s ORDER BY hostname, iddbmsinstance ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_INSTANCES_LIST_3 		"SELECT partitionname, hostname, host.status FROM hostpartition INNER JOIN hashost ON hashost.idhostpartition = hostpartition.idhostpartition INNER JOIN host ON host.idhost = hashost.idhost WHERE (iduser IS NULL OR iduser = %d) %s ORDER BY hostname ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_INSTANCES_LIST_3_PART 	"SELECT partitionname, iduser FROM hostpartition WHERE hidden = 0 AND (iduser IS NULL OR iduser = %d) ORDER BY partitionname ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_FRAGMENTE_NAME_LIST 		"SELECT fragmentname FROM dbinstance INNER JOIN fragment on fragment.iddbinstance=dbinstance.iddbinstance WHERE iddbmsinstance = %d AND iddatacube = %d ORDER BY iddbmsinstance, dbinstance.iddbinstance, fragment.fragrelativeindex ASC;"
#define MYSQL_QUERY_STGE_RETRIEVE_FRAGMENTE_NAME_LIST_LIMIT	"SELECT fragmentname FROM dbinstance INNER JOIN fragment on fragment.iddbinstance=dbinstance.iddbinstance WHERE iddbmsinstance = %d AND iddatacube = %d ORDER BY iddbmsinstance, dbinstance.iddbinstance, fragment.fragrelativeindex ASC LIMIT %d, %d;"
#define MYSQL_QUERY_STGE_RETRIEVE_FRAG 				"SELECT idfragment, iddatacube, iddbinstance, fragrelativeindex, fragmentname, keystart, keyend FROM fragment WHERE fragmentname = '%s';"
#define MYSQL_QUERY_STGE_RETRIEVE_DB 				"SELECT dbmsinstance.iddbmsinstance, login, password, port, hostname, ioservertype, dbinstance.iddbinstance, dbname FROM dbmsinstance INNER JOIN host ON dbmsinstance.idhost=host.idhost INNER JOIN dbinstance on dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance WHERE dbinstance.iddbinstance = %d AND host.status = 'up' AND dbmsinstance.status = 'up';"
#define MYSQL_QUERY_STGE_UPDATE_OPHIDIADB_FRAG 			"INSERT INTO `fragment` (`iddbinstance`, `iddatacube`, `fragrelativeindex`, `fragmentname`, `keystart`, `keyend`) VALUES (%d, %d, %d, '%s', %d, %d)"
#define MYSQL_QUERY_STGE_RETRIEVE_CONTAINER_FROM_FRAGMENT	 "SELECT idcontainer from fragment INNER JOIN datacube on datacube.iddatacube = fragment.iddatacube where fragmentname = '%s';"
#define MYSQL_QUERY_STGE_UPDATE_OPHIDIADB_DB 			"INSERT INTO `dbinstance` (`iddbmsinstance`, `dbname`) VALUES (%d, '%s')"
#define MYSQL_QUERY_STGE_UPDATE_OPHIDIADB_PART 			"INSERT INTO `partitioned` (`iddbinstance`, `iddatacube`) VALUES (%d, %d)"

#define MYSQL_QUERY_STGE_RETRIEVE_PARTITIONED_DB 		"SELECT iddbinstance from `partitioned` where iddatacube = %d"
#define MYSQL_QUERY_STGE_RETRIEVE_PARTITIONED_DATACUBE 		"SELECT iddatacube from `partitioned` where iddbinstance = %d"
#define MYSQL_STGE_FRAGMENT_RELATIVE_INDEX_COND1 		"fragrelativeindex = %d"
#define MYSQL_STGE_FRAGMENT_RELATIVE_INDEX_COND2 		"fragrelativeindex >= %d AND fragrelativeindex <= %d"

#define MYSQL_QUERY_STGE_RETRIEVE_HOSTPARTITION_FS      	"SELECT tmp.partitionname, tmp.fstype FROM (SELECT hostpartition.idhostpartition, partitionname, host.idhost, fstype FROM hostpartition INNER JOIN hashost ON hostpartition.idhostpartition = hashost.idhostpartition INNER JOIN host ON host.idhost = hashost.idhost INNER JOIN dbmsinstance ON dbmsinstance.idhost=host.idhost WHERE partitionname like '%s' AND host.status = 'up' AND dbmsinstance.status = 'up' AND fstype like '%s' AND ioservertype='%s' GROUP BY fstype, host.idhost HAVING count(iddbmsinstance) >= %d) AS tmp GROUP BY tmp.fstype, tmp.idhostpartition HAVING count(tmp.idhost) >= %d limit 1;"

#define MYSQL_QUERY_STGE_RETRIEVE_HOST_DBMS_NUMBER		"SELECT count(iddbmsinstance) FROM hostpartition INNER JOIN hashost ON hostpartition.idhostpartition = hashost.idhostpartition INNER JOIN host ON host.idhost = hashost.idhost INNER JOIN dbmsinstance ON dbmsinstance.idhost=host.idhost WHERE partitionname = '%s' AND host.status = 'up' AND dbmsinstance.status = 'up' AND fstype=%d AND ioservertype='%s' GROUP BY host.idhost;"

#define MYSQL_QUERY_STGE_RETRIEVE_COUNT_HOST_DBMS		"SELECT count(dbms), min(dbms) FROM (SELECT count(iddbmsinstance) AS dbms FROM hostpartition INNER JOIN hashost ON hostpartition.idhostpartition = hashost.idhostpartition INNER JOIN host ON host.idhost = hashost.idhost INNER JOIN dbmsinstance ON dbmsinstance.idhost=host.idhost WHERE partitionname = '%s' AND host.status = 'up' AND dbmsinstance.status = 'up' AND fstype=%d AND ioservertype='%s' GROUP BY host.idhost)tmp;"

#define MYSQL_QUERY_STGE_DELETE_OPHIDIADB_DB 			"DELETE FROM `dbinstance` WHERE `iddbinstance` = %d"

#define MYSQL_QUERY_STGE_RETRIEVE_FRAG_ID 			"SELECT idfragment from `fragment` where fragmentname = '%s'"

#define MYSQL_QUERY_STGE_RETRIEVE_DB_ID 			"SELECT iddbinstance from `dbinstance` where dbname = '%s'"

#define MYSQL_QUERY_STGE_RETRIEVE_DATACUBEXDB_NUMBER 		"SELECT COUNT(*) FROM partitioned WHERE iddbinstance = %d;"

#define MYSQL_QUERY_STGE_RETRIEVE_DBMS_LIST 			"SELECT host.idhost, iddbmsinstance FROM hostpartition INNER JOIN hashost ON hostpartition.idhostpartition = hashost.idhostpartition INNER JOIN host ON host.idhost = hashost.idhost INNER JOIN dbmsinstance ON dbmsinstance.idhost = host.idhost WHERE partitionname = '%s' AND dbmsinstance.status = 'up' AND fstype = %d AND ioservertype = '%s' AND host.idhost IN ( SELECT tmp_id FROM ( SELECT host.idhost AS tmp_id, count(iddbmsinstance) AS tmp_count FROM hostpartition INNER JOIN hashost ON hostpartition.idhostpartition = hashost.idhostpartition INNER JOIN host ON host.idhost = hashost.idhost INNER JOIN dbmsinstance ON dbmsinstance.idhost = host.idhost WHERE partitionname = '%s' AND host.status = 'up' AND dbmsinstance.status = 'up' AND fstype = %d AND ioservertype = '%s' GROUP BY host.idhost ) AS tab WHERE tmp_count >= %d ) ORDER BY CASE hidden WHEN 0 THEN host.datacubecount ELSE hashost.datacubecount END, port;"

#define MYSQL_QUERY_STGE_RETRIVE_PARTITION_STORAGE_INSTANCES	"SELECT dbmsinstance.iddbmsinstance FROM hostpartition INNER JOIN hashost ON hostpartition.idhostpartition = hashost.idhostpartition INNER JOIN host ON host.idhost = hashost.idhost INNER JOIN dbmsinstance ON dbmsinstance.idhost=host.idhost WHERE partitionname = '%s' AND host.status = 'up' AND dbmsinstance.status = 'up' ORDER BY host.idhost, dbmsinstance.iddbmsinstance ASC;"

#define MYSQL_QUERY_STGE_CREATE_PARTITION			"INSERT IGNORE INTO hostpartition (partitionname, iduser) VALUES ('%s', %d);"
#define MYSQL_QUERY_STGE_ADD_HOST_TO_PARTITION		"INSERT INTO hashost (idhostpartition, idhost) VALUES (%d, %d);"
#define MYSQL_QUERY_STGE_ADD_ALL_HOSTS_TO_PARTITION	"INSERT INTO hashost (idhostpartition, idhost) SELECT %d, idhost FROM host;"
#define MYSQL_QUERY_STGE_DELETE_PARTITION			"DELETE FROM hostpartition WHERE partitionname = '%s' AND iduser = %d;"
#define MYSQL_QUERY_STGE_DELETE_PARTITION2			"DELETE FROM hostpartition WHERE idhostpartition = %d;"

#endif				/* __OPH_ODB_STGE_QUERY_H__ */
