/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2022 CMCC Foundation

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

#ifndef __OPH_ODB_CUBE_QUERY_H__
#define __OPH_ODB_CUBE_QUERY_H__

#define MYSQL_QUERY_CUBE_RETRIEVE_CUBE				"SELECT iddatacube, idcontainer, hostxdatacube, fragmentxdb, tuplexfragment, measure, measuretype, fragrelativeindexset, compress, level, idsource from `datacube` where iddatacube = %d"

#define MYSQL_QUERY_CUBE_RETRIEVE_CUBE_ADDITIONAL_INFO		"SELECT creationdate, description from `datacube` where iddatacube = %d"

#define MYSQL_QUERY_CUBE_RETRIEVE_CUBEHASDIM			"SELECT idcubehasdim, cubehasdim.iddimensioninstance, iddatacube, explicit, level, size FROM cubehasdim INNER JOIN dimensioninstance ON cubehasdim.iddimensioninstance = dimensioninstance.iddimensioninstance WHERE iddatacube = %d ORDER by explicit DESC, level ASC;"

#define MYSQL_QUERY_CUBE_RETRIEVE_PART				"SELECT iddbinstance FROM `partitioned` WHERE iddatacube = %d"

#define MYSQL_QUERY_CUBE_RETRIEVE_PART2				"SELECT partitioned.iddbinstance FROM `partitioned` INNER JOIN dbinstance ON partitioned.iddbinstance = dbinstance.iddbinstance INNER JOIN dbmsinstance ON dbinstance.iddbmsinstance = dbmsinstance.iddbmsinstance INNER JOIN host ON dbmsinstance.idhost = host.idhost WHERE iddatacube = %d ORDER BY host.idhost, dbmsinstance.iddbmsinstance, dbinstance.iddbinstance"

#define MYSQL_QUERY_CUBE_RETRIEVE_DATACUBE_ELEMENTS		"SELECT celements from `datacube` where iddatacube = %d"

#define MYSQL_QUERY_CUBE_UPDATE_DATACUBE_ELEMENTS		"UPDATE datacube SET celements=%lld WHERE iddatacube=%d;"

#define MYSQL_QUERY_CUBE_RETRIEVE_DATACUBE_SIZE			"SELECT csize from `datacube` where iddatacube = %d"

#define MYSQL_QUERY_CUBE_UPDATE_DATACUBE_SIZE			"UPDATE datacube SET csize=%lld WHERE iddatacube=%d;"

#define MYSQL_QUERY_CUBE_RETRIEVE_DATACUBE_DBMS_NUMBER		"SELECT count(distinct iddbmsinstance) FROM partitioned INNER JOIN dbinstance ON dbinstance.iddbinstance = partitioned.iddbinstance WHERE iddatacube = %d;"

#define MYSQL_QUERY_CUBE_RETRIEVE_DATACUBE_PARENTS		"SELECT datacube.idcontainer, operation, datacube.iddatacube, uri FROM datacube INNER JOIN hasinput ON hasinput.iddatacube = datacube.iddatacube INNER JOIN task ON task.idtask = hasinput.idtask LEFT JOIN source ON source.idsource = datacube.idsource WHERE task.idoutputcube = %d ORDER BY  datacube.iddatacube ASC;"

#define MYSQL_QUERY_CUBE_RETRIEVE_DATACUBE_CHILDREN		"SELECT datacube.idcontainer, operation, datacube.iddatacube, uri FROM datacube INNER JOIN task ON task.idoutputcube = datacube.iddatacube  INNER JOIN hasinput ON task.idtask = hasinput.idtask LEFT JOIN source ON source.idsource = datacube.idsource WHERE hasinput.iddatacube = %d ORDER BY  datacube.iddatacube ASC;"

#define MYSQL_QUERY_CUBE_RETRIEVE_TASK_LIST			"SELECT operation, i.idcontainer, i.iddatacube, o.idcontainer, o.iddatacube, query FROM task INNER JOIN hasinput ON task.idtask = hasinput.idtask  INNER JOIN datacube AS i ON i.iddatacube = hasinput.iddatacube INNER JOIN datacube AS o ON o.iddatacube = task.idoutputcube WHERE i.idcontainer IN (SELECT idcontainer FROM container WHERE idfolder=%d) %s  ORDER BY task.idtask ASC;"
#define MYSQL_QUERY_CUBE_RETRIEVE_TASK_LIST_WC			"SELECT operation, i.idcontainer, i.iddatacube, o.idcontainer, o.iddatacube, query FROM task INNER JOIN hasinput ON task.idtask = hasinput.idtask  INNER JOIN datacube AS i ON i.iddatacube = hasinput.iddatacube INNER JOIN datacube AS o ON o.iddatacube = task.idoutputcube WHERE i.idcontainer IN (SELECT idcontainer FROM container WHERE idfolder=%d AND containername = '%s') %s  ORDER BY task.idtask ASC;"

#define MYSQL_QUERY_CUBE_FIND_USER_SUBFOLDERS			"SELECT @pv:=idfolder AS id FROM folder JOIN (select @pv:=%d)tmp WHERE idparent=@pv UNION SELECT %d AS id;"

#define MYSQL_QUERY_CUBE_FIND_DATACUBE_CONTAINER_0		"SELECT datacube.idcontainer, datacube.iddatacube, containername FROM container LEFT JOIN datacube ON container.idcontainer = datacube.idcontainer WHERE idfolder IN (%s) %s;"

#define MYSQL_QUERY_CUBE_FIND_DATACUBE_CONTAINER_1		"SELECT containername FROM container WHERE containername LIKE '%%%s%%' AND idfolder IN (%s);"

#define MYSQL_QUERY_CUBE_FIND_DATACUBE_CONTAINER_2		"SELECT datacube.idcontainer, iddatacube FROM container LEFT JOIN datacube ON container.idcontainer = datacube.idcontainer  WHERE iddatacube = %d AND idfolder IN (%s);"

#define MYSQL_QUERY_CUBE_FIND_DATACUBE_CONTAINER_2_1		"SELECT datacube.idcontainer, iddatacube FROM container LEFT JOIN datacube ON container.idcontainer = datacube.idcontainer  WHERE idfolder IN (%s);"

#define MYSQL_QUERY_CUBE_CHECK_DATACUBE_STORAGE_STATUS		"SELECT count(*) FROM partitioned INNER JOIN dbinstance ON dbinstance.iddbinstance=partitioned.iddbinstance INNER JOIN dbmsinstance ON dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance INNER JOIN host ON dbmsinstance.idhost=host.idhost  WHERE iddatacube = %d AND host.status = 'down';"

#define MYSQL_QUERY_CUBE_CHECK_DATACUBE_STORAGE_STATUS_2	"SELECT count(*) FROM fragment INNER JOIN dbinstance ON dbinstance.iddbinstance=fragment.iddbinstance INNER JOIN dbmsinstance ON dbinstance.iddbmsinstance=dbmsinstance.iddbmsinstance INNER JOIN host ON dbmsinstance.idhost=host.idhost  WHERE idfragment = %d AND host.status = 'down';"

#define MYSQL_QUERY_CUBE_RETRIEVE_SOURCE_ID			"SELECT idsource FROM `source` WHERE uri = '%s';"

#define MYSQL_QUERY_CUBE_RETRIEVE_SOURCE			"SELECT uri FROM `source` WHERE idsource = %d;"

#define MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_SOURCE		"INSERT INTO `source` (`uri`) VALUES ('%s')"

#define MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_CUBE			"INSERT INTO `datacube` (`idcontainer`, `hostxdatacube`, `fragmentxdb`, `tuplexfragment`, `measure`, `measuretype`, `fragrelativeindexset`, `compress`, `level`, `idsource` ) VALUES (%d, %d, %d, %d, '%s', '%s', '%s', %d, %d, %d );"
#define MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_CUBE_1		"INSERT INTO `datacube` (`idcontainer`, `hostxdatacube`, `fragmentxdb`, `tuplexfragment`, `measure`, `measuretype`, `fragrelativeindexset`, `compress`, `level` ) VALUES (%d, %d, %d, %d, '%s', '%s', '%s', %d, %d );"
#define MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_CUBE_2		"INSERT INTO `datacube` (`idcontainer`, `hostxdatacube`, `fragmentxdb`, `tuplexfragment`, `measure`, `measuretype`, `fragrelativeindexset`, `compress`, `level`, `idsource`, `description` ) VALUES (%d, %d, %d, %d, '%s', '%s', '%s', %d, %d, %d, '%s' );"
#define MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_CUBE_3		"INSERT INTO `datacube` (`idcontainer`, `hostxdatacube`, `fragmentxdb`, `tuplexfragment`, `measure`, `measuretype`, `fragrelativeindexset`, `compress`, `level`, `description` ) VALUES (%d, %d, %d, %d, '%s', '%s', '%s', %d, %d, '%s' );"

#define MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_TASK_1		"INSERT INTO `task` (`idoutputcube`, `operation`, `inputnumber`) VALUES (%d, '%s', %d)"

#define MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_TASK_2		"INSERT INTO `task` (`idoutputcube`, `operation`, `query`, `inputnumber`) VALUES (%d, '%s', '%s', %d)"

#define MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_TASK_1_job		"INSERT INTO `task` (`idoutputcube`, `operation`, `idjob`, `inputnumber`) VALUES (%d, '%s', %d, %d)"

#define MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_TASK_2_job		"INSERT INTO `task` (`idoutputcube`, `operation`, `query`, `idjob`, `inputnumber`) VALUES (%d, '%s', '%s', %d, %d)"

#define MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_HASINPUT		"INSERT INTO `hasinput` (`iddatacube`, `idtask`) VALUES (%d, %d)"

#define MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_PART			"INSERT INTO `partitioned` (`iddbinstance`, `iddatacube`) VALUES (%d, %d)"

#define MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_CUBEHASDIM		"INSERT INTO `cubehasdim` (`iddimensioninstance`, `iddatacube`, `explicit`, `level`) VALUES (%d, %d, %d, %d)"
#define MYSQL_QUERY_CUBE_UPDATE_LEVEL_OF_CUBEHASDIM			"UPDATE cubehasdim SET level = %d WHERE idcubehasdim = %d"

#define MYSQL_QUERY_CUBE_DELETE_OPHIDIADB_CUBE			"DELETE FROM `datacube` where `iddatacube` = %d"

#define MYSQL_QUERY_CUBE_RETRIEVE_CUBE_ID			"SELECT iddatacube from `datacube` INNER JOIN container on datacube.idcontainer = container.idcontainer where containername = '%s' AND datacubename = '%s'"

//level is chosen just to retrieve a field
#define MYSQL_QUERY_CUBE_CHECK_CUBE_PID				"SELECT level from `datacube` where idcontainer = %d AND iddatacube = %d;"

#define MYSQL_QUERY_CUBE_RETRIEVE_PARTITIONED_DATACUBE		"SELECT iddatacube from `partitioned` where iddbinstance = %d"

// query moved from OPH_EXPORT_NCL
#define MYSQL_QUERY_CUBE_RETRIEVE_CUBE_MEASURE			"SELECT measure FROM `datacube` WHERE iddatacube = %d"

#define MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_TUPLEXFRAGMENT	"UPDATE datacube SET tuplexfragment = %d WHERE iddatacube = %d"

#define MYSQL_QUERY_ORDER_CUBE_BY_SOURCE				"SELECT idcontainer, iddatacube FROM datacube INNER JOIN source ON datacube.idsource = source.idsource WHERE level = 0 AND iddatacube IN (%s) ORDER BY uri ASC;"

#define MYSQL_QUERY_CUBE_UPDATE_OPHIDIADB_CUBE_MS		"UPDATE datacube SET idmissingvalue = %d WHERE iddatacube = %d;"
#define MYSQL_QUERY_CUBE_RETRIEVE_OPHIDIADB_CUBE_MS		"SELECT idmissingvalue, measure FROM `datacube` WHERE iddatacube = %d;"

#endif				/* __OPH_ODB_CUBE_QUERY_H__ */
