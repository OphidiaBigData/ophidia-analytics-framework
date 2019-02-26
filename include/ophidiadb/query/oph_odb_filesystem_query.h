/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2019 CMCC Foundation

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

#ifndef __OPH_ODB_FS_QUERY_H__
#define __OPH_ODB_FS_QUERY_H__

#define MYSQL_QUERY_FS_RETRIEVE_ROOT_ID 		"SELECT idfolder FROM folder WHERE idparent IS NULL"
#define MYSQL_QUERY_FS_PATH_PARSING_ID 			"SELECT idfolder FROM folder WHERE idparent=%d AND foldername='%s'"
#define MYSQL_QUERY_FS_UNIQUENESS 			"SELECT idfolder FROM folder WHERE folder.idparent=%d AND folder.foldername='%s'"
#define MYSQL_QUERY_FS_EMPTINESS 			"SELECT idfolder FROM folder WHERE folder.idparent=%d UNION SELECT iddatacube FROM datacube WHERE datacube.idfolder=%d"

#define MYSQL_QUERY_FS_RETRIEVE_SESSION_FOLDER_ID 	"SELECT idfolder FROM session WHERE sessionid = '%s';"
#define MYSQL_QUERY_FS_RETRIEVE_CONTAINER_FOLDER_ID 	"SELECT idfolder FROM container WHERE idcontainer = %d;"
#define MYSQL_QUERY_FS_RETRIEVE_PARENT_FOLDER_ID 	"SELECT idparent FROM folder WHERE idfolder = %d;"
#define MYSQL_QUERY_FS_RETRIEVE_PARENT_FOLDER 		"SELECT idparent, foldername FROM folder WHERE idfolder = %d;"

#define MYSQL_QUERY_FS_MV 				"UPDATE container SET container.idfolder=%d, container.containername = '%s' WHERE container.idcontainer=%d;"
#define MYSQL_QUERY_FS_LIST_0 				"SELECT foldername, idfolder FROM folder WHERE folder.idparent=%d ORDER BY foldername;"
#define MYSQL_QUERY_FS_LIST_2  				"SELECT foldername AS name, 1 AS type, NULL AS idcontainer, NULL AS iddatacube, NULL AS description, COUNT(iddatacube) FROM folder LEFT OUTER JOIN datacube ON folder.idfolder = datacube.idfolder WHERE folder.idparent=%d GROUP BY folder.idfolder UNION SELECT NULL AS name, 2 AS type, datacube.idcontainer, iddatacube, datacube.description, NULL FROM folder INNER JOIN datacube ON datacube.idfolder=folder.idfolder WHERE folder.idfolder=%d ORDER BY name, iddatacube ASC;"
#define MYSQL_QUERY_FS_LIST_2_FILTER  			"SELECT foldername AS name, 1 AS type, NULL AS idcontainer, NULL AS iddatacube, NULL AS description, NULL AS level, NULL AS measure, NULL AS uri, NULL AS idinput, COUNT(iddatacube) FROM folder LEFT OUTER JOIN datacube ON folder.idfolder = datacube.idfolder WHERE folder.idparent=%d GROUP BY folder.idfolder UNION SELECT NULL AS name, 2 AS type, datacube.idcontainer, datacube.iddatacube, datacube.description, level, measure, uri, CASE WHEN task.inputnumber > 1 THEN '%s' ELSE hasinput.iddatacube END AS idinput, NULL FROM folder INNER JOIN datacube ON datacube.idfolder=folder.idfolder LEFT OUTER JOIN source ON datacube.idsource = source.idsource LEFT OUTER JOIN task ON task.idoutputcube = datacube.iddatacube LEFT OUTER JOIN hasinput ON hasinput.idtask = task.idtask WHERE folder.idfolder=%d %s ORDER BY name, iddatacube ASC;"

#define MYSQL_QUERY_FS_MKDIR				"INSERT INTO folder (idfolder,idparent,foldername) VALUES (null,%d,'%s')"
#define MYSQL_QUERY_FS_RMDIR				"DELETE FROM folder WHERE folder.idfolder=%d"
#define MYSQL_QUERY_FS_MVDIR				"UPDATE folder SET folder.idparent=%d,folder.foldername='%s' WHERE folder.idfolder=%d"

#define MYSQL_QUERY_FS_UPDATE_OPHIDIADB_CONTAINER 	"INSERT INTO `container` (`idfolder`, `idparent`, `containername`, `operator`) VALUES (%d, %d, '%s', '%s')"
#define MYSQL_QUERY_FS_UPDATE_OPHIDIADB_CONTAINER_2 	"INSERT INTO `container` (`idfolder`, `containername`, `operator`) VALUES (%d, '%s', '%s')"
#define MYSQL_QUERY_FS_UPDATE_OPHIDIADB_CONTAINER_3 	"INSERT INTO `container` (`idfolder`, `idparent`, `containername`, `operator`, `idvocabulary`) VALUES (%d, %d, '%s', '%s', %d)"
#define MYSQL_QUERY_FS_UPDATE_OPHIDIADB_CONTAINER_4 	"INSERT INTO `container` (`idfolder`, `containername`, `operator`, `idvocabulary`) VALUES (%d, '%s', '%s', %d)"
#define MYSQL_QUERY_FS_UPDATE_OPHIDIADB_CONTAINER_D 	"INSERT INTO `container` (`idfolder`, `idparent`, `containername`, `operator`, `description`) VALUES (%d, %d, '%s', '%s', '%s')"
#define MYSQL_QUERY_FS_UPDATE_OPHIDIADB_CONTAINER_D_2 	"INSERT INTO `container` (`idfolder`, `containername`, `operator`, `description`) VALUES (%d, '%s', '%s', '%s')"
#define MYSQL_QUERY_FS_UPDATE_OPHIDIADB_CONTAINER_D_3 	"INSERT INTO `container` (`idfolder`, `idparent`, `containername`, `operator`, `idvocabulary`, `description`) VALUES (%d, %d, '%s', '%s', %d, '%s')"
#define MYSQL_QUERY_FS_UPDATE_OPHIDIADB_CONTAINER_D_4 	"INSERT INTO `container` (`idfolder`, `containername`, `operator`, `idvocabulary`, `description`) VALUES (%d, '%s', '%s', %d, '%s')"

#define MYSQL_QUERY_FS_DELETE_OPHIDIADB_CONTAINER 	"DELETE FROM `container` WHERE `idcontainer` = %d"
#define MYSQL_QUERY_FS_RETRIEVE_EMPTY_CONTAINER 	"SELECT count(*) FROM container INNER JOIN datacube ON container.idcontainer = datacube.idcontainer WHERE container.idcontainer = %d"
#define MYSQL_QUERY_FS_RETRIEVE_CONTAINER_ID 		"SELECT idcontainer from `container` where containername = '%s' AND idfolder = %d;"

#define MYSQL_QUERY_FS_RETRIEVE_CONTAINER	 		"SELECT idcontainer, container.description, name FROM `container` LEFT JOIN `vocabulary` ON container.idvocabulary = vocabulary.idvocabulary WHERE containername = '%s' AND idfolder = %d;"

#define MYSQL_QUERY_FS_UPDATE_OPHIDIADB_CONTAINER_NAME	"UPDATE `container` SET containername = CONCAT(containername, CONCAT('_', idcontainer)) WHERE idcontainer = '%d'"

#endif				/* __OPH_ODB_FS_QUERY_H__ */
