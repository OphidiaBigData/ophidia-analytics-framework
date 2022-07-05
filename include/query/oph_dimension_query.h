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

#ifndef __OPH_DIM_QUERY_H__
#define __OPH_DIM_QUERY_H__

#define MYSQL_DIM_CREATE_TABLE 							"CREATE TABLE %s (`iddimension` int(10) unsigned NOT NULL AUTO_INCREMENT, `dimension` longblob, PRIMARY KEY(`iddimension`))  ENGINE=MyISAM AUTO_INCREMENT=0 DEFAULT CHARSET=latin1;"
#define MYSQL_DIM_INSERT_TABLE 							"INSERT INTO %s (dimension) VALUES (?);"
#define MYSQL_DIM_INSERT_TABLE_FROM_QUERY 				"INSERT INTO %s (dimension) SELECT (%s);"
#define MYSQL_DIM_GET_DIMENSION 						"SELECT oph_compare('', '',%s, ?) FROM %s WHERE iddimension = %d;"
#define MYSQL_DIM_CHECK_DIMENSION_TABLE 				"SELECT count(*) FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s';"
#define MYSQL_DIM_SELECT_DIMENSION_ROW 					"SELECT dimension FROM %s WHERE iddimension='%d';"
#define MYSQL_DIM_RETRIEVE_COMPRESSED_DIMENSION_VALUES 	"SELECT oph_uncompress('', '',%s) FROM %s WHERE iddimension = %d;"
#define MYSQL_DIM_RETRIEVE_DIMENSION_VALUES 			"SELECT %s FROM %s WHERE iddimension = %d;"
#define MYSQL_DIM_RETRIEVE_COMPRESSED_DIMENSION_DUMP 	"SELECT oph_dump('oph_%s', '',oph_uncompress('', '',dimension))  FROM %s WHERE iddimension = %d;"
#define MYSQL_DIM_RETRIEVE_DIMENSION_DUMP 				"SELECT oph_dump('oph_%s', '', dimension)  FROM %s WHERE iddimension = %d;"
#define MYSQL_DIM_RETRIEVE_COMPRESSED_DIMENSION_LABELS 	"SELECT oph_compress('','',oph_extract('oph_%s', 'oph_%s',%s,?)) FROM %s WHERE iddimension = %d;"
#define MYSQL_DIM_RETRIEVE_DIMENSION_LABELS 			"SELECT oph_extract('oph_%s', 'oph_%s',%s,?) FROM %s WHERE iddimension = %d;"
#define MYSQL_DIM_DELETE_FRAG							"DROP TABLE IF EXISTS %s"
#define MYSQL_DIM_CREATE_DB								"CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci"
#define MYSQL_DIM_DELETE_DB								"DROP DATABASE IF EXISTS %s"
#define MYSQL_DIM_INDEX_ARRAY							"oph_get_index_array('','oph_%s',%d,%d)"
#define MYSQL_DIM_COPY_FRAG								"INSERT INTO %s (dimension) SELECT dimension FROM %s WHERE iddimension = %d;"

#endif				// __OPH_DIM_QUERY_H__
