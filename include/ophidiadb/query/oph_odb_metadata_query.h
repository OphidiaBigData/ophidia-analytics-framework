/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2021 CMCC Foundation

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

#ifndef __OPH_ODB_META_QUERY_H__
#define __OPH_ODB_META_QUERY_H__

#define MYSQL_QUERY_META_RETRIEVE_METADATAKEY_LIST 		"SELECT idkey, label, variable, required, template FROM metadatakey WHERE idvocabulary=%d ORDER BY idkey ASC;"
#define MYSQL_QUERY_META_RETRIEVE_METADATAINSTANCE		"SELECT idmetadatainstance FROM metadatainstance WHERE idmetadatainstance = %d AND iddatacube = %d;"
#define MYSQL_QUERY_META_RETRIEVE_METADATAINSTANCE_ID1	"SELECT idmetadatainstance FROM metadatainstance WHERE label LIKE '%s' AND variable LIKE '%s' AND iddatacube = %d;"
#define MYSQL_QUERY_META_RETRIEVE_METADATAINSTANCE_ID2	"SELECT idmetadatainstance FROM metadatainstance WHERE label LIKE '%s' AND variable IS NULL AND iddatacube = %d;"
#define MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE1 	"INSERT INTO `metadatainstance` (`iddatacube`, `idtype`, `value`, `label`, `variable`) VALUES (%d, %d, '%s', '%s', '%s');"
#define MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE2 	"INSERT INTO `metadatainstance` (`iddatacube`, `idtype`, `value`, `label`) VALUES (%d, %d, '%s', '%s');"
#define MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE3 	"INSERT INTO `metadatainstance` (`iddatacube`, `idkey`, `idtype`, `value`, `label`, `variable`) VALUES (%d, %d, %d, '%s', '%s', '%s');"
#define MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE4 	"INSERT INTO `metadatainstance` (`iddatacube`, `idkey`, `idtype`, `value`, `label`) VALUES (%d, %d, %d, '%s', '%s');"
#define MYSQL_QUERY_META_UPDATE_OPHIDIADB_MANAGE 		"INSERT INTO `manage` (`iduser`, `idmetadatainstance` ) VALUES (%d, %d);"
#define MYSQL_QUERY_META_READ_KEY_ID 				"SELECT metadatakey.idkey FROM container,vocabulary,metadatakey WHERE container.idcontainer=%d AND container.idvocabulary=vocabulary.idvocabulary AND vocabulary.idvocabulary=metadatakey.idvocabulary AND metadatakey.label='%s' AND metadatakey.variable='%s'"
#define MYSQL_QUERY_META_READ_KEY_ID2 				"SELECT metadatakey.idkey FROM container,vocabulary,metadatakey WHERE container.idcontainer=%d AND container.idvocabulary=vocabulary.idvocabulary AND vocabulary.idvocabulary=metadatakey.idvocabulary AND metadatakey.label='%s' AND metadatakey.variable IS NULL"
#define MYSQL_QUERY_META_READ_INSTANCE				"SELECT metadatainstance.idmetadatainstance AS Id, metadatainstance.variable AS Variable, metadatainstance.label AS 'Key', metadatatype.name AS Type, metadatainstance.value AS Value FROM metadatainstance INNER JOIN metadatatype ON metadatainstance.idtype = metadatatype.idtype WHERE metadatainstance.idmetadatainstance = %d;"
#define MYSQL_QUERY_META_READ_INSTANCES				"SELECT metadatainstance.idmetadatainstance AS Id, metadatainstance.variable AS Variable, metadatainstance.label AS 'Key', metadatatype.name AS Type, metadatainstance.value AS Value, MAX(manage.managedate) AS Last_Modified, user.username AS User, vocabulary.name AS Vocabulary FROM (((( metadatainstance LEFT JOIN metadatakey ON metadatainstance.idkey=metadatakey.idkey ) LEFT JOIN vocabulary ON metadatakey.idvocabulary=vocabulary.idvocabulary ) INNER JOIN metadatatype ON metadatainstance.idtype=metadatatype.idtype ) INNER JOIN manage ON metadatainstance.idmetadatainstance=manage.idmetadatainstance ) INNER JOIN user ON manage.iduser=user.iduser WHERE metadatainstance.iddatacube=%d AND metadatainstance.idmetadatainstance LIKE '%s' AND (%s metadatainstance.variable LIKE '%s') AND metadatatype.name LIKE '%s' AND CONVERT(metadatainstance.value USING latin1) LIKE '%%%s%%' %s GROUP BY manage.idmetadatainstance"
#define MYSQL_QUERY_META_UPDATE_INSTANCE 			"UPDATE metadatainstance SET value='%s' WHERE idmetadatainstance=%d AND iddatacube=%d;"
#define MYSQL_QUERY_META_DELETE_INSTANCE 			"DELETE FROM metadatainstance WHERE idmetadatainstance=%d AND iddatacube=%d;"
#define MYSQL_QUERY_META_DELETE_INSTANCES 			"DELETE FROM metadatainstance WHERE iddatacube=%d AND (%s variable LIKE '%s') AND idtype IN (SELECT idtype FROM metadatatype WHERE name LIKE '%s') AND CONVERT(value USING latin1) LIKE '%%%s%%' %s;"
#define MYSQL_QUERY_META_RETRIEVE_VOCABULARY_ID 		"SELECT idvocabulary from `vocabulary` where name = '%s';"
#define MYSQL_QUERY_META_RETRIEVE_METADATATYPE_ID 		"SELECT idtype from `metadatatype` where name = '%s';"
#define MYSQL_QUERY_META_RETRIEVE_VOCABULARY_ID_FROM_CONTAINER 	"SELECT idvocabulary FROM container WHERE idcontainer = %d AND idvocabulary IS NOT NULL;"
#define MYSQL_QUERY_META_COPY_INSTANCES				"CREATE TEMPORARY TABLE IF NOT EXISTS metadatainstance_tmp AS (SELECT `idkey`, `idtype`, `value`, `label`, `variable` FROM metadatainstance WHERE iddatacube = %d);"
#define MYSQL_QUERY_META_INSERT_INSTANCES			"INSERT INTO `metadatainstance` (`iddatacube`, `idkey`, `idtype`, `value`, `label`, `variable`) SELECT %d, `idkey`, `idtype`, `value`, `label`, `variable` FROM metadatainstance_tmp;"
#define MYSQL_QUERY_META_COPY_MANAGE				"INSERT INTO `manage` (`iduser`, `idmetadatainstance`) SELECT %d, idmetadatainstance FROM metadatainstance WHERE iddatacube = %d;"
#define MYSQL_QUERY_META_GET1 					"SELECT idmetadatainstance, value FROM metadatainstance INNER JOIN metadatakey ON metadatainstance.idkey = metadatakey.idkey WHERE iddatacube = %d AND template = '%s' AND metadatakey.variable = '%s';"
#define MYSQL_QUERY_META_GET2 					"SELECT idmetadatainstance, value FROM metadatainstance INNER JOIN metadatakey ON metadatainstance.idkey = metadatakey.idkey WHERE iddatacube = %d AND template = '%s'"
#define MYSQL_QUERY_META_TIME_DIMENSION_CHECK			"SELECT COUNT(*), idvocabulary FROM metadatainstance INNER JOIN metadatakey ON metadatainstance.idkey = metadatakey.idkey WHERE template LIKE 'time:%%' AND iddatacube = %d AND (metadatakey.variable IS NULL OR metadatakey.variable = '%s') AND required"
#define MYSQL_QUERY_META_TIME_DIMENSION_CHECK2			"SELECT COUNT(*) FROM metadatakey WHERE template LIKE 'time:%%' AND idvocabulary = %d AND required"
#define MYSQL_QUERY_META_CHECK_VOCABULARY			"SELECT idvocabulary FROM metadatakey INNER JOIN metadatainstance ON metadatakey.idkey = metadatainstance.idkey WHERE idvocabulary IS NOT NULL AND metadatainstance.idmetadatainstance=%d;"
#define MYSQL_QUERY_META_CHECK_VOCABULARIES			"SELECT idvocabulary FROM metadatakey INNER JOIN metadatainstance ON metadatakey.idkey = metadatainstance.idkey WHERE idvocabulary IS NOT NULL AND metadatainstance.iddatacube=%d %s;"
#define MYSQL_QUERY_META_RETRIEVE_KEY_OF_INSTANCE		"SELECT idmetadatainstance FROM metadatainstance WHERE iddatacube = %d AND variable = '%s';"
#define MYSQL_QUERY_META_UPDATE_KEY_OF_INSTANCE			"UPDATE metadatainstance SET variable = '%s' WHERE idmetadatainstance = %d;"
#define MYSQL_QUERY_META_RETRIEVE_ID_BY_NAME			"SELECT idmetadatainstance FROM metadatainstance WHERE label = '%s' AND variable = '%s' AND iddatacube = %d;"

#endif				/* __OPH_ODB_META_QUERY_H__ */
