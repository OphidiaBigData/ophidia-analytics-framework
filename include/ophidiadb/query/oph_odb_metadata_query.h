/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2016 CMCC Foundation

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

#define MYSQL_QUERY_META_RETRIEVE_METADATAKEY_LIST 			"SELECT idkey, label, variable, required FROM metadatakey WHERE idvocabulary=%d ORDER BY idkey ASC;"
#define MYSQL_QUERY_META_RETRIEVE_METADATAINSTANCE			"SELECT idmetadatainstance FROM metadatainstance WHERE idmetadatainstance = %d AND iddatacube = %d;"
#define MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE 	"INSERT INTO `metadatainstance` (`iddatacube`, `idkey`, `idtype`, `value`) VALUES (%d, %d, %d, '%s')"
#define MYSQL_QUERY_META_UPDATE_OPHIDIADB_MANAGE 			"INSERT INTO `manage` (`iduser`, `idmetadatainstance` ) VALUES (%d, %d)"
#define MYSQL_QUERY_META_READ_KEY_ID 						"SELECT metadatakey.idkey FROM container,vocabulary,metadatakey WHERE container.idcontainer=%d AND container.idvocabulary=vocabulary.idvocabulary AND vocabulary.idvocabulary=metadatakey.idvocabulary AND metadatakey.label='%s' AND metadatakey.variable='%s'"
#define MYSQL_QUERY_META_READ_KEY_ID2 						"SELECT metadatakey.idkey FROM container,vocabulary,metadatakey WHERE container.idcontainer=%d AND container.idvocabulary=vocabulary.idvocabulary AND vocabulary.idvocabulary=metadatakey.idvocabulary AND metadatakey.label='%s' AND metadatakey.variable IS NULL"
#define MYSQL_QUERY_META_READ_INSTANCES \
"SELECT metadatainstance.idmetadatainstance AS Id,metadatakey.variable AS Variable,metadatakey.label AS 'Key',metadatatype.name AS Type,metadatainstance.value AS Value,MAX(manage.managedate) AS Last_Modified,user.username AS User,vocabulary.name AS Vocabulary \
FROM (((( metadatainstance INNER JOIN metadatakey ON metadatainstance.idkey=metadatakey.idkey ) LEFT JOIN vocabulary ON metadatakey.idvocabulary=vocabulary.idvocabulary ) INNER JOIN metadatatype ON metadatainstance.idtype=metadatatype.idtype ) INNER JOIN manage ON metadatainstance.idmetadatainstance=manage.idmetadatainstance ) INNER JOIN user ON manage.iduser=user.iduser \
WHERE metadatainstance.iddatacube=%d AND metadatainstance.idmetadatainstance LIKE '%s' AND metadatatype.name LIKE '%s' AND CONVERT(metadatainstance.value USING latin1) LIKE '%%%s%%' %s GROUP BY manage.idmetadatainstance"
#define MYSQL_QUERY_META_UPDATE_INSTANCE 			"UPDATE metadatainstance SET value='%s' WHERE idmetadatainstance=%d AND iddatacube=%d;"
#define MYSQL_QUERY_META_DELETE_INSTANCE 			"DELETE FROM metadatainstance WHERE idmetadatainstance=%d AND iddatacube=%d;"
#define MYSQL_QUERY_META_DELETE_INSTANCES 			"DELETE FROM metadatainstance WHERE metadatainstance.idmetadatainstance IN (SELECT tmp.id FROM (SELECT metadatainstance.idmetadatainstance AS id FROM metadatainstance,metadatakey WHERE metadatainstance.iddatacube=%d AND metadatainstance.idkey=metadatakey.idkey %s) AS tmp)"
#define MYSQL_QUERY_META_RETRIEVE_VOCABULARY_ID 	"SELECT idvocabulary from `vocabulary` where name = '%s';"
#define MYSQL_QUERY_META_RETRIEVE_METADATATYPE_ID 	"SELECT idtype from `metadatatype` where name = '%s';"
#define MYSQL_QUERY_META_RETRIEVE_VOCABULARY_ID_FROM_CONTAINER 	"SELECT idvocabulary FROM container WHERE idcontainer = %d AND idvocabulary IS NOT NULL;"
#define MYSQL_QUERY_META_COPY_DATACUBE_AND_MANAGE		"LOCK TABLES metadatainstance WRITE, metadatainstance AS mi READ; INSERT INTO `metadatainstance` (`iddatacube`, `idkey`, `idtype`, `value`) SELECT %d, `idkey`, `idtype`, `value` FROM metadatainstance AS mi WHERE iddatacube = %d; UNLOCK TABLES; INSERT INTO `manage` (`iduser`, `idmetadatainstance`) SELECT %d, idmetadatainstance FROM metadatainstance WHERE iddatacube = %d;"
#define MYSQL_QUERY_META_INSERT_METADATAKEY 		"INSERT INTO `metadatakey` (`label`, `variable`) VALUES ('%s','%s');"
#define MYSQL_QUERY_META_INSERT_METADATAKEY2 		"INSERT INTO `metadatakey` (`label`) VALUES ('%s');"
#define MYSQL_QUERY_META_INSERT_METADATAKEY3 		"INSERT INTO `metadatakey` (`label`, `template`, `variable`) VALUES ('%s','%s','%s');"
#define MYSQL_QUERY_META_INSERT_METADATAKEY4 		"INSERT INTO `metadatakey` (`label`, `template`) VALUES ('%s','%s');"
#define MYSQL_QUERY_META_GET1 					"SELECT idmetadatainstance, value FROM metadatainstance INNER JOIN metadatakey ON metadatainstance.idkey = metadatakey.idkey WHERE iddatacube = %d AND template = '%s' AND variable = '%s';"
#define MYSQL_QUERY_META_GET2 					"SELECT idmetadatainstance, value FROM metadatainstance INNER JOIN metadatakey ON metadatainstance.idkey = metadatakey.idkey WHERE iddatacube = %d AND template = '%s'"
#define MYSQL_QUERY_META_TIME_DIMENSION_CHECK			"SELECT COUNT(*), idvocabulary FROM metadatainstance INNER JOIN metadatakey ON metadatainstance.idkey = metadatakey.idkey WHERE template LIKE 'time:%%' AND iddatacube = %d AND (variable IS NULL OR variable = '%s') AND required"
#define MYSQL_QUERY_META_TIME_DIMENSION_CHECK2			"SELECT COUNT(*) FROM metadatakey WHERE template LIKE 'time:%%' AND idvocabulary = %d AND required"
#define MYSQL_QUERY_META_DELETE_KEYS				"DELETE FROM metadatakey WHERE idvocabulary IS NULL AND idkey IN (SELECT idkey FROM metadatainstance WHERE iddatacube = %d AND idkey IN (SELECT idkey FROM metadatainstance GROUP BY idkey HAVING COUNT(*)=1));"
#define MYSQL_QUERY_META_CHECK_VOCABULARY		"SELECT idvocabulary FROM metadatakey INNER JOIN metadatainstance ON metadatakey.idkey = metadatainstance.idkey WHERE idvocabulary IS NOT NULL AND metadatainstance.idmetadatainstance=%d;"
#define MYSQL_QUERY_META_CHECK_VOCABULARIES		"SELECT idvocabulary FROM metadatakey INNER JOIN metadatainstance ON metadatakey.idkey = metadatainstance.idkey WHERE idvocabulary IS NOT NULL AND metadatainstance.iddatacube=%d %s;"

#endif /* __OPH_ODB_META_QUERY_H__ */
