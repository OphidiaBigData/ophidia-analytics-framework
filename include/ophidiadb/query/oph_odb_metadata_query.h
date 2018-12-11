/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2018 CMCC Foundation

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
#define MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE1 	"INSERT INTO `metadatainstance` (`iddatacube`, `idtype`, `value`, `label`, `variable`) VALUES (%d, %d, '%s', '%s', '%s');"
#define MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE2 	"INSERT INTO `metadatainstance` (`iddatacube`, `idtype`, `value`, `label`) VALUES (%d, %d, '%s', '%s');"
#define MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE3 	"INSERT INTO `metadatainstance` (`iddatacube`, `idkey`, `idtype`, `value`, `label`, `variable`) VALUES (%d, %d, %d, '%s', '%s', '%s');"
#define MYSQL_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE4 	"INSERT INTO `metadatainstance` (`iddatacube`, `idkey`, `idtype`, `value`, `label`) VALUES (%d, %d, %d, '%s', '%s');"
#define MYSQL_QUERY_META_UPDATE_OPHIDIADB_MANAGE 		"INSERT INTO `manage` (`iduser`, `idmetadatainstance` ) VALUES (%d, %d);"
#define MYSQL_QUERY_META_READ_KEY_ID 				"SELECT metadatakey.idkey FROM container,vocabulary,metadatakey WHERE container.idcontainer=%d AND container.idvocabulary=vocabulary.idvocabulary AND vocabulary.idvocabulary=metadatakey.idvocabulary AND metadatakey.label='%s' AND metadatakey.variable='%s'"
#define MYSQL_QUERY_META_READ_KEY_ID2 				"SELECT metadatakey.idkey FROM container,vocabulary,metadatakey WHERE container.idcontainer=%d AND container.idvocabulary=vocabulary.idvocabulary AND vocabulary.idvocabulary=metadatakey.idvocabulary AND metadatakey.label='%s' AND metadatakey.variable IS NULL"
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

#ifdef OPH_ODB_MNG
#define MONGODB_QUERY_META_RETRIEVE_METADATAINSTANCE			"db.metadatainstance.find({ idmetadatainstance: %d, iddatacube: %d })"
#define MONGODB_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE1 	"db.metadatainstance.insertOne({ iddatacube: %d, idtype: %d, value: \"%s\", label: \"%s\", variable: \"%s\" })"
#define MONGODB_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE2 	"db.metadatainstance.insertOne({ iddatacube: %d, idtype: %d, value: \"%s\", label: \"%s\" })"
#define MONGODB_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE3 	"db.metadatainstance.insertOne({ iddatacube: %d, idkey: %d, idtype: %d, value: \"%s\", label: \"%s\", variable: \"%s\" })"
#define MONGODB_QUERY_META_UPDATE_OPHIDIADB_METADATAINSTANCE4 	"db.metadatainstance.insertOne({ iddatacube: %d, idkey: %d, idtype: %d, value: \"%s\", label: \"%s\" })"
#define MONGODB_QUERY_META_UPDATE_OPHIDIADB_MANAGE 				"db.manage.insertOne({ iduser: %d, idmetadatainstance: %d })"
#define MONGODB_QUERY_META_READ_INSTANCES						"db.metadatainstance.find({ iddatacube: %d, idmetadatainstance: /%s/, variable: /%s/, idtype: %d, value: /%s/ }, { idmetadatainstance: 1, iddatacube: 0, idkey: 0, idtype: 1, value: 1, label: 1, variable: 1 })"
//SELECT metadatainstance.idmetadatainstance AS Id, metadatainstance.variable AS Variable, metadatainstance.label AS 'Key', metadatatype.name AS Type, metadatainstance.value AS Value, MAX(manage.managedate) AS Last_Modified, user.username AS User, vocabulary.name AS Vocabulary FROM (((( metadatainstance LEFT JOIN metadatakey ON metadatainstance.idkey=metadatakey.idkey ) LEFT JOIN vocabulary ON metadatakey.idvocabulary=vocabulary.idvocabulary ) INNER JOIN metadatatype ON metadatainstance.idtype=metadatatype.idtype ) INNER JOIN manage ON metadatainstance.idmetadatainstance=manage.idmetadatainstance ) INNER JOIN user ON manage.iduser=user.iduser WHERE metadatainstance.iddatacube=%d AND metadatainstance.idmetadatainstance LIKE '%s' AND (%s metadatainstance.variable LIKE '%s') AND metadatatype.name LIKE '%s' AND CONVERT(metadatainstance.value USING latin1) LIKE '%%%s%%' %s GROUP BY manage.idmetadatainstance"
#define MONGODB_QUERY_META_UPDATE_INSTANCE 						"db.metadatainstance.updateMany({ idmetadatainstance: %d, iddatacube: %d }, { $set: { value: \"%s\" } })"
#define MONGODB_QUERY_META_DELETE_INSTANCE 						"db.metadatainstance.deleteMany({ idmetadatainstance: %d, iddatacube: %d })"
#define MONGODB_QUERY_META_DELETE_INSTANCES 					"db.metadatainstance.deleteMany({ iddatacube: %d, variable: /%s/ }, idtype: %d, value: /%s/)"
//DELETE FROM metadatainstance WHERE iddatacube=%d AND (%s variable LIKE '%s') AND idtype IN (SELECT idtype FROM metadatatype WHERE name LIKE '%s') AND CONVERT(value USING latin1) LIKE '%%%s%%' %s;"
#define MONGODB_QUERY_META_COPY_INSTANCES						"db.metadatainstance.find({ iddatacube: %d }).forEach(function(doc) { db.metadatainstance_tmp.insert(doc); })"
#define MONGODB_QUERY_META_INSERT_INSTANCES						"db.metadatainstance_tmp.find().forEach(function(doc) { db.metadatainstance.insertOne({ iddatacube: %d, idkey: doc., idtype: doc., value: \"doc.\", label: \"doc.\", variable: \"doc.\"}); })"
// INSERT INTO `metadatainstance` (`iddatacube`, `idkey`, `idtype`, `value`, `label`, `variable`) SELECT %d, `idkey`, `idtype`, `value`, `label`, `variable` FROM metadatainstance_tmp;
#define MONGODB_QUERY_META_COPY_MANAGE							"db.metadatainstance.find({ iddatacube: %d }).forEach(function(doc) { db.manage.insertOne({ iduser: %d, idmetadatainstance: doc. }); })"
//INSERT INTO `manage` (`iduser`, `idmetadatainstance`) SELECT %d, idmetadatainstance FROM metadatainstance WHERE iddatacube = %d;"
#define MONGODB_QUERY_META_GET 									"db.metadatainstance.find({ iddatacube: %d, idkey: %d }, { idmetadatainstance: 1, iddatacube: 0, idkey: 0, idtype: 0, value: 1, label: 0, variable: 0 })"
//#define MONGODB_QUERY_META_GET1                                                               "SELECT idmetadatainstance, value FROM metadatainstance INNER JOIN metadatakey ON metadatainstance.idkey = metadatakey.idkey WHERE iddatacube = %d AND template = '%s' AND metadatakey.variable = '%s';"
//#define MONGODB_QUERY_META_GET2                                                               "SELECT idmetadatainstance, value FROM metadatainstance INNER JOIN metadatakey ON metadatainstance.idkey = metadatakey.idkey WHERE iddatacube = %d AND template = '%s'"
// TODO begin
//#define MONGODB_QUERY_META_TIME_DIMENSION_CHECK                                       "SELECT COUNT(*), idvocabulary FROM metadatainstance INNER JOIN metadatakey ON metadatainstance.idkey = metadatakey.idkey WHERE template LIKE 'time:%%' AND iddatacube = %d AND (metadatakey.variable IS NULL OR metadatakey.variable = '%s') AND required"
//#define MONGODB_QUERY_META_CHECK_VOCABULARY                                           "SELECT idvocabulary FROM metadatakey INNER JOIN metadatainstance ON metadatakey.idkey = metadatainstance.idkey WHERE idvocabulary IS NOT NULL AND metadatainstance.idmetadatainstance=%d;"
//#define MONGODB_QUERY_META_CHECK_VOCABULARIES                                 "SELECT idvocabulary FROM metadatakey INNER JOIN metadatainstance ON metadatakey.idkey = metadatainstance.idkey WHERE idvocabulary IS NOT NULL AND metadatainstance.iddatacube=%d %s;"
// TODO end
#define MONGODB_QUERY_META_RETRIEVE_KEY_OF_INSTANCE				"db.metadatainstance.find({ variable: \"%s\", iddatacube: %d }, { idmetadatainstance: 1, iddatacube: 0, idkey: 0, idtype: 0, value: 0, label: 0, variable: 0 })"
#define MONGODB_QUERY_META_UPDATE_KEY_OF_INSTANCE				"db.metadatainstance.updateOne({ idmetadatainstance: %d }, { $set: { variable: \"%s\" } })"
#endif

#endif				/* __OPH_ODB_META_QUERY_H__ */
