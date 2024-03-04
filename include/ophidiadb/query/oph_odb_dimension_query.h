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

#ifndef __OPH_ODB_DIM_QUERY_H__
#define __OPH_ODB_DIM_QUERY_H__

#define MYSQL_QUERY_DIM_RETRIEVE_DIMENSIONS_FEATURES			"SELECT dimension.iddimension, dimensionname, dimensiontype, size, explicit, level, fkiddimensionindex, idhierarchy, conceptlevel, fkiddimensionlabel, unlimited FROM dimension INNER JOIN dimensioninstance ON dimension.iddimension = dimensioninstance.iddimension INNER JOIN cubehasdim ON cubehasdim.iddimensioninstance = dimensioninstance.iddimensioninstance where iddatacube = %d and size<>0 order by explicit desc, level asc;"

#define MYSQL_QUERY_DIM_RETRIEVE_FULL_DIMENSION				"SELECT hierarchy.idhierarchy, hierarchyname, filename, dimension.iddimension, idcontainer,  dimensionname, dimensiontype, dimensioninstance.iddimensioninstance,  size,  fkiddimensionindex, conceptlevel, fkiddimensionlabel, grid.idgrid, gridname, basetime, units, calendar, monthlengths, leapyear, leapmonth, unlimited FROM hierarchy INNER JOIN dimension ON hierarchy.idhierarchy = dimension.idhierarchy INNER JOIN dimensioninstance ON dimension.iddimension = dimensioninstance.iddimension LEFT OUTER JOIN grid ON grid.idgrid = dimensioninstance.idgrid where dimensioninstance.iddimensioninstance = %d;"

#define MYSQL_QUERY_DIM_RETRIEVE_CONTAINER_DIMENSIONS			"SELECT iddimension, idcontainer, dimensionname, dimensiontype, idhierarchy, basetime, units, calendar, monthlengths, leapyear, leapmonth from `dimension` where idcontainer = %d;"

#define MYSQL_QUERY_DIM_RETRIEVE_DIMENSION				"SELECT iddimension, idcontainer, dimensionname, dimensiontype, dimension.idhierarchy, basetime, units, calendar, monthlengths, leapyear, leapmonth, hierarchyname from `dimension` inner join `hierarchy` on dimension.idhierarchy = hierarchy.idhierarchy where iddimension = %d;"

#define MYSQL_QUERY_DIM_RETRIEVE_DIMENSION_FROM_INSTANCE_ID	"SELECT dimensionname FROM `dimensioninstance` INNER JOIN `dimension` ON dimensioninstance.iddimension = dimension.iddimension WHERE iddimensioninstance = %d;"

#define MYSQL_QUERY_DIM_RETRIEVE_DIMENSION_INSTANCE			"SELECT iddimensioninstance, dimensioninstance.iddimension, idgrid, size, fkiddimensionindex, conceptlevel, fkiddimensionlabel, dimensionname, filename, unlimited FROM `dimensioninstance` INNER JOIN `dimension` ON dimensioninstance.iddimension = dimension.iddimension LEFT JOIN `hierarchy` ON hierarchy.idhierarchy = dimension.idhierarchy WHERE iddimensioninstance = %d;"

#define MYSQL_QUERY_DIM_RETRIEVE_GRID_DIMENSION_INSTANCES		"SELECT dimensioninstance.iddimensioninstance, dimensioninstance.iddimension, dimensioninstance.idgrid, size, fkiddimensionindex, conceptlevel, fkiddimensionlabel from `dimensioninstance` INNER JOIN grid ON grid.idgrid = dimensioninstance.idgrid INNER JOIN dimension ON dimension.iddimension = dimensioninstance.iddimension where gridname = '%s' AND idcontainer = %d;"

#define MYSQL_QUERY_DIM_UPDATE_OPHIDIADB_DIMENSION			"INSERT IGNORE INTO `dimension` (`idcontainer`, `dimensionname`, `dimensiontype`, `idhierarchy`) VALUES (%d, '%s', '%s', %d);"

#define MYSQL_QUERY_DIM_RETRIEVE_OPHIDIADB_DIMENSION			"SELECT iddimension FROM dimension WHERE idcontainer = %d AND dimensionname = '%s';"

#define MYSQL_QUERY_DIM_UPDATE_OPHIDIADB_DIMENSION_WITH_TIME_METADATA	"INSERT IGNORE INTO `dimension` (`idcontainer`, `dimensionname`, `dimensiontype`, `idhierarchy`, `basetime`, `units`, `calendar`, `monthlengths`, `leapyear`, `leapmonth`) VALUES (%d, '%s', '%s', %d, '%s', '%s', '%s', '%s', %d, %d);"
#define MYSQL_QUERY_DIM_UPDATE_OPHIDIADB_DIMENSION_WITH_TIME_METADATA2	"UPDATE `dimension` SET idhierarchy = %d, basetime = '%s', units = '%s', calendar = '%s', leapyear = %d, leapmonth = %d WHERE iddimension = %d;"
#define MYSQL_QUERY_DIM_UPDATE_OPHIDIADB_DIMENSION_WITH_TIME_METADATA3	"UPDATE `dimensioninstance` SET conceptlevel = '%c' WHERE iddimensioninstance = %d"

#define MYSQL_QUERY_DIM_UPDATE_OPHIDIADB_DIMENSION_INST			"INSERT INTO `dimensioninstance` (`iddimension`, `size`, `fkiddimensionindex`, `idgrid`, `conceptlevel`, `unlimited`, `fkiddimensionlabel`) VALUES (%d, %d, %d, %d, '%c', '%d', %d);"

#define MYSQL_QUERY_DIM_UPDATE_OPHIDIADB_DIMENSION_INST_2		"INSERT INTO `dimensioninstance` (`iddimension`, `size`, `fkiddimensionindex`, `conceptlevel`, `unlimited`, `fkiddimensionlabel`) VALUES (%d, %d, %d, '%c', '%d', %d);"

#define MYSQL_QUERY_DIM_RETRIEVE_DIMENSION_INSTANCE_ID			"SELECT dimensioninstance.iddimensioninstance FROM (cubehasdim INNER JOIN dimensioninstance ON cubehasdim.iddimensioninstance = dimensioninstance.iddimensioninstance) INNER JOIN dimension ON dimension.iddimension = dimensioninstance.iddimension WHERE iddatacube = %d and dimensionname='%s';"
#define MYSQL_QUERY_DIM_RETRIEVE_DIMENSION_INSTANCE_ID2			"SELECT dimensioninstance.iddimensioninstance, dimension.iddimension, idhierarchy FROM (cubehasdim INNER JOIN dimensioninstance ON cubehasdim.iddimensioninstance = dimensioninstance.iddimensioninstance) INNER JOIN dimension ON dimension.iddimension = dimensioninstance.iddimension WHERE iddatacube = %d and dimensionname='%s';"

#define MYSQL_QUERY_DIM_RETRIEVE_DIMENSION_INSTANCE_DATA		"SELECT dimensioninstance.iddimensioninstance, conceptlevel, units, basetime, calendar, leapyear, leapmonth, monthlengths FROM (cubehasdim INNER JOIN dimensioninstance ON cubehasdim.iddimensioninstance = dimensioninstance.iddimensioninstance) INNER JOIN dimension ON dimension.iddimension = dimensioninstance.iddimension WHERE iddatacube = %d AND dimensionname='%s';"

#define MYSQL_QUERY_DIM_RETRIEVE_GRID_ID				"SELECT DISTINCT grid.idgrid FROM grid INNER JOIN dimensioninstance ON dimensioninstance.idgrid = grid.idgrid INNER JOIN dimension ON dimension.iddimension = dimensioninstance.iddimension WHERE gridname='%s' AND idcontainer = %d AND enabled;"
#define MYSQL_QUERY_DIM_RETRIEVE_GRID_ID2				"SELECT idgrid FROM grid WHERE gridname = '%s';"
#define MYSQL_QUERY_DIM_RETRIEVE_GRID					"SELECT gridname FROM grid WHERE idgrid = %d;"

#define MYSQL_QUERY_DIM_RETRIEVE_DIMENSIONS_FROM_GRID_IN_CONTAINER	"SELECT dimension.iddimension, dimensionname, dimensiontype, idhierarchy, iddimensioninstance, dimensioninstance.idgrid, size, fkiddimensionindex, conceptlevel, fkiddimensionlabel, basetime, units, calendar, monthlengths, leapyear, leapmonth FROM (dimensioninstance INNER JOIN grid ON grid.idgrid = dimensioninstance.idgrid) INNER JOIN dimension ON dimensioninstance.iddimension = dimension.iddimension WHERE gridname = '%s' AND idcontainer = %d;"

#define MYSQL_QUERY_DIM_RETRIEVE_HIERARCHY_FROM_DIMENSION_NAME		"SELECT hierarchy.idhierarchy, hierarchyname, filename, conceptlevel, dimensioninstance.iddimensioninstance FROM ((hierarchy INNER JOIN dimension ON hierarchy.idhierarchy = dimension.idhierarchy) INNER JOIN dimensioninstance ON dimension.iddimension = dimensioninstance.iddimension) INNER JOIN cubehasdim ON dimensioninstance.iddimensioninstance = cubehasdim.iddimensioninstance WHERE iddatacube = %d AND dimensionname ='%s';"

#define MYSQL_QUERY_DIM_RETRIEVE_GRID_LIST				"SELECT distinct gridname FROM grid INNER JOIN dimensioninstance ON grid.idgrid = dimensioninstance.idgrid INNER JOIN dimension ON dimensioninstance.iddimension = dimension.iddimension WHERE idcontainer = %d ORDER BY gridname ASC;"

#define MYSQL_QUERY_DIM_UPDATE_OPHIDIADB_GRID				"INSERT IGNORE INTO `grid` (`gridname`) VALUES ('%s')"

#define MYSQL_DELETE_OPHIDIADB_GRID					"DELETE FROM grid WHERE idgrid NOT IN (SELECT DISTINCT idgrid FROM dimensioninstance, dimension WHERE dimension.iddimension = dimensioninstance.iddimension AND NOT ISNULL(idgrid) AND idcontainer <> %d);"

#define MYSQL_QUERY_DIM_RETRIEVE_HIERARCHY				"SELECT hierarchyname, filename FROM `hierarchy` WHERE idhierarchy = %d;"

#define MYSQL_QUERY_DIM_RETRIEVE_HIERARCHY_LIST				"SELECT hierarchyname FROM hierarchy ORDER BY hierarchyname ASC;"

#define MYSQL_RETRIEVE_HIERARCHY_ID					"SELECT idhierarchy from `hierarchy` where hierarchyname = '%s';"

#define MYSQL_QUERY_DIM_ENABLE_GRID					"UPDATE grid SET enabled = 1 WHERE idgrid = %d;"

#define MYSQL_DELETE_OPHIDIADB_DIMENSION_INSTANCE	"DELETE FROM dimensioninstance WHERE iddimensioninstance = %d;"

#define MYSQL_RETRIEVE_DIMENSIONS_OF_DATACUBE		"SELECT dimensionname FROM dimension INNER JOIN dimensioninstance ON dimension.iddimension = dimensioninstance.iddimension INNER JOIN cubehasdim ON dimensioninstance.iddimensioninstance = cubehasdim.iddimensioninstance WHERE iddatacube = %d;"

#endif				/* __OPH_ODB_DIM_QUERY_H__ */
