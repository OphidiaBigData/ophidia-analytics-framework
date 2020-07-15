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

#ifndef __OPH_ODB_JOB_QUERY_H__
#define __OPH_ODB_JOB_QUERY_H__

#define MYSQL_QUERY_JOB_RETRIEVE_SESSION_ID				"SELECT idsession FROM session WHERE sessionid = '%s'"
#define MYSQL_QUERY_JOB_RETRIEVE_FOLDER_ID				"SELECT idfolder FROM session WHERE sessionid = '%s'"
#define MYSQL_QUERY_JOB_UPDATE_SESSION					"INSERT INTO `session` (`iduser`, `idfolder`, `sessionid`) VALUES (%d, %d, '%s')"
#define MYSQL_QUERY_UPDATE_OPHIDIADB_SESSION_FOLDER		"INSERT INTO `folder` (`idparent`, `foldername`) VALUES (1, '%s')"

#ifdef OPH_DB_SUPPORT
#define MYSQL_QUERY_JOB_UPDATE_JOB_STATUS_1				"UPDATE job SET status='%s' WHERE idjob=%d"
#define MYSQL_QUERY_JOB_UPDATE_JOB_STATUS_2				"UPDATE job SET status='%s', timestart=NOW() WHERE idjob=%d AND timestart IS NULL"
#define MYSQL_QUERY_JOB_UPDATE_JOB_STATUS_3				"UPDATE job SET status='%s', timeend=NOW() WHERE idjob=%d AND timeend IS NULL"
#define MYSQL_QUERY_JOB_UPDATE_JOB						"INSERT INTO `job` (`idsession`, `markerid`, `status`, `submissionstring`, `iduser`) VALUES (%d, '%s', '%s', '%s', '%d')"
#define MYSQL_QUERY_JOB_UPDATE_JOB2						"INSERT INTO `job` (`idsession`, `markerid`, `status`, `submissionstring`, `idparent`, `iduser`) VALUES (%d, '%s', '%s', '%s', '%s', '%d')"
#define MYSQL_QUERY_JOB_RETRIEVE_JOB_ID					"SELECT idjob FROM session INNER JOIN job ON session.idsession = job.idsession WHERE sessionid = '%s' AND markerid = %s"
#endif

#endif				/* __OPH_ODB_JOB_QUERY_H__ */
