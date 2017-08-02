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

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

#include "taketime.h"
#include "oph_analytics_framework.h"
#include "oph_framework_paths.h"
#include "debug.h"
#include "oph_common.h"

int msglevel = LOG_WARNING_T;

int main(int argc, char *argv[])
{

	//Initialize environment
	int size, myrank, res = -1;
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

	if (!myrank) {
		fprintf(stdout, OPH_VERSION, PACKAGE_VERSION);
		fprintf(stdout, "%s", OPH_DISCLAIMER);
	}

	if (argc != 2) {
		if (!myrank)
			fprintf(stdout, "USAGE: ./oph_analytics_framework \"operator=value;param=value;...\"\n");
		res = 0;
	}

	if (!strcmp(argv[1], "-v")) {
		res = 0;
	}

	if (!strcmp(argv[1], "-x")) {
		if (!myrank)
			fprintf(stdout, "%s", OPH_WARRANTY);
		res = 0;
	}

	if (!strcmp(argv[1], "-z")) {
		if (!myrank)
			fprintf(stdout, "%s", OPH_CONDITIONS);
		res = 0;
	}

	if (res) {
		struct timeval start_time, end_time, total_time;

#ifdef OPH_PARALLEL_LOCATION
		char log_prefix[OPH_COMMON_BUFFER_LEN];
		snprintf(log_prefix, OPH_COMMON_BUFFER_LEN, OPH_FRAMEWORK_LOG_PATH_PREFIX, OPH_PARALLEL_LOCATION);
		set_log_prefix(log_prefix);
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Set logging directory to '%s'\n", log_prefix);
#endif

		if (!myrank)
			gettimeofday(&start_time, NULL);

		char task_string[OPH_COMMON_BUFFER_LEN + 1];
		strncpy(task_string, argv[1], OPH_COMMON_BUFFER_LEN);
		task_string[OPH_COMMON_BUFFER_LEN] = 0;
		if (!myrank)
			pmesg(LOG_INFO, __FILE__, __LINE__, "Task string:\n%s\n", task_string);

		if ((res = oph_af_execute_framework(task_string, size, myrank))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Framework execution failed! ERROR: %d\n", res);
		}

		MPI_Barrier(MPI_COMM_WORLD);
		if (!myrank) {
			gettimeofday(&end_time, NULL);
			timeval_subtract(&total_time, &end_time, &start_time);
			printf("Proc %d: Total execution:\t Time %d,%06d sec\n", myrank, (int) total_time.tv_sec, (int) total_time.tv_usec);
		}
	}

	MPI_Finalize();

	return 0;
}
