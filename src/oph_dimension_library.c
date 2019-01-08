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

#define _GNU_SOURCE

#include "oph_dimension_library.h"

/* Standard C99 headers */
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>
#include <zlib.h>
#include <math.h>

#include "oph_framework_paths.h"
#include "oph-lib-binary-io.h"

#include "query/oph_datacube_query.h"

#include <ctype.h>
#include <mysql.h>
#include "debug.h"

extern int msglevel;

char oph_dim_typeof(char *dimension_type)
{
	if (!strcasecmp(dimension_type, OPH_DIM_BYTE_TYPE))
		return OPH_DIM_BYTE_FLAG;
	else if (!strcasecmp(dimension_type, OPH_DIM_SHORT_TYPE))
		return OPH_DIM_SHORT_FLAG;
	else if (!strcasecmp(dimension_type, OPH_DIM_INT_TYPE))
		return OPH_DIM_INT_FLAG;
	else if (!strcasecmp(dimension_type, OPH_DIM_LONG_TYPE))
		return OPH_DIM_LONG_FLAG;
	else if (!strcasecmp(dimension_type, OPH_DIM_FLOAT_TYPE))
		return OPH_DIM_FLOAT_FLAG;
	else if (!strcasecmp(dimension_type, OPH_DIM_DOUBLE_TYPE))
		return OPH_DIM_DOUBLE_FLAG;
	return 0;
}

size_t oph_dim_sizeof(char type_flag)
{
	switch (type_flag) {
		case OPH_DIM_BYTE_FLAG:
			return sizeof(char);
		case OPH_DIM_SHORT_FLAG:
			return sizeof(short);
		case OPH_DIM_INT_FLAG:
			return sizeof(int);
		case OPH_DIM_LONG_FLAG:
			return sizeof(long long);
		case OPH_DIM_FLOAT_FLAG:
			return sizeof(float);
		case OPH_DIM_DOUBLE_FLAG:
			return sizeof(double);
	}
	return 0;
}

int oph_dim_check_data_type(char *input_type, int *size)
{
	if (!input_type || !size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	if (!strncmp(input_type, OPH_DIM_DOUBLE_TYPE, sizeof(OPH_DIM_DOUBLE_TYPE))) {
		*size = sizeof(double);
		return OPH_DIM_SUCCESS;
	}
	if (!strncmp(input_type, OPH_DIM_FLOAT_TYPE, sizeof(OPH_DIM_FLOAT_TYPE))) {
		*size = sizeof(float);
		return OPH_DIM_SUCCESS;
	}
	if (!strncmp(input_type, OPH_DIM_BYTE_TYPE, sizeof(OPH_DIM_BYTE_TYPE))) {
		*size = sizeof(char);
		return OPH_DIM_SUCCESS;
	}
	if (!strncmp(input_type, OPH_DIM_SHORT_TYPE, sizeof(OPH_DIM_SHORT_TYPE))) {
		*size = sizeof(short);
		return OPH_DIM_SUCCESS;
	}
	if (!strncmp(input_type, OPH_DIM_INT_TYPE, sizeof(OPH_DIM_INT_TYPE))) {
		*size = sizeof(int);
		return OPH_DIM_SUCCESS;
	}
	if (!strncmp(input_type, OPH_DIM_LONG_TYPE, sizeof(OPH_DIM_LONG_TYPE))) {
		*size = sizeof(long long);
		return OPH_DIM_SUCCESS;
	}

	pmesg(LOG_ERROR, __FILE__, __LINE__, "Data type not recognized\n");
	return OPH_DIM_DATA_ERROR;
}

int oph_dim_get_double_value_of(char *dim_row, unsigned int kk, const char *dimension_type, double *value)
{
	if (!dim_row || !dimension_type || !value) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	if (!strncmp(dimension_type, OPH_DIM_DOUBLE_TYPE, sizeof(OPH_DIM_DOUBLE_TYPE)))
		*value = ((double *) dim_row)[kk];
	else if (!strncmp(dimension_type, OPH_DIM_FLOAT_TYPE, sizeof(OPH_DIM_FLOAT_TYPE)))
		*value = (double) ((float *) dim_row)[kk];
	else if (!strncmp(dimension_type, OPH_DIM_LONG_TYPE, sizeof(OPH_DIM_LONG_TYPE)))
		*value = (double) ((long long *) dim_row)[kk];
	else if (!strncmp(dimension_type, OPH_DIM_INT_TYPE, sizeof(OPH_DIM_INT_TYPE)))
		*value = (double) ((int *) dim_row)[kk];
	else if (!strncmp(dimension_type, OPH_DIM_SHORT_TYPE, sizeof(OPH_DIM_SHORT_TYPE)))
		*value = (double) ((short *) dim_row)[kk];
	else if (!strncmp(dimension_type, OPH_DIM_BYTE_TYPE, sizeof(OPH_DIM_BYTE_TYPE)))
		*value = (double) ((char *) dim_row)[kk];
	else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Data type not recognized\n");
		return OPH_DIM_DATA_ERROR;
	}

	return OPH_DIM_SUCCESS;
}

int oph_dim_set_double_value_of(char *dim_row, unsigned int kk, const char *dimension_type, double value)
{
	if (!dim_row || !dimension_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	if (!strncmp(dimension_type, OPH_DIM_DOUBLE_TYPE, sizeof(OPH_DIM_DOUBLE_TYPE)))
		((double *) dim_row)[kk] = value;
	else if (!strncmp(dimension_type, OPH_DIM_FLOAT_TYPE, sizeof(OPH_DIM_FLOAT_TYPE)))
		((float *) dim_row)[kk] = (float) value;
	else if (!strncmp(dimension_type, OPH_DIM_LONG_TYPE, sizeof(OPH_DIM_LONG_TYPE)))
		((long long *) dim_row)[kk] = (long long) value;
	else if (!strncmp(dimension_type, OPH_DIM_INT_TYPE, sizeof(OPH_DIM_INT_TYPE)))
		((int *) dim_row)[kk] = (int) value;
	else if (!strncmp(dimension_type, OPH_DIM_SHORT_TYPE, sizeof(OPH_DIM_SHORT_TYPE)))
		((short *) dim_row)[kk] = (short) value;
	else if (!strncmp(dimension_type, OPH_DIM_BYTE_TYPE, sizeof(OPH_DIM_BYTE_TYPE)))
		((char *) dim_row)[kk] = (char) value;
	else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Data type not recognized\n");
		return OPH_DIM_DATA_ERROR;
	}

	return OPH_DIM_SUCCESS;
}

int oph_date_to_day(int y, int m, int d, long long *g, oph_odb_dimension * dim)
{
	if (!g || !dim)
		return OPH_DIM_NULL_PARAM;
	if (!dim->calendar || !strlen(dim->calendar))
		return OPH_DIM_TIME_PARSING_ERROR;

	if (!strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_GREGORIAN, OPH_ODB_DIM_TIME_SIZE) || !strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_STANDARD, OPH_ODB_DIM_TIME_SIZE)
	    || !strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_365_DAY, OPH_ODB_DIM_TIME_SIZE) || !strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_365DAY, OPH_ODB_DIM_TIME_SIZE)) {
		int offset = 0;
		if ((y > 1582) || ((y == 1582) && ((m > 10) || ((m == 10) && (d >= 15)))))
			offset = 1;
		else if ((y < 1582) || ((y == 1582) && ((m < 10) || ((m == 10) && (d <= 4)))))
			offset = -1;
		else
			return OPH_DIM_DATA_ERROR;
		m = (m + 9) % 12;
		y = y - m / 10;
		*g = 365L * y + y / 4 + (offset > 0 ? -y / 100 + y / 400 : 0) + (m * 306L + 5) / 10 + (d + offset - 2);
	} else if (!strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_PGREGORIAN, OPH_ODB_DIM_TIME_SIZE) || !strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_PGREGORIAN2, OPH_ODB_DIM_TIME_SIZE)) {
		m = (m + 9) % 12;
		y = y - m / 10;
		*g = 365L * y + y / 4 - y / 100 + y / 400 + (m * 306L + 5) / 10 + (d - 1);
	} else if (!strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_JULIAN, OPH_ODB_DIM_TIME_SIZE)) {
		m = (m + 9) % 12;
		y = y - m / 10;
		*g = 365L * y + y / 4 + (m * 306L + 5) / 10 + (d - 1);
	} else if (!strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_360_DAY, OPH_ODB_DIM_TIME_SIZE) || !strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_360DAY, OPH_ODB_DIM_TIME_SIZE)) {
		*g = 360L * y + 30L * (m - 1) + (d - 1);
	} else if (!strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_NO_LEAP, OPH_ODB_DIM_TIME_SIZE) || !strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_NOLEAP, OPH_ODB_DIM_TIME_SIZE)) {
		m = (m + 9) % 12;
		y = y - m / 10;
		*g = 365L * y + (m * 306L + 5) / 10 + (d - 1);
	} else if (!strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_ALL_LEAP, OPH_ODB_DIM_TIME_SIZE) || !strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_ALLLEAP, OPH_ODB_DIM_TIME_SIZE)) {
		m = (m + 9) % 12;
		y = y - m / 10;
		*g = 366L * y + (m * 306L + 5) / 10 + (d - 1);
	} else if (!strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_USER_DEFINED, OPH_ODB_DIM_TIME_SIZE)) {
		int i;
		long long days = 0, pdays = 0;
		for (i = 0; i < OPH_ODB_DIM_MONTH_NUMBER; ++i) {
			days += dim->month_lengths[i];
			if (i >= dim->leap_month)
				pdays += dim->month_lengths[i];
		}
		m = (m + (OPH_ODB_DIM_MONTH_NUMBER - dim->leap_month - 1)) % OPH_ODB_DIM_MONTH_NUMBER;
		y = y - m / 10;
		*g = days * y + (y - dim->leap_year) / 4 + (m * pdays + 5) / 10 + (d - 1);
	} else
		return OPH_DIM_DATA_ERROR;

	return OPH_DIM_SUCCESS;
}

int oph_day_to_date(long long g, int *yy, int *mm, int *dd, int *wd, int *yd, oph_odb_dimension * dim)
{
	if (!yy || !mm || !dd || !dim)
		return OPH_DIM_NULL_PARAM;
	if (!dim->calendar || !strlen(dim->calendar))
		return OPH_DIM_TIME_PARSING_ERROR;

	long long y, mi, ddd;
	if (!strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_GREGORIAN, OPH_ODB_DIM_TIME_SIZE) || !strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_STANDARD, OPH_ODB_DIM_TIME_SIZE)
	    || !strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_365_DAY, OPH_ODB_DIM_TIME_SIZE) || !strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_365DAY, OPH_ODB_DIM_TIME_SIZE)) {
		int offset = g >= 578041 ? 0 : 2;
		g += offset;
		y = !offset ? (10000 * g + 14780) / 3652425 : 100 * g / 36525;
		ddd = g - (365 * y + y / 4 + (!offset ? -y / 100 + y / 400 : 0));
		if (ddd < 0) {
			y--;
			ddd = g - (365 * y + y / 4 + (!offset ? -y / 100 + y / 400 : 0));
		}
		mi = (100 * ddd + 52) / 3060;
		*mm = (mi + 2) % 12 + 1;
		*yy = y + (mi + 2) / 12;
		*dd = ddd - (mi * 306 + 5) / 10 + 1;
	} else if (!strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_PGREGORIAN, OPH_ODB_DIM_TIME_SIZE) || !strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_PGREGORIAN2, OPH_ODB_DIM_TIME_SIZE)) {
		y = (10000 * g + 14780) / 3652425;
		ddd = g - (365 * y + y / 4 - y / 100 + y / 400);
		if (ddd < 0) {
			y--;
			ddd = g - (365 * y + y / 4 - y / 100 + y / 400);
		}
		mi = (100 * ddd + 52) / 3060;
		*mm = (mi + 2) % 12 + 1;
		*yy = y + (mi + 2) / 12;
		*dd = ddd - (mi * 306 + 5) / 10 + 1;
	} else if (!strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_JULIAN, OPH_ODB_DIM_TIME_SIZE)) {
		y = 100 * g / 36525;
		ddd = g - (365 * y + y / 4);
		if (ddd < 0) {
			y--;
			ddd = g - (365 * y + y / 4);
		}
		mi = (100 * ddd + 52) / 3060;
		*mm = (mi + 2) % 12 + 1;
		*yy = y + (mi + 2) / 12;
		*dd = ddd - (mi * 306 + 5) / 10 + 1;
	} else if (!strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_360_DAY, OPH_ODB_DIM_TIME_SIZE) || !strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_360DAY, OPH_ODB_DIM_TIME_SIZE)) {
		*dd = g % OPH_ODB_DIM_DAY_NUMBER + 1;
		g /= OPH_ODB_DIM_DAY_NUMBER;
		*mm = g % OPH_ODB_DIM_MONTH_NUMBER + 1;
		g /= OPH_ODB_DIM_MONTH_NUMBER;
		*yy = g;
	} else if (!strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_NO_LEAP, OPH_ODB_DIM_TIME_SIZE) || !strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_NOLEAP, OPH_ODB_DIM_TIME_SIZE)) {
		y = g / 365;
		ddd = g - 365 * y;
		if (ddd < 0) {
			y--;
			ddd = g - 365 * y;
		}
		mi = (100 * ddd + 52) / 3060;
		*mm = (mi + 2) % 12 + 1;
		*yy = y + (mi + 2) / 12;
		*dd = ddd - (mi * 306 + 5) / 10 + 1;
	} else if (!strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_ALL_LEAP, OPH_ODB_DIM_TIME_SIZE) || !strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_ALLLEAP, OPH_ODB_DIM_TIME_SIZE)) {
		y = g / 366;
		ddd = g - 366 * y;
		if (ddd < 0) {
			y--;
			ddd = g - 366 * y;
		}
		mi = (100 * ddd + 52) / 3060;
		*mm = (mi + 2) % 12 + 1;
		*yy = y + (mi + 2) / 12;
		*dd = ddd - (mi * 306 + 5) / 10 + 1;
	} else if (!strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_USER_DEFINED, OPH_ODB_DIM_TIME_SIZE)) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "This option is not well supported\n");
		int i;
		long long days = 0, pdays = 0;
		for (i = 0; i < OPH_ODB_DIM_MONTH_NUMBER; ++i) {
			days += dim->month_lengths[i];
			if (i >= dim->leap_month)
				pdays += dim->month_lengths[i];
		}
		y = g / days;
		ddd = g - (days * y + (y - dim->leap_year) / 4);
		if (ddd < 0) {
			y--;
			ddd = g - (days * y + (y - dim->leap_year) / 4);
		}
		mi = (100 * ddd + 52) / (10 * days);
		*mm = (mi + dim->leap_month) % OPH_ODB_DIM_MONTH_NUMBER + 1;
		*yy = y + (mi + dim->leap_month) / OPH_ODB_DIM_MONTH_NUMBER;
		*dd = ddd - (mi * days + 5) / 10 + 1;
	} else
		return OPH_DIM_DATA_ERROR;

	if (wd || yd) {
		time_t rawtime;
		time(&rawtime);
		struct tm timeinfo;
		if (!localtime_r(&rawtime, &timeinfo))
			return OPH_DIM_SYSTEM_ERROR;
		timeinfo.tm_year = *yy - 1900;
		timeinfo.tm_mon = *mm - 1;
		timeinfo.tm_mday = *dd;
		mktime(&timeinfo);
		if (wd)
			*wd = (timeinfo.tm_wday ? timeinfo.tm_wday : OPH_ODB_DIM_WEEK_NUMBER) - 1;	// Sunday will be marked with the last index
		if (yd)
			*yd = timeinfo.tm_yday;
	}

	return OPH_DIM_SUCCESS;
}

int oph_dim_get_time_value_of(char *dim_row, unsigned int kk, oph_odb_dimension * dim, struct tm *tm_base, long long *base_time)
{
	if (!dim_row || !dim || !tm_base) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}
	if (!dim->calendar || !strlen(dim->calendar))
		return OPH_DIM_TIME_PARSING_ERROR;

	memset(tm_base, 0, sizeof(struct tm));
	long long _base_time, *base_time_ = base_time ? base_time : &_base_time;
	*base_time_ = 0;

	double _value;
	// Read the offset
	if (oph_dim_get_double_value_of(dim_row, kk, dim->dimension_type, &_value))
		return OPH_DIM_DATA_ERROR;

	// Convert to "seconds"
	switch (dim->units[0]) {
		case 'd':
			_value *= 4.0;
		case '6':
			_value *= 2.0;
		case '3':
			_value *= 3.0;
		case 'h':
			_value *= 60.0;
		case 'm':
			_value *= 60.0;
		case 's':
			break;
		default:
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unrecognized or unsupported units\n");
	}

	// Add the base
	if (dim->base_time && strlen(dim->base_time)) {
		strptime(dim->base_time, "%Y-%m-%d %H:%M:%S", tm_base);
		tm_base->tm_year += 1900;
		tm_base->tm_mon++;
		if (oph_date_to_day(tm_base->tm_year, tm_base->tm_mon, tm_base->tm_mday, base_time_, dim)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unrecognized calendar type '%s'\n", dim->calendar);
			return OPH_DIM_DATA_ERROR;
		}
		*base_time_ = tm_base->tm_sec + OPH_ODB_DIM_SECOND_NUMBER * (tm_base->tm_min + OPH_ODB_DIM_MINUTE_NUMBER * (tm_base->tm_hour + OPH_ODB_DIM_HOUR_NUMBER * (*base_time_)));
	}
	// Convert to "date"
	memset(tm_base, 0, sizeof(struct tm));
	long long value = (long long) _value + (*base_time_);
	tm_base->tm_sec = value % OPH_ODB_DIM_SECOND_NUMBER;
	value /= OPH_ODB_DIM_SECOND_NUMBER;	// minutes
	tm_base->tm_min = value % OPH_ODB_DIM_MINUTE_NUMBER;
	value /= OPH_ODB_DIM_MINUTE_NUMBER;	// hours
	tm_base->tm_hour = value % OPH_ODB_DIM_HOUR_NUMBER;
	value /= OPH_ODB_DIM_HOUR_NUMBER;	// days
	if (oph_day_to_date(value, &tm_base->tm_year, &tm_base->tm_mon, &tm_base->tm_mday, &tm_base->tm_wday, &tm_base->tm_yday, dim)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unrecognized calendar type '%s'\n", dim->calendar);
		return OPH_DIM_DATA_ERROR;
	}

	return OPH_DIM_SUCCESS;
}

int oph_dim_get_time_string_of(char *dim_row, unsigned int kk, oph_odb_dimension * dim, char *output_string)
{
	if (!dim_row || !dim || !output_string) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}
	if (!dim->calendar || !strlen(dim->calendar))
		return OPH_DIM_TIME_PARSING_ERROR;

	struct tm tm_base;
	memset(&tm_base, 0, sizeof(struct tm));

	if (oph_dim_get_time_value_of(dim_row, kk, dim, &tm_base, NULL))
		return OPH_DIM_DATA_ERROR;

	snprintf(output_string, MYSQL_BUFLEN, "%04d-%02d-%02d %02d:%02d:%02d", tm_base.tm_year, tm_base.tm_mon, tm_base.tm_mday, tm_base.tm_hour, tm_base.tm_min, tm_base.tm_sec);

	return OPH_DIM_SUCCESS;
}

int oph_set_centroid(char *dim_row, unsigned int kk, oph_odb_dimension * dim, struct tm *tm_centroid, long long base_time)
{
	if (!dim_row || !dim || !tm_centroid) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}
	if (!dim->calendar || !strlen(dim->calendar))
		return OPH_DIM_TIME_PARSING_ERROR;

	long long value;
	if (oph_date_to_day(tm_centroid->tm_year, tm_centroid->tm_mon, tm_centroid->tm_mday, &value, dim)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unrecognized calendar type '%s'\n", dim->calendar);
		return OPH_DIM_DATA_ERROR;
	}
	value = tm_centroid->tm_sec + OPH_ODB_DIM_SECOND_NUMBER * (tm_centroid->tm_min + OPH_ODB_DIM_MINUTE_NUMBER * (tm_centroid->tm_hour + OPH_ODB_DIM_HOUR_NUMBER * value));

	// Drop the base time
	value -= base_time;

	// Convert to units
	double _value = (double) value;
	switch (dim->units[0]) {
		case 'd':
			_value /= 4.0;
		case '6':
			_value /= 2.0;
		case '3':
			_value /= 3.0;
		case 'h':
			_value /= 60.0;
		case 'm':
			_value /= 60.0;
		case 's':
			break;
		default:
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unrecognized or unsupported units\n");
	}

	if (oph_dim_set_double_value_of(dim_row, kk, dim->dimension_type, _value))
		return OPH_DIM_DATA_ERROR;

	return OPH_DIM_SUCCESS;
}

int oph_dim_get_month_size_of(struct tm *tm_time, oph_odb_dimension * dim)
{
	if (!tm_time || !dim) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return 0;
	}
	if (!dim->calendar || !strlen(dim->calendar))
		return 0;

	if (strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_USER_DEFINED, OPH_ODB_DIM_TIME_SIZE)) {
		switch (tm_time->tm_mon) {
			case 1:
			case 3:
			case 5:
			case 7:
			case 8:
			case 10:
			case 12:
				return 31;
			case 4:
			case 6:
			case 9:
			case 11:
				return 30;
			case 2:
				{
					int leap = 0, y = tm_time->tm_year;
					if (!strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_GREGORIAN, OPH_ODB_DIM_TIME_SIZE)
					    || !strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_STANDARD, OPH_ODB_DIM_TIME_SIZE)
					    || !strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_PGREGORIAN, OPH_ODB_DIM_TIME_SIZE)
					    || !strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_PGREGORIAN2, OPH_ODB_DIM_TIME_SIZE))
						leap = !(y % 4) && (y % 100) && !(y % 400);
					else if (!strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_JULIAN, OPH_ODB_DIM_TIME_SIZE))
						leap = !(y % 4);
					else if (!strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_ALL_LEAP, OPH_ODB_DIM_TIME_SIZE)
						 || !strncmp(dim->calendar, OPH_DIM_TIME_CALENDAR_ALLLEAP, OPH_ODB_DIM_TIME_SIZE))
						leap = 1;
					return leap ? 29 : 28;
				}
			default:
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong month %d\n", tm_time->tm_mon);
				return 0;
		}

	} else {

		pmesg(LOG_WARNING, __FILE__, __LINE__, "This option is not well supported\n");
		if ((tm_time->tm_mon > 0) && (tm_time->tm_mon <= OPH_ODB_DIM_MONTH_NUMBER)) {
			if ((tm_time->tm_mon == dim->leap_month) && (!(tm_time->tm_year - dim->leap_year) % 4))
				return dim->month_lengths[tm_time->tm_mon] + 1;
			else
				return dim->month_lengths[tm_time->tm_mon];
		}
	}

	pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong month %d\n", tm_time->tm_mon);
	return 0;
}

int oph_dim_is_in_time_group_of(char *dim_row, unsigned int kk, oph_odb_dimension * dim, char concept_level_out, struct tm *tm_prev, int midnight, int centroid, int *res)
{
	if (!dim_row || !dim || !tm_prev || !res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}
	if (!dim->calendar || !strlen(dim->calendar))
		return OPH_DIM_TIME_PARSING_ERROR;
	*res = 0;

	struct tm tm_base;
	long long base_time;
	if (oph_dim_get_time_value_of(dim_row, kk, dim, &tm_base, &base_time))
		return OPH_DIM_DATA_ERROR;
	int msize, prev_week, base_week;

	// Check for group
	int first_element = tm_prev->tm_year < 0;
	struct tm tm_centroid;
	memcpy(&tm_centroid, &tm_base, sizeof(struct tm));
	switch (concept_level_out) {
		case 's':
			if (first_element || (tm_prev->tm_sec != tm_base.tm_sec))
				break;
		case 'm':
			if (centroid) {
				tm_centroid.tm_sec = 30;
				if (oph_set_centroid(dim_row, kk, dim, &tm_centroid, base_time)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in setting the centroid\n");
					return OPH_DIM_DATA_ERROR;
				}
				centroid = 0;
			}
			if (first_element || (midnight && (tm_prev->tm_min == tm_base.tm_min) && !tm_prev->tm_sec)
			    || ((tm_prev->tm_min != tm_base.tm_min) && (!midnight || ((tm_prev->tm_min + 1) % 60 != tm_base.tm_min) || tm_base.tm_sec)))
				break;
		case 'h':
			if (centroid) {
				tm_centroid.tm_min = 30;
				tm_centroid.tm_sec = 0;
				if (oph_set_centroid(dim_row, kk, dim, &tm_centroid, base_time)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in setting the centroid\n");
					return OPH_DIM_DATA_ERROR;
				}
				centroid = 0;
			}
			if (first_element || (midnight && (tm_prev->tm_hour == tm_base.tm_hour) && !tm_prev->tm_sec && !tm_prev->tm_min)
			    || ((tm_prev->tm_hour != tm_base.tm_hour) && (!midnight || ((tm_prev->tm_hour + 1) % 24 != tm_base.tm_hour) || tm_base.tm_sec || tm_base.tm_min)))
				break;
		case '3':
			if (centroid) {
				tm_centroid.tm_hour = 1 + 3 * (tm_centroid.tm_hour / 3);
				tm_centroid.tm_min = 30;
				tm_centroid.tm_sec = 0;
				if (oph_set_centroid(dim_row, kk, dim, &tm_centroid, base_time)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in setting the centroid\n");
					return OPH_DIM_DATA_ERROR;
				}
				centroid = 0;
			}
			if (first_element || (midnight && (tm_prev->tm_hour / 3 == tm_base.tm_hour / 3) && !tm_prev->tm_sec && !tm_prev->tm_min)
			    || ((tm_prev->tm_hour / 3 != tm_base.tm_hour / 3) && (!midnight || ((tm_prev->tm_hour / 3 + 1) % 8 != tm_base.tm_hour / 3) || tm_base.tm_sec || tm_base.tm_min)))
				break;
		case '6':
			if (centroid) {
				tm_centroid.tm_hour = 3 + 6 * (tm_centroid.tm_hour / 6);
				tm_centroid.tm_min = 0;
				tm_centroid.tm_sec = 0;
				if (oph_set_centroid(dim_row, kk, dim, &tm_centroid, base_time)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in setting the centroid\n");
					return OPH_DIM_DATA_ERROR;
				}
				centroid = 0;
			}
			if (first_element || (midnight && (tm_prev->tm_hour / 6 == tm_base.tm_hour / 6) && !tm_prev->tm_sec && !tm_prev->tm_min)
			    || ((tm_prev->tm_hour / 6 != tm_base.tm_hour / 6) && (!midnight || ((tm_prev->tm_hour / 6 + 1) % 4 != tm_base.tm_hour / 6) || tm_base.tm_sec || tm_base.tm_min)))
				break;
		case 'd':
			if (centroid) {
				tm_centroid.tm_hour = 12;
				tm_centroid.tm_min = 0;
				tm_centroid.tm_sec = 0;
				if (oph_set_centroid(dim_row, kk, dim, &tm_centroid, base_time)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in setting the centroid\n");
					return OPH_DIM_DATA_ERROR;
				}
				centroid = 0;
			}
			if (first_element)
				break;
			msize = oph_dim_get_month_size_of(tm_prev, dim);
			if (!msize) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in setting month size\n");
				return OPH_DIM_DATA_ERROR;
			}
			if ((midnight && (tm_prev->tm_mday == tm_base.tm_mday) && !tm_prev->tm_sec && !tm_prev->tm_min && !tm_prev->tm_hour)
			    || ((tm_prev->tm_mday != tm_base.tm_mday) && (!midnight || ((tm_prev->tm_mday + 1) % msize != tm_base.tm_mday) || tm_base.tm_sec || tm_base.tm_min || tm_base.tm_hour)))
				break;
		case 'w':
			if (centroid) {
				tm_centroid.tm_mday += 3 - tm_centroid.tm_wday;
				tm_centroid.tm_wday = 3;
				tm_centroid.tm_hour = 12;
				tm_centroid.tm_min = 0;
				tm_centroid.tm_sec = 0;
				if (oph_set_centroid(dim_row, kk, dim, &tm_centroid, base_time)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in setting the centroid\n");
					return OPH_DIM_DATA_ERROR;
				}
				centroid = 0;
			}
			if (first_element)
				break;
			prev_week = (tm_prev->tm_yday + (tm_prev->tm_wday + OPH_ODB_DIM_WEEK_NUMBER - tm_prev->tm_yday % OPH_ODB_DIM_WEEK_NUMBER) % OPH_ODB_DIM_WEEK_NUMBER) / OPH_ODB_DIM_WEEK_NUMBER;
			base_week = (tm_base.tm_yday + (tm_base.tm_wday + OPH_ODB_DIM_WEEK_NUMBER - tm_base.tm_yday % OPH_ODB_DIM_WEEK_NUMBER) % OPH_ODB_DIM_WEEK_NUMBER) / OPH_ODB_DIM_WEEK_NUMBER;
			if ((midnight && (prev_week == base_week) && !tm_prev->tm_sec && !tm_prev->tm_min && !tm_prev->tm_hour && !tm_prev->tm_wday)
			    || ((prev_week != base_week) && (!midnight || ((prev_week + 1) % 53 != base_week) || tm_base.tm_sec || tm_base.tm_min || tm_base.tm_hour || tm_base.tm_wday)))
				break;
		case 'M':
			if (concept_level_out != 'w') {
				if (centroid) {
					tm_centroid.tm_mday = 15;
					tm_centroid.tm_hour = 0;
					tm_centroid.tm_min = 0;
					tm_centroid.tm_sec = 0;
					if (oph_set_centroid(dim_row, kk, dim, &tm_centroid, base_time)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in setting the centroid\n");
						return OPH_DIM_DATA_ERROR;
					}
					centroid = 0;
				}
				if (first_element || (midnight && (tm_prev->tm_mon == tm_base.tm_mon) && !tm_prev->tm_sec && !tm_prev->tm_min && !tm_prev->tm_hour && (tm_prev->tm_mday == 1))
				    || ((tm_prev->tm_mon != tm_base.tm_mon)
					&& (!midnight || ((tm_prev->tm_mon + 1) % 12 != tm_base.tm_mon) || tm_base.tm_sec || tm_base.tm_min || tm_base.tm_hour || (tm_base.tm_mday != 1))))
					break;
			}
		case 'q':
			if (concept_level_out != 'w') {
				if (centroid) {
					tm_centroid.tm_mon = 1 + 3 * (tm_centroid.tm_mon / 3);
					tm_centroid.tm_mday = 15;
					tm_centroid.tm_hour = 0;
					tm_centroid.tm_min = 0;
					tm_centroid.tm_sec = 0;
					if (oph_set_centroid(dim_row, kk, dim, &tm_centroid, base_time)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in setting the centroid\n");
						return OPH_DIM_DATA_ERROR;
					}
					centroid = 0;
				}
				if (first_element || (midnight && (tm_prev->tm_mon / 3 == tm_base.tm_mon / 3) && !tm_prev->tm_sec && !tm_prev->tm_min && !tm_prev->tm_hour && (tm_prev->tm_mday == 1))
				    || ((tm_prev->tm_mon / 3 != tm_base.tm_mon / 3)
					&& (!midnight || ((tm_prev->tm_mon / 3 + 1) % 4 != tm_base.tm_mon / 3) || tm_base.tm_sec || tm_base.tm_min || tm_base.tm_hour || (tm_base.tm_mday != 1))))
					break;
			}
		case 'y':
			if (centroid) {
				tm_centroid.tm_mon = 6;
				tm_centroid.tm_mday = 1;
				tm_centroid.tm_hour = 0;
				tm_centroid.tm_min = 0;
				tm_centroid.tm_sec = 0;
				if (oph_set_centroid(dim_row, kk, dim, &tm_centroid, base_time)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in setting the centroid\n");
					return OPH_DIM_DATA_ERROR;
				}
				centroid = 0;
			}
			if (first_element
			    || (midnight && (tm_prev->tm_year == tm_base.tm_year) && !tm_prev->tm_sec && !tm_prev->tm_min && !tm_prev->tm_hour && (tm_prev->tm_mday == 1) && (tm_prev->tm_mon == 1))
			    || ((tm_prev->tm_year != tm_base.tm_year)
				&& (!midnight || (tm_prev->tm_year + 1 != tm_base.tm_year) || tm_base.tm_sec || tm_base.tm_min || tm_base.tm_hour || (tm_base.tm_mday != 1) || (tm_base.tm_mon != 1))))
				break;
			*res = 1;
			break;
		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unrecognized frequency\n");
			return OPH_DIM_DATA_ERROR;
	}

	// Save data for next element
	memcpy(tm_prev, &tm_base, sizeof(struct tm));

	return OPH_DIM_SUCCESS;
}

int oph_dim_update_value(char *dim_row, const char *dimension_type, unsigned int first, unsigned int last)
{
	if (!dim_row || !dimension_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}
	if (first == last)
		return OPH_DIM_SUCCESS;

	double first_value, last_value;
	if (oph_dim_get_double_value_of(dim_row, first, dimension_type, &first_value))
		return OPH_DIM_DATA_ERROR;
	if (oph_dim_get_double_value_of(dim_row, last, dimension_type, &last_value))
		return OPH_DIM_DATA_ERROR;
	first_value = (first_value + last_value) / 2.0;
	if (oph_dim_set_double_value_of(dim_row, first, dimension_type, first_value))
		return OPH_DIM_DATA_ERROR;

	return OPH_DIM_SUCCESS;
}

int _oph_dim_get_base_time(oph_odb_dimension * dim, long long *base_time)
{
	if (!dim || !base_time) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}
	*base_time = 0;

	struct tm tm_value;
	memset(&tm_value, 0, sizeof(struct tm));
	if (dim->base_time && strlen(dim->base_time)) {
		strptime(dim->base_time, "%Y-%m-%d %H:%M:%S", &tm_value);
		tm_value.tm_year += 1900;
		tm_value.tm_mon++;
		if (oph_date_to_day(tm_value.tm_year, tm_value.tm_mon, tm_value.tm_mday, base_time, dim)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unrecognized calendar type '%s'\n", dim->calendar);
			return OPH_DIM_DATA_ERROR;
		}
		*base_time = tm_value.tm_sec + OPH_ODB_DIM_SECOND_NUMBER * (tm_value.tm_min + OPH_ODB_DIM_MINUTE_NUMBER * (tm_value.tm_hour + OPH_ODB_DIM_HOUR_NUMBER * (*base_time)));
	}

	return OPH_DIM_SUCCESS;
}

int _oph_dim_get_scaling_factor(oph_odb_dimension * dim, double *scaling_factor)
{
	if (!dim || !scaling_factor) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}
	*scaling_factor = 1.0;

	switch (dim->units[0]) {
		case 'd':
			*scaling_factor *= 4.0;
		case '6':
			*scaling_factor *= 2.0;
		case '3':
			*scaling_factor *= 3.0;
		case 'h':
			*scaling_factor *= 60.0;
		case 'm':
			*scaling_factor *= 60.0;
		case 's':
			break;
		default:
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unrecognized or unsupported units in '%s'\n", dim->units);
	}

	return OPH_DIM_SUCCESS;
}

int oph_dim_parse_season_subset(const char *subset_string, oph_odb_dimension * dim, char *output_string, char *data, unsigned long long data_size)
{
	if (!subset_string || !dim || !output_string || !data || !data_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}
	if (!dim->calendar || !strlen(dim->calendar))
		return OPH_DIM_TIME_PARSING_ERROR;
	*output_string = 0;

	char *pch = NULL, *save_pointer = NULL, temp[MYSQL_BUFLEN];
	snprintf(temp, MYSQL_BUFLEN, "%s", subset_string);

	long long base_time = 0, value_time;
	double min, max, scaling_factor;
	if (oph_dim_get_double_value_of(data, 0, dim->dimension_type, &min))
		return OPH_DIM_DATA_ERROR;
	if (oph_dim_get_double_value_of(data, data_size - 1, dim->dimension_type, &max))
		return OPH_DIM_DATA_ERROR;
	if (_oph_dim_get_base_time(dim, &base_time))
		return OPH_DIM_DATA_ERROR;
	if (_oph_dim_get_scaling_factor(dim, &scaling_factor))
		return OPH_DIM_DATA_ERROR;

	// Get bounds as years
	int min_year, max_year;
	struct tm tm_base;
	memset(&tm_base, 0, sizeof(struct tm));
	value_time = (long long) (min * scaling_factor) + base_time;
	value_time /= OPH_ODB_DIM_SECOND_NUMBER;	// minutes
	value_time /= OPH_ODB_DIM_MINUTE_NUMBER;	// hours
	value_time /= OPH_ODB_DIM_HOUR_NUMBER;	// days
	if (oph_day_to_date(value_time, &tm_base.tm_year, &tm_base.tm_mon, &tm_base.tm_mday, &tm_base.tm_wday, &tm_base.tm_yday, dim)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unrecognized calendar type '%s'\n", dim->calendar);
		return OPH_DIM_DATA_ERROR;
	}
	min_year = tm_base.tm_year - 1900;
	memset(&tm_base, 0, sizeof(struct tm));
	value_time = (long long) (max * scaling_factor) + base_time;
	value_time /= OPH_ODB_DIM_SECOND_NUMBER;	// minutes
	value_time /= OPH_ODB_DIM_MINUTE_NUMBER;	// hours
	value_time /= OPH_ODB_DIM_HOUR_NUMBER;	// days
	if (oph_day_to_date(value_time, &tm_base.tm_year, &tm_base.tm_mon, &tm_base.tm_mday, &tm_base.tm_wday, &tm_base.tm_yday, dim)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unrecognized calendar type '%s'\n", dim->calendar);
		return OPH_DIM_DATA_ERROR;
	}
	max_year = tm_base.tm_year - 1900;

	int i, n = 0, nn;
	unsigned char type, first = 0;
	char season[5], temp2[MYSQL_BUFLEN], temp3[MYSQL_BUFLEN];
	for (i = 0; i < 5; ++i)
		season[i] = 0;
	while ((pch = strtok_r(pch ? NULL : temp, OPH_DIM_SUBSET_SEPARATOR1, &save_pointer))) {
		if (!strcmp(pch, OPH_DIM_TIME_WINTER))
			type = 1;
		else if (!strcmp(pch, OPH_DIM_TIME_SPRING))
			type = 2;
		else if (!strcmp(pch, OPH_DIM_TIME_SUMMER))
			type = 3;
		else if (!strcmp(pch, OPH_DIM_TIME_AUTUMN))
			type = 4;
		else {
			type = 0;
			n += snprintf(output_string + n, MYSQL_BUFLEN - n, "%s%s", first ? OPH_DIM_SUBSET_SEPARATOR1 : "", pch);
		}
		if (type && !season[type]) {
			season[type] = 1;	// Avoid to repeat the same season more times
			*temp2 = nn = 0;
			type *= 3;
			for (i = min_year; i <= max_year; ++i) {
				memset(&tm_base, 0, sizeof(struct tm));
				tm_base.tm_mon = type - 4;
				tm_base.tm_year = i;
				if (tm_base.tm_mon < 0) {
					tm_base.tm_mon = 11;
					tm_base.tm_year--;
				}
				strftime(temp3, MYSQL_BUFLEN, "%Y-%m", &tm_base);
				nn += snprintf(temp2 + nn, MYSQL_BUFLEN - nn, "%s%s%c", i > min_year ? OPH_DIM_SUBSET_SEPARATOR1 : "", temp3, OPH_DIM_SUBSET_SEPARATOR[1]);
				memset(&tm_base, 0, sizeof(struct tm));
				tm_base.tm_mon = type - 1;
				tm_base.tm_year = i;
				strftime(temp3, MYSQL_BUFLEN, "%Y-%m", &tm_base);
				nn += snprintf(temp2 + nn, MYSQL_BUFLEN - nn, "%s", temp3);
			}
			n += snprintf(output_string + n, MYSQL_BUFLEN - n, "%s%s", first ? OPH_DIM_SUBSET_SEPARATOR1 : "", temp2);
		}
		first = 1;
	}

	return OPH_DIM_SUCCESS;
}

int oph_dim_parse_time_subset(const char *subset_string, oph_odb_dimension * dim, char *output_string)
{
	if (!subset_string || !dim || !output_string) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}
	if (!dim->calendar || !strlen(dim->calendar))
		return OPH_DIM_TIME_PARSING_ERROR;
	*output_string = 0;

	int n, nn, nnn;
	char *pch = NULL, *save_pointer = NULL, temp[MYSQL_BUFLEN];
	snprintf(temp, MYSQL_BUFLEN, "%s", subset_string);

	nnn = strlen(temp);
	char separator[1 + nnn];
	for (n = nn = 0; n < nnn; ++n) {
		if (temp[n] == OPH_DIM_SUBSET_SEPARATOR[0])
			separator[nn++] = OPH_DIM_SUBSET_SEPARATOR[0];
		else if (temp[n] == OPH_DIM_SUBSET_SEPARATOR[1]) {
			if (nn && (separator[nn - 1] == OPH_DIM_SUBSET_SEPARATOR2)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Interval bounds are not correctly set in '%s'.", temp);
				return OPH_DIM_DATA_ERROR;
			}
			separator[nn++] = OPH_DIM_SUBSET_SEPARATOR2;
		}
	}
	separator[nn] = 0;
	n = nn = 0;

	struct tm tm_value;
	long long base_time = 0, value_time;
	double scaling_factor, _value;
	if (_oph_dim_get_base_time(dim, &base_time))
		return OPH_DIM_DATA_ERROR;
	if (_oph_dim_get_scaling_factor(dim, &scaling_factor))
		return OPH_DIM_DATA_ERROR;

	while ((pch = strtok_r(pch ? NULL : temp, OPH_DIM_SUBSET_SEPARATOR, &save_pointer))) {
		value_time = 0;
		memset(&tm_value, 0, sizeof(struct tm));

		tm_value.tm_year = -1;
		strptime(pch, "%Y-%m-%d %H:%M:%S", &tm_value);
		if (tm_value.tm_year < 0)
			return OPH_DIM_TIME_PARSING_ERROR;

		tm_value.tm_year += 1900;
		tm_value.tm_mon++;
		if (!n && !tm_value.tm_mday)
			tm_value.tm_mday++;
		if (oph_date_to_day(tm_value.tm_year, tm_value.tm_mon, tm_value.tm_mday, &value_time, dim)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unrecognized calendar type '%s'\n", dim->calendar);
			return OPH_DIM_DATA_ERROR;
		}
		value_time = tm_value.tm_sec + OPH_ODB_DIM_SECOND_NUMBER * (tm_value.tm_min + OPH_ODB_DIM_MINUTE_NUMBER * (tm_value.tm_hour + OPH_ODB_DIM_HOUR_NUMBER * value_time)) - base_time;

		_value = ((double) value_time) / scaling_factor;

		if (!strncmp(dim->dimension_type, OPH_DIM_DOUBLE_TYPE, sizeof(OPH_DIM_DOUBLE_TYPE)))
			n += snprintf(output_string + n, MYSQL_BUFLEN - n, "%f%c", _value, separator[nn++]);
		else if (!strncmp(dim->dimension_type, OPH_DIM_FLOAT_TYPE, sizeof(OPH_DIM_FLOAT_TYPE)))
			n += snprintf(output_string + n, MYSQL_BUFLEN - n, "%f%c", (float) _value, separator[nn++]);
		else if (!strncmp(dim->dimension_type, OPH_DIM_LONG_TYPE, sizeof(OPH_DIM_LONG_TYPE)))
			n += snprintf(output_string + n, MYSQL_BUFLEN - n, "%lld%c", (long long) _value, separator[nn++]);
		else if (!strncmp(dim->dimension_type, OPH_DIM_INT_TYPE, sizeof(OPH_DIM_INT_TYPE)))
			n += snprintf(output_string + n, MYSQL_BUFLEN - n, "%d%c", (int) _value, separator[nn++]);
		else if (!strncmp(dim->dimension_type, OPH_DIM_SHORT_TYPE, sizeof(OPH_DIM_SHORT_TYPE)))
			n += snprintf(output_string + n, MYSQL_BUFLEN - n, "%d%c", (short) _value, separator[nn++]);
		else if (!strncmp(dim->dimension_type, OPH_DIM_BYTE_TYPE, sizeof(OPH_DIM_BYTE_TYPE)))
			n += snprintf(output_string + n, MYSQL_BUFLEN - n, "%d%c", (char) _value, separator[nn++]);
		else {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Data type not recognized\n");
			return OPH_DIM_DATA_ERROR;
		}
	}

	return OPH_DIM_SUCCESS;
}

int oph_dim_load_dim_dbinstance(oph_odb_db_instance * db)
{
	if (!db) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	db->dbms_instance = (oph_odb_dbms_instance *) malloc(sizeof(oph_odb_dbms_instance));
	db->id_db = 0;
	db->id_dbms = 0;
	db->dbms_instance->conn = NULL;

	char config[OPH_DIM_PATH_LEN];
	snprintf(config, sizeof(config), OPH_FRAMEWORK_DIMENSION_CONF_FILE_PATH, OPH_ANALYTICS_LOCATION);
	FILE *file = fopen(config, "r");
	if (file == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Configuration file not found\n");
		return OPH_DIM_DATA_ERROR;
	}

	unsigned int i;
	char *argument = NULL;
	char *argument_value = NULL;
	int argument_length = 0;
	char *result = NULL;
	char line[OPH_DIM_BUFFER_LEN] = { '\0' };
	while (!feof(file)) {
		result = fgets(line, OPH_DIM_BUFFER_LEN, file);
		if (!result) {
			if (ferror(file)) {
				fclose(file);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read file line from %s\n", OPH_DIM_BUFFER_LEN);
				return OPH_DIM_DATA_ERROR;
			} else {
				break;
			}
		}

		/* Remove trailing newline */
		if (line[strlen(line) - 1] == '\n')
			line[strlen(line) - 1] = '\0';

		/* Skip comment lines */
		if (line[0] == '#') {
			pmesg(LOG_DEBUG, __FILE__, __LINE__, "Read comment line: %s\n", line);
			continue;
		}

		/* Check if line contains only spaces */
		for (i = 0; (i < strlen(line)) && (i < OPH_DIM_BUFFER_LEN); i++) {
			if (!isspace((unsigned char) line[i]))
				break;
		}
		if (i == strlen(line) || i == OPH_DIM_BUFFER_LEN) {
			pmesg(LOG_DEBUG, __FILE__, __LINE__, "Read empty or blank line\n");
			continue;
		}

		/* Split argument and value on '=' character */
		for (i = 0; (i < strlen(line)) && (i < OPH_DIM_BUFFER_LEN); i++) {
			if (line[i] == '=')
				break;
		}
		if ((i == strlen(line)) || (i == OPH_DIM_BUFFER_LEN)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Read invalid line: %s\n", line);
			continue;
		}

		argument_length = strlen(line) - i - 1;

		argument = (char *) strndup(line, sizeof(char) * i);
		if (!argument) {
			fclose(file);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to alloc memory for configuration argument\n");
			return OPH_DIM_DATA_ERROR;
		}

		argument_value = (char *) strndup(line + i + 1, sizeof(char) * argument_length);
		if (!argument_value) {
			fclose(file);
			free(argument);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to alloc memory for configuration argument\n");
			return OPH_DIM_DATA_ERROR;
		}

		if (!strncasecmp(argument, OPH_CONF_DIMDB_NAME, strlen(OPH_CONF_DIMDB_NAME))) {
			snprintf(db->db_name, OPH_ODB_STGE_DB_NAME_SIZE, "%s", argument_value);
		} else if (!strncasecmp(argument, OPH_CONF_DIMDB_HOST, strlen(OPH_CONF_DIMDB_HOST))) {
			snprintf(db->dbms_instance->hostname, OPH_ODB_STGE_HOST_NAME_SIZE, "%s", argument_value);
		} else if (!strncasecmp(argument, OPH_CONF_DIMDB_PORT, strlen(OPH_CONF_DIMDB_PORT))) {
			db->dbms_instance->port = (int) strtol(argument_value, NULL, 10);
		} else if (!strncasecmp(argument, OPH_CONF_DIMDB_LOGIN, strlen(OPH_CONF_DIMDB_LOGIN))) {
			snprintf(db->dbms_instance->login, OPH_ODB_STGE_LOGIN_SIZE, "%s", argument_value);
		} else if (!strncasecmp(argument, OPH_CONF_DIMDB_PWD, strlen(OPH_CONF_DIMDB_PWD))) {
			snprintf(db->dbms_instance->pwd, OPH_ODB_STGE_PWD_SIZE, "%s", argument_value);
		}

		free(argument_value);
		free(argument);
	}

	fclose(file);

	return OPH_DIM_SUCCESS;
}

int oph_dim_create_db(oph_odb_db_instance * db)
{
	if (!db) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	if (oph_dim_check_connection_to_db(db->dbms_instance, NULL, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	char db_creation_query[MYSQL_BUFLEN];
	int n = snprintf(db_creation_query, MYSQL_BUFLEN, MYSQL_DIM_CREATE_DB, db->db_name);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_query(db->dbms_instance->conn, db_creation_query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query execution error: %s\n", mysql_error(db->dbms_instance->conn));
		return OPH_DIM_MYSQL_ERROR;
	}
	return OPH_DIM_SUCCESS;
}

int oph_dim_delete_db(oph_odb_db_instance * db)
{
	if (!db) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	if (oph_dim_check_connection_to_db(db->dbms_instance, NULL, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DIM_MYSQL_ERROR;
	}
	//Check if Databse is empty and DELETE
	char delete_query[MYSQL_BUFLEN];
	int n;
	n = snprintf(delete_query, MYSQL_BUFLEN, MYSQL_DIM_DELETE_DB, db->db_name);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_query(db->dbms_instance->conn, delete_query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL error: %s\n", mysql_error(db->dbms_instance->conn));
		return OPH_DIM_MYSQL_ERROR;
	}

	return OPH_DIM_SUCCESS;
}

int oph_dim_connect_to_dbms(oph_odb_dbms_instance * dbms, unsigned long flag)
{
	if (!dbms) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	if (mysql_library_init(0, NULL, NULL)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL initialization error\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	dbms->conn = NULL;
	if (!(dbms->conn = mysql_init(NULL))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL initialization error: %s\n", mysql_error(dbms->conn));
		oph_dim_disconnect_from_dbms(dbms);
		return OPH_DIM_MYSQL_ERROR;
	}

	if (!mysql_real_connect(dbms->conn, dbms->hostname, dbms->login, dbms->pwd, NULL, dbms->port, NULL, flag)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL connection error: %s\n", mysql_error(dbms->conn));
		oph_dim_disconnect_from_dbms(dbms);
		return OPH_DIM_MYSQL_ERROR;
	}
	return OPH_DIM_SUCCESS;
}

int oph_dim_use_db_of_dbms(oph_odb_dbms_instance * dbms, oph_odb_db_instance * db)
{
	if (!dbms || !db) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameters\n");
		return OPH_DIM_NULL_PARAM;
	}

	if (dbms->conn) {
		if (mysql_select_db(dbms->conn, db->db_name)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL use DB error: %s\n", mysql_error(dbms->conn));
			oph_dim_disconnect_from_dbms(dbms);
			return OPH_DIM_MYSQL_ERROR;
		}
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL connection not established\n");
		return OPH_DIM_MYSQL_ERROR;
	}
	return OPH_DIM_SUCCESS;
}

int oph_dim_check_connection_to_db(oph_odb_dbms_instance * dbms, oph_odb_db_instance * db, unsigned long flag)
{
	if (!dbms) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameters\n");
		return OPH_DIM_NULL_PARAM;
	}

	if (!(dbms->conn)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Connection was somehow closed.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_ping(dbms->conn)) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Connection was lost. Reconnecting...\n");
		mysql_close(dbms->conn);	// Flush any data related to previuos connection

		// Connect to database 
		if (oph_dim_connect_to_dbms(dbms, flag)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DB. Check access parameters.\n");
			oph_dim_disconnect_from_dbms(dbms);
			return OPH_DIM_MYSQL_ERROR;
		}
		if (db) {
			if (oph_dim_use_db_of_dbms(dbms, db)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
				oph_dim_disconnect_from_dbms(dbms);
				return OPH_DIM_MYSQL_ERROR;
			}
		}
	}
	return OPH_DIM_SUCCESS;
}

int oph_dim_disconnect_from_dbms(oph_odb_dbms_instance * dbms)
{
	if (!dbms) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	if (dbms->conn) {
		mysql_close(dbms->conn);
		dbms->conn = NULL;
	}
	mysql_library_end();
	return OPH_DIM_SUCCESS;
}

int oph_dim_create_empty_table(oph_odb_db_instance * db, char *dimension_table_name)
{
	if (!db || !dimension_table_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	if (oph_dim_check_connection_to_db(db->dbms_instance, db, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	char create_query[MYSQL_BUFLEN];
	int n = snprintf(create_query, MYSQL_BUFLEN, MYSQL_DIM_CREATE_TABLE, dimension_table_name);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_query(db->dbms_instance->conn, create_query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL error: %s\n", mysql_error(db->dbms_instance->conn));
		return OPH_DIM_MYSQL_ERROR;
	}

	return OPH_DIM_SUCCESS;
}

int oph_dim_retrieve_dimension(oph_odb_db_instance * db, char *dimension_table_name, int dimension_id, char **dim_row)
{
	if (!db || !dimension_table_name || !dimension_id || !dim_row) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	char query[MYSQL_BUFLEN];

	if (oph_dim_check_connection_to_db(db->dbms_instance, db, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DIM_MYSQL_ERROR;
	}
	//Execute query
	int n = snprintf(query, MYSQL_BUFLEN, MYSQL_DIM_SELECT_DIMENSION_ROW, dimension_table_name, dimension_id);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DIM_DATA_ERROR;
	}

	if (mysql_query(db->dbms_instance->conn, query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL error: %s\n", mysql_error(db->dbms_instance->conn));
		return OPH_DIM_MYSQL_ERROR;
	}
	// Init res
	MYSQL_RES *res;
	MYSQL_ROW row;
	int num_rows;
	res = mysql_store_result(db->dbms_instance->conn);

	if ((num_rows = mysql_num_rows(res)) != 1) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Too many rows found by query\n");
		mysql_free_result(res);
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_field_count(db->dbms_instance->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(res);
		return OPH_DIM_MYSQL_ERROR;
	}

	unsigned long *lengths = NULL;

	//Fill dimension row struct
	if ((row = mysql_fetch_row(res))) {
		if ((lengths = mysql_fetch_lengths(res))) {
			*dim_row = (char *) calloc(lengths[0], sizeof(char));
			memcpy(*dim_row, row[0], lengths[0]);
		}
	}
	mysql_free_result(res);

	return OPH_DIM_SUCCESS;
}

int oph_dim_compare_dimension(oph_odb_db_instance * db, char *dimension_table_name, char *dim_type, long long dim_size, char *dim_row, int dimension_id, int *match)
{
	return oph_dim_compare_dimension2(db, dimension_table_name, dim_type, dim_size, 0, dim_row, dimension_id, match);
}

int oph_dim_compare_dimension2(oph_odb_db_instance * db, char *dimension_table_name, char *dim_type, long long dim_size, char *apply_clause, char *dim_row, int dimension_id, int *match)
{
	if (!db || !dimension_table_name || !dim_type || !match || (!dim_size && dim_row) || (dim_size && !dim_row)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	*match = -1;

	if (oph_dim_check_connection_to_db(db->dbms_instance, db, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	char type_flag = oph_dim_typeof(dim_type);
	if (!type_flag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
		return OPH_DIM_DATA_ERROR;
	}
	//Insert into fragment
	MYSQL_STMT *stmt = mysql_stmt_init(db->dbms_instance->conn);
	if (!stmt) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL statement error: %s\n", mysql_error(db->dbms_instance->conn));
		return OPH_DIM_MYSQL_ERROR;
	}

	char select_query[MYSQL_BUFLEN];
	int n = snprintf(select_query, MYSQL_BUFLEN, MYSQL_DIM_GET_DIMENSION, (apply_clause ? apply_clause : MYSQL_DIMENSION), dimension_table_name, dimension_id);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_stmt_prepare(stmt, select_query, strlen(select_query))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL statement error: %u: %s\n", mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	int param_count = mysql_stmt_param_count(stmt);
	MYSQL_BIND *bind = (MYSQL_BIND *) calloc(param_count, sizeof(MYSQL_BIND));
	if (!bind) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input buffers\n");
		mysql_stmt_close(stmt);
		free(bind);
		return OPH_DIM_MYSQL_ERROR;
	}

	my_bool nn = !dim_row;
	unsigned long sizeof_var = 0;
	if (dim_size) {
		sizeof_var = dim_size * oph_dim_sizeof(type_flag);
		if (!sizeof_var) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
			mysql_stmt_close(stmt);
			free(bind);
			return OPH_DIM_DATA_ERROR;
		}
	}

	bind[param_count - 1].buffer_length = sizeof_var;
	bind[param_count - 1].buffer_type = MYSQL_TYPE_BLOB;
	bind[param_count - 1].length = dim_row ? &sizeof_var : 0;
	bind[param_count - 1].is_null = &nn;
	bind[param_count - 1].is_unsigned = 0;
	bind[param_count - 1].buffer = dim_row;

	if (mysql_stmt_bind_param(stmt, bind)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in mysql_stmt_bind_param() failed: %s\n", mysql_stmt_error(stmt));
		free(bind);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_stmt_execute(stmt)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in mysql_stmt_execute() failed: %s\n", mysql_stmt_error(stmt));
		free(bind);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}
	free(bind);

	unsigned long length;
	int data;
	bind = (MYSQL_BIND *) calloc(1, sizeof(MYSQL_BIND));
	bind[0].buffer_type = MYSQL_TYPE_LONG;
	bind[0].buffer = (char *) &data;
	bind[0].is_null = 0;
	bind[0].length = &length;
	bind[0].error = 0;

	if (mysql_stmt_bind_result(stmt, bind)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in mysql_stmt_bind_result() failed: %s\n", mysql_stmt_error(stmt));
		free(bind);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_stmt_store_result(stmt)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in mysql_stmt_store_result() failed: %s\n", mysql_stmt_error(stmt));
		free(bind);
		mysql_stmt_free_result(stmt);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_stmt_num_rows(stmt) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No rows found by query.\n");
		free(bind);
		mysql_stmt_free_result(stmt);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_stmt_field_count(stmt) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		free(bind);
		mysql_stmt_free_result(stmt);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}
	while (!mysql_stmt_fetch(stmt))
		*match = data;

	mysql_stmt_free_result(stmt);
	mysql_stmt_close(stmt);
	free(bind);

	return OPH_DIM_SUCCESS;
}

int oph_dim_insert_into_dimension_table(oph_odb_db_instance * db, char *dimension_table_name, char *dimension_type, long long dim_size, char *dim_row, int *dimension_id)
{
	if (!db || !dimension_table_name || !dimension_type || !dimension_id || (!dim_size && dim_row) || (dim_size && !dim_row)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	*dimension_id = 0;

	if (oph_dim_check_connection_to_db(db->dbms_instance, db, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	char type_flag = oph_dim_typeof(dimension_type);
	if (!type_flag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
		return OPH_DIM_DATA_ERROR;
	}
	//Insert into fragment
	MYSQL_STMT *stmt = mysql_stmt_init(db->dbms_instance->conn);
	if (!stmt) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL statement error: %s\n", mysql_error(db->dbms_instance->conn));
		return OPH_DIM_MYSQL_ERROR;
	}

	char select_query[MYSQL_BUFLEN];
	int n = snprintf(select_query, MYSQL_BUFLEN, MYSQL_DIM_INSERT_TABLE, dimension_table_name);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DIM_MYSQL_ERROR;
	}
	if (mysql_stmt_prepare(stmt, select_query, strlen(select_query))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL statement error: %u: %s\n", mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	int param_count = mysql_stmt_param_count(stmt);
	MYSQL_BIND *bind = (MYSQL_BIND *) calloc(param_count, sizeof(MYSQL_BIND));
	if (!bind) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input buffers\n");
		mysql_stmt_close(stmt);
		free(bind);
		return OPH_DIM_MYSQL_ERROR;
	}

	my_bool nn = !dim_row;
	unsigned long sizeof_var = 0;
	if (dim_size) {
		sizeof_var = dim_size * oph_dim_sizeof(type_flag);
		if (!sizeof_var) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
			mysql_stmt_close(stmt);
			free(bind);
			return OPH_DIM_DATA_ERROR;
		}
	}

	bind[param_count - 1].buffer_length = sizeof_var;
	bind[param_count - 1].buffer_type = MYSQL_TYPE_BLOB;
	bind[param_count - 1].length = dim_row ? &sizeof_var : 0;
	bind[param_count - 1].is_null = &nn;
	bind[param_count - 1].is_unsigned = 0;
	bind[param_count - 1].buffer = (char *) dim_row;

	if (mysql_stmt_bind_param(stmt, bind)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in mysql_stmt_bind_param() failed: %s\n", mysql_stmt_error(stmt));
		free(bind);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_stmt_execute(stmt)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in mysql_stmt_execute() failed: %s\n", mysql_stmt_error(stmt));
		free(bind);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	mysql_stmt_close(stmt);
	free(bind);

	if (!(*dimension_id = mysql_insert_id(db->dbms_instance->conn))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find last inserted datacube id\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	return OPH_DIM_SUCCESS;
}

int oph_dim_insert_into_dimension_table_from_query(oph_odb_db_instance * db, char *dimension_table_name, char *dimension_type, long long dim_size, char *query, char *dim_row, int *dimension_id)
{
	if (!db || !dimension_table_name || !dimension_type || !dimension_id || (!dim_size && dim_row) || (dim_size && !dim_row)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	*dimension_id = 0;

	if (oph_dim_check_connection_to_db(db->dbms_instance, db, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	char type_flag = oph_dim_typeof(dimension_type);
	if (!type_flag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
		return OPH_DIM_DATA_ERROR;
	}
	//Insert into fragment
	MYSQL_STMT *stmt = mysql_stmt_init(db->dbms_instance->conn);
	if (!stmt) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL statement error: %s\n", mysql_error(db->dbms_instance->conn));
		return OPH_DIM_MYSQL_ERROR;
	}

	int n;
	char translated_query[MYSQL_BUFLEN], *tquery = translated_query, *cquery;
	*translated_query = 0;
	while (query && *query && (cquery = strcasestr(query, MYSQL_DIMENSION))) {
		n = cquery - query;
		if (n) {
			strncpy(tquery, query, n);
			tquery += n;
		}
		if (tquery - translated_query + 1 < MYSQL_BUFLEN) {
			*tquery = '?';
			tquery++;
			*tquery = 0;
		} else {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Buffer overflow\n");
			return OPH_DIM_DATA_ERROR;
		}
		query = cquery + strlen(MYSQL_DIMENSION);
	}
	if (query && *query)
		snprintf(tquery, MYSQL_BUFLEN - strlen(translated_query), "%s", query);

	char select_query[MYSQL_BUFLEN];
	n = snprintf(select_query, MYSQL_BUFLEN, MYSQL_DIM_INSERT_TABLE_FROM_QUERY, dimension_table_name, translated_query);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DIM_MYSQL_ERROR;
	}
	if (mysql_stmt_prepare(stmt, select_query, strlen(select_query))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL statement error: %u: %s\n", mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	int i, param_count = mysql_stmt_param_count(stmt);
	MYSQL_BIND *bind = (MYSQL_BIND *) calloc(param_count, sizeof(MYSQL_BIND));
	if (!bind) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input buffers\n");
		mysql_stmt_close(stmt);
		free(bind);
		return OPH_DIM_MYSQL_ERROR;
	}

	unsigned long sizeof_var = 0;
	if (dim_size) {
		sizeof_var = dim_size * oph_dim_sizeof(type_flag);
		if (!sizeof_var) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
			mysql_stmt_close(stmt);
			free(bind);
			return OPH_DIM_DATA_ERROR;
		}
	}

	my_bool nn = !dim_row;
	for (i = 0; i < param_count; ++i) {
		bind[i].buffer_length = sizeof_var;
		bind[i].buffer_type = MYSQL_TYPE_BLOB;
		bind[i].length = dim_row ? &sizeof_var : 0;
		bind[i].is_null = &nn;
		bind[i].is_unsigned = 0;
		bind[i].buffer = (char *) dim_row;
	}

	if (mysql_stmt_bind_param(stmt, bind)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in mysql_stmt_bind_param() failed: %s\n", mysql_stmt_error(stmt));
		free(bind);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_stmt_execute(stmt)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in mysql_stmt_execute() failed: %s\n", mysql_stmt_error(stmt));
		free(bind);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	mysql_stmt_close(stmt);
	free(bind);

	if (!(*dimension_id = mysql_insert_id(db->dbms_instance->conn))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find last inserted datacube id\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	return OPH_DIM_SUCCESS;
}

int oph_dim_check_if_dimension_table_exists(oph_odb_db_instance * db, char *dimension_table_name, int *exist_flag)
{
	if (!db || !dimension_table_name || !exist_flag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	*exist_flag = 0;

	if (oph_dim_check_connection_to_db(db->dbms_instance, db, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	char selectQuery[MYSQL_BUFLEN];
	int n = snprintf(selectQuery, MYSQL_BUFLEN, MYSQL_DIM_CHECK_DIMENSION_TABLE, db->db_name, dimension_table_name);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DIM_DATA_ERROR;
	}

	if (mysql_query(db->dbms_instance->conn, selectQuery)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL query error: %s\n", mysql_error(db->dbms_instance->conn));
		return OPH_DIM_MYSQL_ERROR;
	}

	MYSQL_RES *res;
	res = mysql_store_result(db->dbms_instance->conn);
	int num_rows = mysql_num_rows(res);
	if (num_rows != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No rows found in query\n");
		mysql_free_result(res);
		return OPH_DIM_MYSQL_ERROR;
	}
	MYSQL_ROW row;

	row = mysql_fetch_row(res);
	*exist_flag = (int) strtol(row[0], NULL, 10);
	if (*exist_flag > 1)
		*exist_flag = 1;

	mysql_free_result(res);

	return OPH_DIM_SUCCESS;
}

int oph_dim_insert_into_dimension_table_rand_data(oph_odb_db_instance * db, char *dimension_table_name, char *dimension_type, long long dim_size, int *dimension_id)
{
	if (!db || !dimension_table_name || !dimension_type || !dim_size || !dimension_id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	*dimension_id = 0;

	if (oph_dim_check_connection_to_db(db->dbms_instance, db, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to OphidiaDB.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	char type_flag = oph_dim_typeof(dimension_type);
	if (!type_flag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
		return OPH_DIM_DATA_ERROR;
	}
	//Insert into fragment
	MYSQL_STMT *stmt = mysql_stmt_init(db->dbms_instance->conn);
	if (!stmt) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL statement error: %s\n", mysql_error(db->dbms_instance->conn));
		return OPH_DIM_MYSQL_ERROR;
	}

	char select_query[MYSQL_BUFLEN];
	int n = snprintf(select_query, MYSQL_BUFLEN, MYSQL_DIM_INSERT_TABLE, dimension_table_name);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DIM_MYSQL_ERROR;
	}
	if (mysql_stmt_prepare(stmt, select_query, strlen(select_query))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL statement error: %u: %s\n", mysql_stmt_errno(stmt), mysql_stmt_error(stmt));
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	int param_count = mysql_stmt_param_count(stmt);
	MYSQL_BIND *bind = (MYSQL_BIND *) calloc(param_count, sizeof(MYSQL_BIND));
	if (!bind) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input buffers\n");
		mysql_stmt_close(stmt);
		free(bind);
		return OPH_DIM_MYSQL_ERROR;
	}

	unsigned long sizeof_var = dim_size * oph_dim_sizeof(type_flag);
	if (!sizeof_var) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
		mysql_stmt_close(stmt);
		free(bind);
		return OPH_DIM_DATA_ERROR;
	}

	long long i = 0;

	//Fill random array
	char *dim_row = (char *) malloc(sizeof_var);
	void *dim_array = NULL;
	struct timeval time;
	gettimeofday(&time, NULL);
	srand(time.tv_sec * 1000000 + time.tv_usec);
	if (type_flag == OPH_DIM_BYTE_FLAG) {
		dim_array = (int *) malloc(dim_size * sizeof(char));
		for (i = 0; i < dim_size; i++)
			((char *) dim_array)[i] = ((double) rand() / RAND_MAX) * 99.0 + 1 + (i ? ((char *) dim_array)[i - 1] : 0);
	} else if (type_flag == OPH_DIM_SHORT_FLAG) {
		dim_array = (short *) malloc(dim_size * sizeof(short));
		for (i = 0; i < dim_size; i++)
			((short *) dim_array)[i] = ((double) rand() / RAND_MAX) * 99.0 + 1 + (i ? ((short *) dim_array)[i - 1] : 0);
	} else if (type_flag == OPH_DIM_INT_FLAG) {
		dim_array = (int *) malloc(dim_size * sizeof(int));
		for (i = 0; i < dim_size; i++)
			((int *) dim_array)[i] = ((double) rand() / RAND_MAX) * 99.0 + 1 + (i ? ((int *) dim_array)[i - 1] : 0);
	} else if (type_flag == OPH_DIM_LONG_FLAG) {
		dim_array = (long long *) malloc(dim_size * sizeof(long long));
		for (i = 0; i < dim_size; i++)
			((long long *) dim_array)[i] = ((double) rand() / RAND_MAX) * 99.0 + 1 + (i ? ((long long *) dim_array)[i - 1] : 0);
	} else if (type_flag == OPH_DIM_FLOAT_FLAG) {
		dim_array = (float *) malloc(dim_size * sizeof(float));
		for (i = 0; i < dim_size; i++)
			((float *) dim_array)[i] = ((float) rand() / RAND_MAX) * 99.0 + 1 + (i ? ((float *) dim_array)[i - 1] : 0);
	} else if (type_flag == OPH_DIM_DOUBLE_FLAG) {
		dim_array = (double *) malloc(dim_size * sizeof(double));
		for (i = 0; i < dim_size; i++)
			((double *) dim_array)[i] = ((double) rand() / RAND_MAX) * 99.0 + 1 + (i ? ((double *) dim_array)[i - 1] : 0);
	}
	memcpy(dim_row, (void *) dim_array, sizeof_var);
	free(dim_array);

	bind[param_count - 1].buffer_length = sizeof_var;
	bind[param_count - 1].buffer_type = MYSQL_TYPE_BLOB;
	bind[param_count - 1].length = &sizeof_var;
	bind[param_count - 1].is_null = 0;
	bind[param_count - 1].is_unsigned = 0;
	bind[param_count - 1].buffer = (char *) dim_row;

	if (mysql_stmt_bind_param(stmt, bind)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in mysql_stmt_bind_param() failed: %s\n", mysql_stmt_error(stmt));
		free(dim_row);
		free(bind);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_stmt_execute(stmt)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in mysql_stmt_execute() failed: %s\n", mysql_stmt_error(stmt));
		free(bind);
		free(dim_row);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	mysql_stmt_close(stmt);
	free(bind);
	free(dim_row);

	if (!(*dimension_id = mysql_insert_id(db->dbms_instance->conn))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to find last inserted datacube id\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	return OPH_DIM_SUCCESS;
}

int oph_dim_read_dimension_data(oph_odb_db_instance * db, char *dimension_table_name, int dimension_id, char *array_clause, int compressed, char **dim_row)
{
	if (!db || !dimension_table_name || !array_clause || !dim_row) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	*dim_row = 0;

	if (oph_dim_check_connection_to_db(db->dbms_instance, db, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	int n;
	char select_query[MYSQL_BUFLEN];
	if (compressed)
		n = snprintf(select_query, MYSQL_BUFLEN, MYSQL_DIM_RETRIEVE_COMPRESSED_DIMENSION_VALUES, array_clause, dimension_table_name, dimension_id);
	else
		n = snprintf(select_query, MYSQL_BUFLEN, MYSQL_DIM_RETRIEVE_DIMENSION_VALUES, array_clause, dimension_table_name, dimension_id);

	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_query(db->dbms_instance->conn, select_query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL error: %s\n", mysql_error(db->dbms_instance->conn));
		return OPH_DIM_MYSQL_ERROR;
	}

	MYSQL_RES *result = NULL;
	MYSQL_ROW row;

	// Init res
	result = mysql_store_result(db->dbms_instance->conn);

	if (mysql_num_rows(result) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query.\n");
		mysql_free_result(result);
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_field_count(db->dbms_instance->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(result);
		return OPH_DIM_MYSQL_ERROR;
	}

	unsigned long *lengths = NULL;
	row = mysql_fetch_row(result);
	lengths = mysql_fetch_lengths(result);

	if (lengths[0]) {
		*dim_row = (char *) calloc(lengths[0], sizeof(char));
		memcpy(*dim_row, row[0], lengths[0]);
	} else
		*dim_row = 0;

	mysql_free_result(result);

	return OPH_DIM_SUCCESS;
}

int oph_dim_read_dimension(oph_odb_db_instance * db, char *dimension_table_name, char *dimension_type, int dimension_id, int compressed, char **dim_row)
{
	if (!db || !dimension_table_name || !dim_row) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	*dim_row = 0;

	if (oph_dim_check_connection_to_db(db->dbms_instance, db, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	if (strcasecmp(dimension_type, OPH_DIM_BYTE_TYPE) && strcasecmp(dimension_type, OPH_DIM_SHORT_TYPE) && strcasecmp(dimension_type, OPH_DIM_INT_TYPE)
	    && strcasecmp(dimension_type, OPH_DIM_LONG_TYPE) && strcasecmp(dimension_type, OPH_DIM_FLOAT_TYPE) && strcasecmp(dimension_type, OPH_DIM_DOUBLE_TYPE)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
		return OPH_DIM_DATA_ERROR;
	}

	int n;
	char select_query[MYSQL_BUFLEN];
	if (compressed)
		n = snprintf(select_query, MYSQL_BUFLEN, MYSQL_DIM_RETRIEVE_COMPRESSED_DIMENSION_DUMP, dimension_type, dimension_table_name, dimension_id);
	else
		n = snprintf(select_query, MYSQL_BUFLEN, MYSQL_DIM_RETRIEVE_DIMENSION_DUMP, dimension_type, dimension_table_name, dimension_id);

	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_query(db->dbms_instance->conn, select_query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL error: %s\n", mysql_error(db->dbms_instance->conn));
		return OPH_DIM_MYSQL_ERROR;
	}

	MYSQL_RES *result = NULL;
	MYSQL_ROW row;

	// Init res
	result = mysql_store_result(db->dbms_instance->conn);

	if (mysql_num_rows(result) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No/more than one row found by query.\n");
		mysql_free_result(result);
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_field_count(db->dbms_instance->conn) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		mysql_free_result(result);
		return OPH_DIM_MYSQL_ERROR;
	}

	unsigned long *lengths = NULL;
	row = mysql_fetch_row(result);
	lengths = mysql_fetch_lengths(result);
	if (lengths[0]) {
		*dim_row = (char *) calloc(lengths[0] + 1, sizeof(char));
		snprintf(*dim_row, lengths[0] + 1, "%s", row[0]);
	} else
		*dim_row = 0;
	mysql_free_result(result);

	return OPH_DIM_SUCCESS;
}

int oph_dim_read_dimension_filtered_data(oph_odb_db_instance * db, char *dimension_table_name, int dimension_id, char *array_clause, int compressed, char **dim_row, char *dim_type, long long dim_size)
{
	if (!db || !dimension_table_name || !dim_row) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	if (oph_dim_check_connection_to_db(db->dbms_instance, db, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	char type_flag = oph_dim_typeof(dim_type);
	if (!type_flag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
		return OPH_DIM_DATA_ERROR;
	}
	//Insert into fragment
	MYSQL_STMT *stmt = mysql_stmt_init(db->dbms_instance->conn);
	if (!stmt) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL statement error: %s\n", mysql_error(db->dbms_instance->conn));
		return OPH_DIM_MYSQL_ERROR;
	}

	int n;
	char select_query[MYSQL_BUFLEN];
	if (compressed)
		n = snprintf(select_query, MYSQL_BUFLEN, MYSQL_DIM_RETRIEVE_COMPRESSED_DIMENSION_LABELS, dim_type, dim_type, array_clause, dimension_table_name, dimension_id);
	else
		n = snprintf(select_query, MYSQL_BUFLEN, MYSQL_DIM_RETRIEVE_DIMENSION_LABELS, dim_type, dim_type, array_clause, dimension_table_name, dimension_id);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_stmt_prepare(stmt, select_query, strlen(select_query))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL statement error: %u: %s in query %s\n", mysql_stmt_errno(stmt), mysql_stmt_error(stmt), select_query);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	int param_count = mysql_stmt_param_count(stmt);
	MYSQL_BIND *bind = (MYSQL_BIND *) calloc(param_count, sizeof(MYSQL_BIND));
	if (!bind) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Cannot allocate input buffers\n");
		mysql_stmt_close(stmt);
		free(bind);
		return OPH_DIM_MYSQL_ERROR;
	}


	my_bool nn = !(*dim_row);
	unsigned long sizeof_var = 0;
	if (dim_size)
		sizeof_var = dim_size * sizeof(long long);	// Indexes are long long

	bind[param_count - 1].buffer_length = sizeof_var;
	bind[param_count - 1].buffer_type = MYSQL_TYPE_BLOB;
	bind[param_count - 1].length = *dim_row ? &sizeof_var : 0;
	bind[param_count - 1].is_null = &nn;
	bind[param_count - 1].is_unsigned = 0;
	bind[param_count - 1].buffer = *dim_row;

	if (mysql_stmt_bind_param(stmt, bind)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in mysql_stmt_bind_param() failed: %s\n", mysql_stmt_error(stmt));
		free(bind);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_stmt_execute(stmt)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in mysql_stmt_execute() failed: %s\n", mysql_stmt_error(stmt));
		free(bind);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}
	free(bind);

	if (*dim_row)
		free(*dim_row);
	n = 0;

	sizeof_var = dim_size * oph_dim_sizeof(type_flag);
	if (!sizeof_var) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading data type\n");
		mysql_stmt_close(stmt);
		free(bind);
		return OPH_DIM_DATA_ERROR;
	}

	unsigned long length;
	*dim_row = (char *) malloc(sizeof_var);
	nn = !(*dim_row);
	bind = (MYSQL_BIND *) calloc(1, sizeof(MYSQL_BIND));
	bind[0].buffer_length = sizeof_var;
	bind[0].buffer_type = MYSQL_TYPE_BLOB;
	bind[0].length = &length;
	bind[0].buffer = *dim_row;
	bind[0].is_null = &nn;
	bind[0].error = 0;

	if (mysql_stmt_bind_result(stmt, bind)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in mysql_stmt_bind_result() failed: %s\n", mysql_stmt_error(stmt));
		free(*dim_row);
		*dim_row = 0;
		free(bind);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_stmt_store_result(stmt)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in mysql_stmt_store_result() failed: %s\n", mysql_stmt_error(stmt));
		free(*dim_row);
		*dim_row = 0;
		free(bind);
		mysql_stmt_free_result(stmt);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_stmt_num_rows(stmt) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "No rows found by query.\n");
		free(*dim_row);
		*dim_row = 0;
		free(bind);
		mysql_stmt_free_result(stmt);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_stmt_field_count(stmt) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Not enough fields found by query\n");
		free(*dim_row);
		*dim_row = 0;
		free(bind);
		mysql_stmt_free_result(stmt);
		mysql_stmt_close(stmt);
		return OPH_DIM_MYSQL_ERROR;
	}
	while (!mysql_stmt_fetch(stmt));

	mysql_stmt_free_result(stmt);
	mysql_stmt_close(stmt);
	free(bind);

	return OPH_DIM_SUCCESS;
}

int oph_dim_delete_table(oph_odb_db_instance * db, char *dimension_table_name)
{
	if (!dimension_table_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}

	if (oph_dim_check_connection_to_db(db->dbms_instance, db, 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to reconnect to DB.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	char delete_query[MYSQL_BUFLEN];
	int n = snprintf(delete_query, MYSQL_BUFLEN, MYSQL_DIM_DELETE_FRAG, dimension_table_name);
	if (n >= MYSQL_BUFLEN) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of query exceed query limit.\n");
		return OPH_DIM_MYSQL_ERROR;
	}

	if (mysql_query(db->dbms_instance->conn, delete_query)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL error: %s\n", mysql_error(db->dbms_instance->conn));
		return OPH_DIM_MYSQL_ERROR;
	}

	return OPH_DIM_SUCCESS;
}

int oph_dim_unload_dim_dbinstance(oph_odb_db_instance * db)
{
	if (!db || !db->dbms_instance) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_DIM_NULL_PARAM;
	}
	free(db->dbms_instance);
	return OPH_DIM_SUCCESS;
}
