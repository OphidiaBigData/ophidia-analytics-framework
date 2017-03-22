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

#define _GNU_SOURCE
#include "oph_utility_library.h"

#include <string.h>
#include <inttypes.h>

#include "debug.h"

int oph_utl_short_folder(int max_size, int start_size, char *path)
{
	if (!max_size || !path || strlen(path) <= (unsigned int) start_size || (max_size - 3) <= start_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null parameter\n");
		return OPH_UTL_ERROR;
	}

	int old_strlen = strlen(path);
	int new_strlen = max_size;

	if (old_strlen > new_strlen) {
		snprintf(path + start_size, (max_size - start_size) + 1, "...%s", path + (strlen(path) - (max_size - 3 - start_size)));
	}
	return OPH_UTL_SUCCESS;
}

int oph_utl_auto_compute_size(long long byte_size, double *computed_size, int *byte_unit)
{
	if (!byte_size || !computed_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null parameter\n");
		return OPH_UTL_ERROR;
	}

	if (byte_size < OPH_UTL_MB_SIZE) {
		*byte_unit = OPH_UTL_KB_UNIT_VALUE;
		return oph_utl_compute_size(byte_size, OPH_UTL_KB_UNIT_VALUE, computed_size);
	} else if (byte_size < OPH_UTL_GB_SIZE) {
		*byte_unit = OPH_UTL_MB_UNIT_VALUE;
		return oph_utl_compute_size(byte_size, OPH_UTL_MB_UNIT_VALUE, computed_size);
	} else if (byte_size < OPH_UTL_TB_SIZE) {
		*byte_unit = OPH_UTL_GB_UNIT_VALUE;
		return oph_utl_compute_size(byte_size, OPH_UTL_GB_UNIT_VALUE, computed_size);
	} else if (byte_size < OPH_UTL_PB_SIZE) {
		*byte_unit = OPH_UTL_TB_UNIT_VALUE;
		return oph_utl_compute_size(byte_size, OPH_UTL_TB_UNIT_VALUE, computed_size);
	} else {
		*byte_unit = OPH_UTL_PB_UNIT_VALUE;
		return oph_utl_compute_size(byte_size, OPH_UTL_PB_UNIT_VALUE, computed_size);
	}
	return OPH_UTL_SUCCESS;
}

int oph_utl_compute_size(long long byte_size, int byte_unit, double *computed_size)
{
	if (!byte_size || !byte_unit || !computed_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null parameter\n");
		return OPH_UTL_ERROR;
	}

	switch (byte_unit) {
		case OPH_UTL_KB_UNIT_VALUE:
			*computed_size = (double) byte_size / OPH_UTL_KB_SIZE;
			break;
		case OPH_UTL_MB_UNIT_VALUE:
			*computed_size = (double) byte_size / OPH_UTL_MB_SIZE;
			break;
		case OPH_UTL_GB_UNIT_VALUE:
			*computed_size = (double) byte_size / OPH_UTL_GB_SIZE;
			break;
		case OPH_UTL_TB_UNIT_VALUE:
			*computed_size = (double) byte_size / OPH_UTL_TB_SIZE;
			break;
		case OPH_UTL_PB_UNIT_VALUE:
			*computed_size = (double) byte_size / OPH_UTL_PB_SIZE;
			break;
		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unrecognized byte unit size\n");
			return OPH_UTL_ERROR;
	}
	return OPH_UTL_SUCCESS;
}

int oph_utl_unit_to_str(int unit_value, char (*unit_str)[OPH_UTL_UNIT_SIZE])
{
	if (!unit_value || !unit_str) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null parameter\n");
		return OPH_UTL_ERROR;
	}

	switch (unit_value) {
		case OPH_UTL_KB_UNIT_VALUE:
			snprintf(*unit_str, OPH_UTL_UNIT_SIZE, OPH_UTL_KB_UNIT);
			break;
		case OPH_UTL_MB_UNIT_VALUE:
			snprintf(*unit_str, OPH_UTL_UNIT_SIZE, OPH_UTL_MB_UNIT);
			break;
		case OPH_UTL_GB_UNIT_VALUE:
			snprintf(*unit_str, OPH_UTL_UNIT_SIZE, OPH_UTL_GB_UNIT);
			break;
		case OPH_UTL_TB_UNIT_VALUE:
			snprintf(*unit_str, OPH_UTL_UNIT_SIZE, OPH_UTL_TB_UNIT);
			break;
		case OPH_UTL_PB_UNIT_VALUE:
			snprintf(*unit_str, OPH_UTL_UNIT_SIZE, OPH_UTL_PB_UNIT);
			break;
		default:
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unrecognized byte unit size\n");
			return OPH_UTL_ERROR;
	}
	return OPH_UTL_SUCCESS;
}

int oph_utl_unit_to_value(char unit_str[OPH_UTL_UNIT_SIZE], int *unit_value)
{
	if (!unit_value || !unit_str) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null parameter\n");
		return OPH_UTL_ERROR;
	}

	if (strncasecmp(unit_str, OPH_UTL_KB_UNIT, OPH_UTL_UNIT_SIZE) == 0)
		*unit_value = OPH_UTL_KB_UNIT_VALUE;
	else if (strncasecmp(unit_str, OPH_UTL_MB_UNIT, OPH_UTL_UNIT_SIZE) == 0)
		*unit_value = OPH_UTL_MB_UNIT_VALUE;
	else if (strncasecmp(unit_str, OPH_UTL_GB_UNIT, OPH_UTL_UNIT_SIZE) == 0)
		*unit_value = OPH_UTL_GB_UNIT_VALUE;
	else if (strncasecmp(unit_str, OPH_UTL_TB_UNIT, OPH_UTL_UNIT_SIZE) == 0)
		*unit_value = OPH_UTL_TB_UNIT_VALUE;
	else if (strncasecmp(unit_str, OPH_UTL_PB_UNIT, OPH_UTL_UNIT_SIZE) == 0)
		*unit_value = OPH_UTL_PB_UNIT_VALUE;
	else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unrecognized byte unit size\n");
		return OPH_UTL_ERROR;
	}

	return OPH_UTL_SUCCESS;
}

/*
	Base64 encoding & decoding code
	Retrieved from: https://en.wikibooks.org/wiki/Algorithm_Implementation/Miscellaneous/Base64
*/
int oph_utl_base64encode(const void *data_buf, size_t dataLength, char *result, size_t resultSize)
{
	const char base64chars[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	const uint8_t *data = (const uint8_t *) data_buf;
	size_t resultIndex = 0;
	size_t x;
	uint32_t n = 0;
	int padCount = dataLength % 3;
	uint8_t n0, n1, n2, n3;

	/* increment over the length of the string, three characters at a time */
	for (x = 0; x < dataLength; x += 3) {
		/* these three 8-bit (ASCII) characters become one 24-bit number */
		n = ((uint32_t) data[x]) << 16;	//parenthesis needed, compiler depending on flags can do the shifting before conversion to uint32_t, resulting to 0

		if ((x + 1) < dataLength)
			n += ((uint32_t) data[x + 1]) << 8;	//parenthesis needed, compiler depending on flags can do the shifting before conversion to uint32_t, resulting to 0

		if ((x + 2) < dataLength)
			n += data[x + 2];

		/* this 24-bit number gets separated into four 6-bit numbers */
		n0 = (uint8_t) (n >> 18) & 63;
		n1 = (uint8_t) (n >> 12) & 63;
		n2 = (uint8_t) (n >> 6) & 63;
		n3 = (uint8_t) n & 63;

		/*
		 * if we have one byte available, then its encoding is spread
		 * out over two characters
		 */
		if (resultIndex >= resultSize)
			return 1;	/* indicate failure: buffer too small */
		result[resultIndex++] = base64chars[n0];
		if (resultIndex >= resultSize)
			return 1;	/* indicate failure: buffer too small */
		result[resultIndex++] = base64chars[n1];

		/*
		 * if we have only two bytes available, then their encoding is
		 * spread out over three chars
		 */
		if ((x + 1) < dataLength) {
			if (resultIndex >= resultSize)
				return 1;	/* indicate failure: buffer too small */
			result[resultIndex++] = base64chars[n2];
		}

		/*
		 * if we have all three bytes available, then their encoding is spread
		 * out over four characters
		 */
		if ((x + 2) < dataLength) {
			if (resultIndex >= resultSize)
				return 1;	/* indicate failure: buffer too small */
			result[resultIndex++] = base64chars[n3];
		}
	}

	/*
	 * create and add padding that is required if we did not have a multiple of 3
	 * number of characters available
	 */
	if (padCount > 0) {
		for (; padCount < 3; padCount++) {
			if (resultIndex >= resultSize)
				return 1;	/* indicate failure: buffer too small */
			result[resultIndex++] = '=';
		}
	}
	if (resultIndex >= resultSize)
		return 1;	/* indicate failure: buffer too small */
	result[resultIndex] = 0;
	return 0;		/* indicate success */
}

#define WHITESPACE 64
#define EQUALS     65
#define INVALID    66

static const unsigned char oph_utl_d[] = {
	66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 64, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 64, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 62, 66, 66, 66, 63, 52, 53,
	54, 55, 56, 57, 58, 59, 60, 61, 66, 66, 66, 65, 66, 66, 66, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 66, 66, 66, 66, 66, 66, 26, 27, 28,
	29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66, 66,
	66, 66, 66, 66, 66, 66
};

int _oph_utl_base64decode(const char *in, size_t inLen, char *out, size_t * outLen)
{
	const char *end = in + inLen;
	char iter = 0;
	size_t buf = 0, len = 0;

	while (in < end) {
		unsigned char c = oph_utl_d[(int) (*in++)];

		switch (c) {
			case WHITESPACE:
				continue;	/* skip whitespace */
			case INVALID:
				return 1;	/* invalid input, return error */
			case EQUALS:	/* pad character, end of data */
				in = end;
				continue;
			default:
				buf = buf << 6 | c;
				iter++;	// increment the number of iteration
				/* If the buffer is full, split it into bytes */
				if (iter == 4) {
					if ((len += 3) > *outLen)
						return 1;	/* buffer overflow */
					(*out) = (buf >> 16) & 255;
					out++;
					(*out) = (buf >> 8) & 255;
					out++;
					(*out) = buf & 255;
					out++;
					buf = 0;
					iter = 0;

				}
		}
	}

	if (iter == 3) {
		if ((len += 2) > *outLen)
			return 1;	/* buffer overflow */
		(*out) = (buf >> 10) & 255;
		out++;
		(*out) = (buf >> 2) & 255;
		out++;
	} else if (iter == 2) {
		if (++len > *outLen)
			return 1;	/* buffer overflow */
		(*out) = (buf >> 4) & 255;
		out++;
	}

	*outLen = len;		/* modify to reflect the actual output size */

	return 0;
}

int oph_utl_base64decode(const char *query, char **new_query)
{
	if (!query || !new_query)
		return 1;
	size_t len = OPH_COMMON_BUFFER_LEN;
	char *_query = (char *) malloc(len * sizeof(char));
	if (!_query) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocation error");
		return 1;
	}
	char *query_ = _query;
	int result_code = _oph_utl_base64decode(query, strlen(query), _query, &len);
	if (!result_code) {
		if (len + 1 < OPH_COMMON_BUFFER_LEN)
			*(query_ + len) = 0;
		else {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Space not available");
			return -2;
		}
	}
	*new_query = query_;
	return result_code;
}
