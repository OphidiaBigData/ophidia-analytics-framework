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

#include <stdlib.h>
#include <string.h>

#include "debug.h"

int oph_utl_short_folder(int max_size, int start_size, char* path){
	  if(!max_size || !path || strlen(path) <= (unsigned int)start_size || (max_size - 3) <= start_size ){
	  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null parameter\n");
		return OPH_UTL_ERROR;
	  }

	  int old_strlen = strlen(path);
	  int new_strlen = max_size;

	  if(old_strlen > new_strlen){
	  	snprintf(path + start_size, (max_size - start_size) + 1, "...%s", path + (strlen(path) - (max_size - 3 - start_size)));
	  }
	  return OPH_UTL_SUCCESS;
}

int oph_utl_auto_compute_size(long long byte_size, double *computed_size, int *byte_unit){
	  if(!byte_size || !computed_size ){
	  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null parameter\n");
		return OPH_UTL_ERROR;
	  }

	  if(byte_size < OPH_UTL_MB_SIZE){
		*byte_unit = OPH_UTL_KB_UNIT_VALUE;
		return oph_utl_compute_size(byte_size, OPH_UTL_KB_UNIT_VALUE, computed_size);
	  }
	  else if(byte_size < OPH_UTL_GB_SIZE){
		*byte_unit = OPH_UTL_MB_UNIT_VALUE;
		return oph_utl_compute_size(byte_size, OPH_UTL_MB_UNIT_VALUE, computed_size);
	  }
	  else if(byte_size < OPH_UTL_TB_SIZE){
		*byte_unit = OPH_UTL_GB_UNIT_VALUE;
		return oph_utl_compute_size(byte_size, OPH_UTL_GB_UNIT_VALUE, computed_size);
	  }
	  else if(byte_size < OPH_UTL_PB_SIZE){
		*byte_unit = OPH_UTL_TB_UNIT_VALUE;
		return oph_utl_compute_size(byte_size, OPH_UTL_TB_UNIT_VALUE, computed_size);
	  }
	  else{
		*byte_unit = OPH_UTL_PB_UNIT_VALUE;
		return oph_utl_compute_size(byte_size, OPH_UTL_PB_UNIT_VALUE, computed_size);
	  }
	  return OPH_UTL_SUCCESS;
}

int oph_utl_compute_size(long long byte_size, int byte_unit, double *computed_size){
	  if(!byte_size || !byte_unit || !computed_size ){
	  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null parameter\n");
		return OPH_UTL_ERROR;
	  }

	  switch(byte_unit){
		case OPH_UTL_KB_UNIT_VALUE:
			*computed_size = (double)byte_size/OPH_UTL_KB_SIZE;
			break;
		case OPH_UTL_MB_UNIT_VALUE:
			*computed_size = (double)byte_size/OPH_UTL_MB_SIZE;
			break;
		case OPH_UTL_GB_UNIT_VALUE:
			*computed_size = (double)byte_size/OPH_UTL_GB_SIZE;
			break;
		case OPH_UTL_TB_UNIT_VALUE:
			*computed_size = (double)byte_size/OPH_UTL_TB_SIZE;
			break;
		case OPH_UTL_PB_UNIT_VALUE:
			*computed_size = (double)byte_size/OPH_UTL_PB_SIZE;
			break;
		default:
		  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Unrecognized byte unit size\n");
			return OPH_UTL_ERROR;
	  }
	  return OPH_UTL_SUCCESS;
}

int oph_utl_unit_to_str(int unit_value, char (*unit_str)[OPH_UTL_UNIT_SIZE]){
	  if(!unit_value || !unit_str ){
	  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null parameter\n");
		return OPH_UTL_ERROR;
	  }

	  switch(unit_value){
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

int oph_utl_unit_to_value(char unit_str[OPH_UTL_UNIT_SIZE], int *unit_value){
	  if(!unit_value || !unit_str ){
	  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null parameter\n");
		return OPH_UTL_ERROR;
	  }

	  if(strncasecmp(unit_str, OPH_UTL_KB_UNIT, OPH_UTL_UNIT_SIZE) == 0 )
		*unit_value = OPH_UTL_KB_UNIT_VALUE;
	  else if(strncasecmp(unit_str, OPH_UTL_MB_UNIT, OPH_UTL_UNIT_SIZE) == 0 )
		*unit_value = OPH_UTL_MB_UNIT_VALUE;
	  else if(strncasecmp(unit_str, OPH_UTL_GB_UNIT, OPH_UTL_UNIT_SIZE) == 0 )
		*unit_value = OPH_UTL_GB_UNIT_VALUE;
	  else if(strncasecmp(unit_str, OPH_UTL_TB_UNIT, OPH_UTL_UNIT_SIZE) == 0 )
		*unit_value = OPH_UTL_TB_UNIT_VALUE;
	  else if(strncasecmp(unit_str, OPH_UTL_PB_UNIT, OPH_UTL_UNIT_SIZE) == 0 )
		*unit_value = OPH_UTL_PB_UNIT_VALUE;
	  else{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unrecognized byte unit size\n");
		return OPH_UTL_ERROR;
	  }

	  return OPH_UTL_SUCCESS;
}

