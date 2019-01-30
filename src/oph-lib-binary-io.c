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

#include "oph-lib-binary-io.h"

extern int msglevel;


int oph_iob_copy_in_binary(void *num, char **bin_val, size_t* length, unsigned int num_type){
        size_t sizeof_num;
        int res = oph_iob_sizeof_type(num_type, &sizeof_num);
        if(res)
                return res;

        *bin_val = (char*)malloc(sizeof_num*sizeof(char));
        if(!(*bin_val)){
                pmesg(1, __FILE__, __LINE__, "Not enough memory for conversion");
                return OPH_IOB_NOMEM;
        }
        memcpy(*bin_val, (void*)(num), sizeof_num);
        *length = sizeof_num;
        return OPH_IOB_OK;
}

int oph_iob_readas_binary(void *num, char **bin_val, size_t* length, unsigned int num_type){
        size_t sizeof_num;
        int res = oph_iob_sizeof_type(num_type, &sizeof_num);
        if(res)
                return res;
        *bin_val = (char*)num;
        *length=sizeof_num;
        return OPH_IOB_OK;
}

int oph_iob_copy_in_numeric(const char *bin_val, void *num, unsigned int num_type){
        if(!bin_val){
                pmesg(1, __FILE__, __LINE__, "No data to copy");
                return OPH_IOB_NODATA;
        }
        switch(num_type){
                case OPH_IOB_LONG:
                        *(long long*)num = *(long long*)bin_val;
                        break;
                case OPH_IOB_INT:
                        *(int*)num = *(int*)bin_val;
                        break;
                case OPH_IOB_SHORT:
                        *(short*)num = *(short*)bin_val;
                        break;
                case OPH_IOB_FLOAT:
                        *(float*)num = *(float*)bin_val;
                        break;
                case OPH_IOB_DOUBLE:
                        *(double*)num = *(double*)bin_val;
                        break;
		case OPH_IOB_BYTE:
		case OPH_IOB_CHAR:
                        *(char*)num = *(char*)bin_val;
                        break;
                default:
                        pmesg(1, __FILE__, __LINE__, "Numerical type not recognized");
                        return OPH_IOB_TYPEUNDEF;
        }
        return OPH_IOB_OK;
}

int oph_iob_read_as_numeric(const char *bin_val, void** num, unsigned int num_type){
        if(!bin_val){
                pmesg(1, __FILE__, __LINE__, "No data to read");
                return OPH_IOB_NODATA;
        }
        switch(num_type){
                case OPH_IOB_LONG:
                        *(long long**)num = (long long*)bin_val;
                        break;
                case OPH_IOB_INT:
                        *(int**)num = (int*)bin_val;
                        break;
                case OPH_IOB_SHORT:
                        *(short**)num = (short*)bin_val;
                        break;
                case OPH_IOB_FLOAT:
                        *(float**)num = (float*)bin_val;
                        break;
                case OPH_IOB_DOUBLE:
                        *(double**)num = (double*)bin_val;
                        break;
		case OPH_IOB_BYTE:
		case OPH_IOB_CHAR:
                        *(char**)num = (char*)bin_val;
                        break;
                default:
                        pmesg(1, __FILE__, __LINE__, "Numerical type not recognized");
                        return OPH_IOB_TYPEUNDEF;
        }
        return OPH_IOB_OK;
}

int oph_iob_copy_in_binary_array(const void* num_array, long long array_length, char** bin_array, long long *length, unsigned int num_type){
        size_t sizeof_num;
        int res = oph_iob_sizeof_type(num_type, &sizeof_num);
        if(res)
                return res;

        *bin_array = (char*)malloc(array_length*sizeof_num*sizeof(char));
        if(!(*bin_array)){
                pmesg(1, __FILE__, __LINE__, "Not enough memory for copying");
                return OPH_IOB_NOMEM;
        }
        memcpy(*bin_array, (void*)(num_array), sizeof_num*array_length);
        *length = sizeof_num*array_length;
        return OPH_IOB_OK;
}

int oph_iob_read_as_binary_array(const void* num_array, long long array_length, char** bin_array, long long *length, unsigned int num_type){
        size_t sizeof_num;
        int res = oph_iob_sizeof_type(num_type, &sizeof_num);
        if(res)
                return res;

        *bin_array = (char*)num_array;
        *length = sizeof_num*array_length;
        return OPH_IOB_OK;
}

int oph_iob_copy_in_numeric_array(const char *bin_array, long long array_length, void** num_array, unsigned int num_type){
        if(!bin_array){
                pmesg(1, __FILE__, __LINE__, "No data to copy");
                return OPH_IOB_NODATA;
        }
        size_t sizeof_num;
        int res = oph_iob_sizeof_type(num_type, &sizeof_num);
        if(res)
                return res;
        *num_array = malloc(sizeof_num*array_length*sizeof(char));
        if(!(*num_array)){
                pmesg(1, __FILE__, __LINE__, "Not enough memory for copying");
                return OPH_IOB_NOMEM;
        }
        memcpy(*num_array, (void*)(bin_array), sizeof_num*array_length);

        return OPH_IOB_OK;
}

int oph_iob_read_as_numeric_array(const char *bin_array, long long array_length, void** num_array, unsigned int num_type){
        if(!bin_array || !array_length){
                pmesg(1, __FILE__, __LINE__, "No data to read");
                return OPH_IOB_NODATA;
        }
        switch(num_type){
                case OPH_IOB_LONG:
                        *(long long**)num_array = (long long*)bin_array;
                        break;
                case OPH_IOB_INT:
                        *(int**)num_array = (int*)bin_array;
                        break;
                case OPH_IOB_SHORT:
                        *(short**)num_array = (short*)bin_array;
                        break;
                case OPH_IOB_FLOAT:
                        *(float**)num_array = (float*)bin_array;
                        break;
                case OPH_IOB_DOUBLE:
                        *(double**)num_array = (double*)bin_array;
                        break;
		case OPH_IOB_BYTE:
		case OPH_IOB_CHAR:
                        *(char**)num_array = (char*)bin_array;
                        break;
                default:
                        pmesg(1, __FILE__, __LINE__, "Numerical type not recognized");
                        return OPH_IOB_TYPEUNDEF;
        }
        return OPH_IOB_OK;

}

int oph_iob_bin_array_create(char **bin_array, long long num_values, int oph_iob_type){
        size_t sizeof_num;
        int res = oph_iob_sizeof_type(oph_iob_type, &sizeof_num);
        if(res)
                return res;

        *bin_array = (char*)malloc(sizeof_num*num_values*sizeof(char));
        if(!(*bin_array)){
                pmesg(1, __FILE__, __LINE__, "Not enough memory");
                return OPH_IOB_NOMEM;
        }
        return OPH_IOB_OK;
}

int oph_iob_bin_array_add(char *bin_array, void* num, long long position, unsigned int oph_iob_type){
        if(!bin_array){
                pmesg(1, __FILE__, __LINE__, "Invalid binary buffer");
                return OPH_IOB_NOTBUFFER;
        }
        size_t sizeof_num;
        int res = oph_iob_sizeof_type(oph_iob_type, &sizeof_num);
        if(res)
                return res;

        memcpy(bin_array+(position*sizeof_num), (void*)(num), sizeof_num);
        
        return OPH_IOB_OK;
}

int oph_iob_bin_array_get(const char *bin_array, char **bin_val, long long position, unsigned int oph_iob_type){
        if(!bin_array){
                pmesg(1, __FILE__, __LINE__, "Invalid binary buffer");
                return OPH_IOB_NOTBUFFER;
        }
        size_t sizeof_num;
        int res = oph_iob_sizeof_type(oph_iob_type, &sizeof_num);
        if(res)
                return res;

        *bin_val = (char *)(bin_array+(position*sizeof_num));
        return OPH_IOB_OK;
}

/* INTERNAL FUNCTIONS SECTION */
int oph_iob_sizeof_type(unsigned int num_type, size_t* sizeof_num){
        switch(num_type){
                case OPH_IOB_LONG:
                        *sizeof_num = OPH_IOB_SIZEOFLONG;
                        break;
                case OPH_IOB_INT:
                        *sizeof_num = OPH_IOB_SIZEOFINT;
                        break;
                case OPH_IOB_SHORT:
                        *sizeof_num = OPH_IOB_SIZEOFSHORT;
                        break;
                case OPH_IOB_FLOAT:
                        *sizeof_num = OPH_IOB_SIZEOFFLOAT;
                        break;
                case OPH_IOB_DOUBLE:
                        *sizeof_num = OPH_IOB_SIZEOFDOUBLE;
                        break;
		case OPH_IOB_BYTE:
			*sizeof_num = OPH_IOB_SIZEOFBYTE;
			break;
		case OPH_IOB_CHAR:
                        *sizeof_num = OPH_IOB_SIZEOFCHAR;
                        break;
                default:
                        pmesg(1, __FILE__, __LINE__, "Numerical type non recognized");
                        return OPH_IOB_TYPEUNDEF;
        }
        return OPH_IOB_OK;
}

