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
#ifndef __OPH_BINARY_IO_LIB__
#define __OPH_BINARY_IO_LIB__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "debug.h"

#ifndef OPH_IOB_SIZEOFDOUBLE
        #define OPH_IOB_SIZEOFDOUBLE sizeof(double)
#endif
#ifndef OPH_IOB_SIZEOFLONG
        #define OPH_IOB_SIZEOFLONG sizeof(long long)
#endif
#ifndef OPH_IOB_SIZEOFFLOAT
        #define OPH_IOB_SIZEOFFLOAT sizeof(float)
#endif
#ifndef OPH_IOB_SIZEOFINT
        #define OPH_IOB_SIZEOFINT sizeof(int)
#endif
#ifndef OPH_IOB_SIZEOFCHAR
        #define OPH_IOB_SIZEOFCHAR sizeof(char)
#endif
#ifndef OPH_IOB_SIZEOFBYTE
        #define OPH_IOB_SIZEOFBYTE sizeof(char)
#endif
#ifndef OPH_IOB_SIZEOFSHORT
        #define OPH_IOB_SIZEOFSHORT sizeof(short)
#endif

/* Return codes */
#define OPH_IOB_OK 0
#define OPH_IOB_TYPEUNDEF 1
#define OPH_IOB_NOMEM 2
#define OPH_IOB_NODATA 3
#define OPH_IOB_NOTBUFFER 4

/* OPH_IOB_TYPE */
#define OPH_IOB_INVALID_TYPE 0
#define OPH_IOB_INT 1
#define OPH_IOB_FLOAT 2
#define OPH_IOB_DOUBLE 3
#define OPH_IOB_CHAR 4
#define OPH_IOB_LONG 5
#define OPH_IOB_SHORT 6
#define OPH_IOB_BYTE 7

/* Macro are used for defining the particular (double | float | int | long) functions */

/**
 * \brief Read a single numeric (double | float | int | long) value as a binary value
 * \param num Pointer to numeric value
 * \param bin_val Pointer to the result binary value; it reads the same memory area of the related numeric variable
 * \param length It will be setted to the byte length of the binary value
 * \return 0 if succes, != 0 otherwise
 */
#define oph_iob_readas_binary_i(num,bin_val,length) oph_iob_readas_binary(num,bin_val,length,OPH_IOB_INT)
#define oph_iob_readas_binary_f(num,bin_val,length) oph_iob_readas_binary(num,bin_val,length,OPH_IOB_FLOAT)
#define oph_iob_readas_binary_d(num,bin_val,length) oph_iob_readas_binary(num,bin_val,length,OPH_IOB_DOUBLE)
#define oph_iob_readas_binary_c(num,bin_val,length) oph_iob_readas_binary(num,bin_val,length,OPH_IOB_CHAR)
#define oph_iob_readas_binary_l(num,bin_val,length) oph_iob_readas_binary(num,bin_val,length,OPH_IOB_LONG)
#define oph_iob_readas_binary_s(num,bin_val,length) oph_iob_readas_binary(num,bin_val,length,OPH_IOB_SHORT)
#define oph_iob_readas_binary_b(num,bin_val,length) oph_iob_readas_binary(num,bin_val,length,OPH_IOB_BYTE)
int oph_iob_readas_binary(void *num, char **bin_val, size_t* length, unsigned int num_type);


/**
 * \brief Copy a single numeric (double | float | int | long) value in a binary value
 * \param num Numeric value to copy
 * \param bin_val Pointer to the copied binary value; it may subsequently be deallocated
 * \param length It will be setted to the byte length of the binary value
 * \return 0 if succes, != 0 otherwise
 */
#define oph_iob_copy_in_binary_i(num,bin_val,length) oph_iob_copy_in_binary(num,bin_val,length,OPH_IOB_INT)
#define oph_iob_copy_in_binary_f(num,bin_val,length) oph_iob_copy_in_binary(num,bin_val,length,OPH_IOB_FLOAT)
#define oph_iob_copy_in_binary_d(num,bin_val,length) oph_iob_copy_in_binary(num,bin_val,length,OPH_IOB_DOUBLE)
#define oph_iob_copy_in_binary_c(num,bin_val,length) oph_iob_copy_in_binary(num,bin_val,length,OPH_IOB_CHAR)
#define oph_iob_copy_in_binary_l(num,bin_val,length) oph_iob_copy_in_binary(num,bin_val,length,OPH_IOB_LONG)
#define oph_iob_copy_in_binary_s(num,bin_val,length) oph_iob_copy_in_binary(num,bin_val,length,OPH_IOB_SHORT)
#define oph_iob_copy_in_binary_b(num,bin_val,length) oph_iob_copy_in_binary(num,bin_val,length,OPH_IOB_BYTE)
int oph_iob_copy_in_binary(void *num, char **bin_val, size_t* length, unsigned int num_type);


/**
 * \brief Read a single binary value as a numeric (double | float | int | long ) value
 * \param bin_val Pointer to the binary value
 * \param num Pointer to result numeric value; it reads the same memory area of the related binary variable
 * \return 0 if succes, != 0 otherwise
 */
#define oph_iob_read_as_numeric_i(bin_val,num) oph_iob_read_as_numeric(bin_val,num,OPH_IOB_INT)
#define oph_iob_read_as_numeric_f(bin_val,num) oph_iob_read_as_numeric(bin_val,num,OPH_IOB_FLOAT)
#define oph_iob_read_as_numeric_d(bin_val,num) oph_iob_read_as_numeric(bin_val,num,OPH_IOB_DOUBLE)
#define oph_iob_read_as_numeric_c(bin_val,num) oph_iob_read_as_numeric(bin_val,num,OPH_IOB_CHAR)
#define oph_iob_read_as_numeric_l(bin_val,num) oph_iob_read_as_numeric(bin_val,num,OPH_IOB_LONG)
#define oph_iob_read_as_numeric_s(bin_val,num) oph_iob_read_as_numeric(bin_val,num,OPH_IOB_SHORT)
#define oph_iob_read_as_numeric_b(bin_val,num) oph_iob_read_as_numeric(bin_val,num,OPH_IOB_BYTE)
int oph_iob_read_as_numeric(const char *bin_val, void** num, unsigned int num_type);


/**
 * \brief Copy a single binary value in a numeric (double | float | int | long) value
 * \param bin_val Pointer to the binary value
 * \param num Pointer to result numeric value; it is a copy of the related binary variable
 * \return 0 if succes, != 0 otherwise
 */
#define oph_iob_copy_in_numeric_i(bin_val,num) oph_iob_copy_in_numeric(bin_val,num,OPH_IOB_INT)
#define oph_iob_copy_in_numeric_f(bin_val,num) oph_iob_copy_in_numeric(bin_val,num,OPH_IOB_FLOAT)
#define oph_iob_copy_in_numeric_d(bin_val,num) oph_iob_copy_in_numeric(bin_val,num,OPH_IOB_DOUBLE)
#define oph_iob_copy_in_numeric_c(bin_val,num) oph_iob_copy_in_numeric(bin_val,num,OPH_IOB_CHAR)
#define oph_iob_copy_in_numeric_l(bin_val,num) oph_iob_copy_in_numeric(bin_val,num,OPH_IOB_LONG)
#define oph_iob_copy_in_numeric_s(bin_val,num) oph_iob_copy_in_numeric(bin_val,num,OPH_IOB_SHORT)
#define oph_iob_copy_in_numeric_b(bin_val,num) oph_iob_copy_in_numeric(bin_val,num,OPH_IOB_BYTE)
int oph_iob_copy_in_numeric(const char *bin_val, void* num, unsigned int num_type);


/**
 * \brief Read an array of numeric (double | float | int | long) values as an array of binary values
 * \param num_array Array of numeric values
 * \param array_length Number of elements in the array
 * \param bin_array Pointer to the result binary array; it reads the same memory area of the related numeric array
 * \param length It will be setted to the byte length of the array of binary values
 * \return 0 if succes, != 0 otherwise
 */
#define oph_iob_read_as_binary_array_i(num_array,array_length,bin_array,length) oph_iob_read_as_binary_array(num_array,array_length,bin_array,length,OPH_IOB_INT)
#define oph_iob_read_as_binary_array_f(num_array,array_length,bin_array,length) oph_iob_read_as_binary_array(num_array,array_length,bin_array,length,OPH_IOB_FLOAT)
#define oph_iob_read_as_binary_array_d(num_array,array_length,bin_array,length) oph_iob_read_as_binary_array(num_array,array_length,bin_array,length,OPH_IOB_DOUBLE)
#define oph_iob_read_as_binary_array_c(num_array,array_length,bin_array,length) oph_iob_read_as_binary_array(num_array,array_length,bin_array,length,OPH_IOB_CHAR)
#define oph_iob_read_as_binary_array_l(num_array,array_length,bin_array,length) oph_iob_read_as_binary_array(num_array,array_length,bin_array,length,OPH_IOB_LONG)
#define oph_iob_read_as_binary_array_s(num_array,array_length,bin_array,length) oph_iob_read_as_binary_array(num_array,array_length,bin_array,length,OPH_IOB_SHORT)
#define oph_iob_read_as_binary_array_b(num_array,array_length,bin_array,length) oph_iob_read_as_binary_array(num_array,array_length,bin_array,length,OPH_IOB_BYTE)
int oph_iob_read_as_binary_array(const void* num_array, long long array_length, char** bin_array, long long *length, unsigned int num_type);


/**
 * \brief Copy an array of numeric (double | float | int | long) values in an array of binary values
 * \param num_array Array of numeric values to copy
 * \param array_length Number of elements in the array to copy
 * \param bin_array Pointer to the copied array of binary values; it may subsequently be deallocated
 * \param length It will be setted to the byte length of the array of binary values
 * \return 0 if succes, != 0 otherwise
 */
#define oph_iob_copy_in_binary_array_i(num_array,array_length,bin_array,length) oph_iob_copy_in_binary_array(num_array,array_length,bin_array,length,OPH_IOB_INT)
#define oph_iob_copy_in_binary_array_f(num_array,array_length,bin_array,length) oph_iob_copy_in_binary_array(num_array,array_length,bin_array,length,OPH_IOB_FLOAT)
#define oph_iob_copy_in_binary_array_d(num_array,array_length,bin_array,length) oph_iob_copy_in_binary_array(num_array,array_length,bin_array,length,OPH_IOB_DOUBLE)
#define oph_iob_copy_in_binary_array_c(num_array,array_length,bin_array,length) oph_iob_copy_in_binary_array(num_array,array_length,bin_array,length,OPH_IOB_CHAR)
#define oph_iob_copy_in_binary_array_l(num_array,array_length,bin_array,length) oph_iob_copy_in_binary_array(num_array,array_length,bin_array,length,OPH_IOB_LONG)
#define oph_iob_copy_in_binary_array_s(num_array,array_length,bin_array,length) oph_iob_copy_in_binary_array(num_array,array_length,bin_array,length,OPH_IOB_SHORT)
#define oph_iob_copy_in_binary_array_b(num_array,array_length,bin_array,length) oph_iob_copy_in_binary_array(num_array,array_length,bin_array,length,OPH_IOB_BYTE)
int oph_iob_copy_in_binary_array(const void* num_array, long long array_length, char** bin_array, long long *length, unsigned int num_type);


/**
 * \brief Read an array of binary values as an array of numeric (double | float | int | long) values
 * \param bin_array Array of binary values
 * \param array_length Number of elements in the array
 * \param num_array Pointer to the result array of numeric values; it reads the same memory area of the related numeric array
 * \return 0 if succes, != 0 otherwise
 */
#define oph_iob_read_as_numeric_array_i(bin_array,array_length,num_array) oph_iob_read_as_numeric_array(bin_array,array_length,num_array,OPH_IOB_INT)
#define oph_iob_read_as_numeric_array_f(bin_array,array_length,num_array) oph_iob_read_as_numeric_array(bin_array,array_length,num_array,OPH_IOB_FLOAT)
#define oph_iob_read_as_numeric_array_d(bin_array,array_length,num_array) oph_iob_read_as_numeric_array(bin_array,array_length,num_array,OPH_IOB_DOUBLE)
#define oph_iob_read_as_numeric_array_c(bin_array,array_length,num_array) oph_iob_read_as_numeric_array(bin_array,array_length,num_array,OPH_IOB_CHAR)
#define oph_iob_read_as_numeric_array_l(bin_array,array_length,num_array) oph_iob_read_as_numeric_array(bin_array,array_length,num_array,OPH_IOB_LONG)
#define oph_iob_read_as_numeric_array_s(bin_array,array_length,num_array) oph_iob_read_as_numeric_array(bin_array,array_length,num_array,OPH_IOB_SHORT)
#define oph_iob_read_as_numeric_array_b(bin_array,array_length,num_array) oph_iob_read_as_numeric_array(bin_array,array_length,num_array,OPH_IOB_BYTE)
int oph_iob_read_as_numeric_array(const char *bin_array, long long array_length, void** num_array, unsigned int num_type);


/**
 * \brief Copy an array of binary values in an array of numeric (double | float | int | long) values
 * \param bin_array Array of binary values to copy
 * \param array_length Number of elements in the array to copy
 * \param num_array Pointer to the copied array of numeric values; it may subsequently be deallocated
 * \return 0 if succes, != 0 otherwise
 */
#define oph_iob_copy_in_numeric_array_i(bin_array,array_length,num_array) oph_iob_copy_in_numeric_array(bin_array,array_length,num_array,OPH_IOB_INT)
#define oph_iob_copy_in_numeric_array_f(bin_array,array_length,num_array) oph_iob_copy_in_numeric_array(bin_array,array_length,num_array,OPH_IOB_FLOAT)
#define oph_iob_copy_in_numeric_array_d(bin_array,array_length,num_array) oph_iob_copy_in_numeric_array(bin_array,array_length,num_array,OPH_IOB_DOUBLE)
#define oph_iob_read_as_numeric_array_c(bin_array,array_length,num_array) oph_iob_read_as_numeric_array(bin_array,array_length,num_array,OPH_IOB_CHAR)
#define oph_iob_read_as_numeric_array_l(bin_array,array_length,num_array) oph_iob_read_as_numeric_array(bin_array,array_length,num_array,OPH_IOB_LONG)
#define oph_iob_read_as_numeric_array_s(bin_array,array_length,num_array) oph_iob_read_as_numeric_array(bin_array,array_length,num_array,OPH_IOB_SHORT)
#define oph_iob_read_as_numeric_array_b(bin_array,array_length,num_array) oph_iob_read_as_numeric_array(bin_array,array_length,num_array,OPH_IOB_BYTE)
int oph_iob_copy_in_numeric_array(const char *bin_array, long long array_length, void** num_array, unsigned int num_type);


/**
 * \brief Allocate a memory area for storing as binary values a predefined number of numeric (double | float | int | long) values
 * \param bin_array Pointer to the binary array to allocate; it may subsequently be deallocated
 * \param num_values Number of numeric values to store
 * \return 0 if succes, != 0 otherwise
 */
#define oph_iob_bin_array_create_i(bin_array, num_values) oph_iob_bin_array_create(bin_array,num_values,OPH_IOB_INT)
#define oph_iob_bin_array_create_f(bin_array, num_values) oph_iob_bin_array_create(bin_array,num_values,OPH_IOB_FLOAT)
#define oph_iob_bin_array_create_d(bin_array, num_values) oph_iob_bin_array_create(bin_array,num_values,OPH_IOB_DOUBLE)
#define oph_iob_bin_array_create_c(bin_array, num_values) oph_iob_bin_array_create(bin_array,num_values,OPH_IOB_CHAR)
#define oph_iob_bin_array_create_l(bin_array, num_values) oph_iob_bin_array_create(bin_array,num_values,OPH_IOB_LONG)
#define oph_iob_bin_array_create_s(bin_array, num_values) oph_iob_bin_array_create(bin_array,num_values,OPH_IOB_SHORT)
#define oph_iob_bin_array_create_b(bin_array, num_values) oph_iob_bin_array_create(bin_array,num_values,OPH_IOB_BYTE)
int oph_iob_bin_array_create(char **bin_array, long long num_values, int oph_iob_type);


/**
 * \brief Add (or replace) a binary value in the binary array
 * \param bin_array Pointer to the binary array (already allocated)
 * \param num Numeric value (double | float | int | long) to add
 * \param position Position where add/replace the binary value
 * \return 0 if succes, != 0 otherwise
 */
#define oph_iob_bin_array_add_i(bin_array,num,position) oph_iob_bin_array_add(bin_array,num,position,OPH_IOB_INT)
#define oph_iob_bin_array_add_f(bin_array,num,position) oph_iob_bin_array_add(bin_array,num,position,OPH_IOB_FLOAT)
#define oph_iob_bin_array_add_d(bin_array,num,position) oph_iob_bin_array_add(bin_array,num,position,OPH_IOB_DOUBLE)
#define oph_iob_bin_array_add_c(bin_array,num,position) oph_iob_bin_array_add(bin_array,num,position,OPH_IOB_CHAR)
#define oph_iob_bin_array_add_l(bin_array,num,position) oph_iob_bin_array_add(bin_array,num,position,OPH_IOB_LONG)
#define oph_iob_bin_array_add_s(bin_array,num,position) oph_iob_bin_array_add(bin_array,num,position,OPH_IOB_SHORT)
#define oph_iob_bin_array_add_b(bin_array,num,position) oph_iob_bin_array_add(bin_array,num,position,OPH_IOB_BYTE)
int oph_iob_bin_array_add(char *bin_array, void* num, long long position, unsigned int num_type);


/**
 * \brief Get a binary value in the binary array
 * \param bin_array Pointer to the binary array
 * \param position Position where read the binary value
 * \return 0 if succes, != 0 otherwise
 */
#define oph_iob_bin_array_get_i(bin_array,bin_val,position) oph_iob_bin_array_get(bin_array,bin_val,position,OPH_IOB_INT)
#define oph_iob_bin_array_get_f(bin_array,bin_val,position) oph_iob_bin_array_get(bin_array,bin_val,position,OPH_IOB_FLOAT)
#define oph_iob_bin_array_get_d(bin_array,bin_val,position) oph_iob_bin_array_get(bin_array,bin_val,position,OPH_IOB_DOUBLE)
#define oph_iob_bin_array_get_c(bin_array,bin_val,position) oph_iob_bin_array_get(bin_array,bin_val,position,OPH_IOB_CHAR)
#define oph_iob_bin_array_get_l(bin_array,bin_val,position) oph_iob_bin_array_get(bin_array,bin_val,position,OPH_IOB_LONG)
#define oph_iob_bin_array_get_s(bin_array,bin_val,position) oph_iob_bin_array_get(bin_array,bin_val,position,OPH_IOB_SHORT)
#define oph_iob_bin_array_get_b(bin_array,bin_val,position) oph_iob_bin_array_get(bin_array,bin_val,position,OPH_IOB_BYTE)
int oph_iob_bin_array_get(const char *bin_array, char **bin_val, long long position, unsigned int oph_iob_type);

/**
   Internal functions
 */
int oph_iob_sizeof_type(unsigned int num_type, size_t* sizeof_num);
#endif
