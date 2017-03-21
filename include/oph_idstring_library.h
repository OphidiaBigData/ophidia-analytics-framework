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

#ifndef __OPH_IDSTRING_H
#define __OPH_IDSTRING_H

#define OPH_IDS_LONGLEN		2000

#define OPH_IDS_SUCCESS		0
#define OPH_IDS_ERROR		1

#define OPH_IDS_PATTERN1	"%s%ld;%s"
#define OPH_IDS_PATTERN2	"%s%ld-%s"
#define OPH_IDS_PATTERN3	"%s;%ld;%s"
#define OPH_IDS_PATTERN4	"%s;%ld-%s"
#define OPH_IDS_PATTERN5	"%s;%s";
#define OPH_IDS_PATTERN6	"%s;%ld%s"
#define OPH_IDS_PATTERN7	"%s-%ld%s"
#define OPH_IDS_PATTERN8	"%s-%ld;%s"
#define OPH_IDS_PATTERN9	"%s;%ld;%ld;%s"
#define OPH_IDS_PATTERN10	"%s-%ld;%ld;%s"
#define OPH_IDS_PATTERN11	"%s;%ld;%ld-%s"
#define OPH_IDS_PATTERN12	"%s-%ld;%ld-%s"

#define OPH_IDS_HYPHEN_CHAR	'-'
#define OPH_IDS_SEMICOLON_CHAR	';'

//Retreive ID in string at given position 
int oph_ids_get_id_from_string(char *string, int position, int *id);

//Retreive substring of number IDs starting at given position. new_string is the pointer to the new id string
int oph_ids_get_substring_from_string(char *string, int position, int number, char **new_string);

//Count number of ID in string
int oph_ids_count_number_of_ids(char *string, int *res);

//Remove given ID form string 
int oph_ids_remove_id_from_string(char **string, int removed_id);

//Write ID string specifying first and last id
int oph_ids_create_new_id_string(char **string, int string_len, int first_id, int last_id);

#endif				//__OPH_IDSTRING_H
