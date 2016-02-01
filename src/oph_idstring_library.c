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

#include "oph_idstring_library.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

#include "debug.h"

extern int msglevel;

int oph_ids_get_id_from_string(char *string, int position, int * id){

	if(!string || !id){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameters\n");		
		return OPH_IDS_ERROR;
	}

	int res;
	if (oph_ids_count_number_of_ids(string,&res)){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check ID string\n");		
		return OPH_IDS_ERROR;
	}

	if(position >= res){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Specified position is out of string range\n");		
		return OPH_IDS_ERROR;
	}

	int count = 0;
	int a,b;

	char *hyphen;
	char *semicolon;
	char *start;
	char buffer[OPH_IDS_LONGLEN], buffer2[OPH_IDS_LONGLEN];

	start = string;
	while(count <= position){
		hyphen = strchr(start,OPH_IDS_HYPHEN_CHAR);
		semicolon = strchr(start,OPH_IDS_SEMICOLON_CHAR);
		if(hyphen == NULL && semicolon == NULL){
			if(count == position){			
				a = (int)strtol(start, NULL, 10);
				if (a == 0){	
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while converting ASCII to INT\n");		
					return OPH_IDS_ERROR;
				}
				*id = a;
			}
			count++;
		}
		else if(hyphen != NULL && (semicolon == NULL || hyphen < semicolon)){
			if( (hyphen-start) >= OPH_IDS_LONGLEN){	
				pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
				return OPH_IDS_ERROR;
			}
			strncpy(buffer,start,(hyphen-start));
			buffer[(hyphen-start)] = '\0';

			if(semicolon == NULL){
				if( strlen(hyphen+1) +1 >= OPH_IDS_LONGLEN){	
					pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
					return OPH_IDS_ERROR;
				}
				strncpy(buffer2,hyphen+1,strlen(hyphen+1)+1);
				buffer[strlen(start)+1] = '\0';
			}
			else{
				if( (semicolon-hyphen) >= OPH_IDS_LONGLEN){	
					pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
					return OPH_IDS_ERROR;
				}
				strncpy(buffer2,hyphen+1,(semicolon-hyphen));
				buffer[(semicolon-hyphen)] = '\0';
			}
			a = (int)strtol(buffer, NULL, 10);
			b = (int)strtol(buffer2, NULL, 10);
			if (a == 0 || b == 0){	
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while converting ASCII to INT\n");		
				return OPH_IDS_ERROR;
			}

			if(count + (b-a+1) > position)
				*id = (a + position - count);
			count += (b-a+1);
		}
		else{
			if(count == position){			
				if( (semicolon-start) >= OPH_IDS_LONGLEN){	
					pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
					return OPH_IDS_ERROR;
				}
				strncpy(buffer,start,(semicolon-start));
				buffer[(semicolon-start)] = '\0';
				a = (int)strtol(buffer, NULL, 10);
				if (a == 0){	
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while converting ASCII to INT\n");		
					return OPH_IDS_ERROR;
				}
				*id = a;
			}
			count++;
		}
		start = semicolon;
		if(start == NULL) 
			break;
		start++;
	}
	return OPH_IDS_SUCCESS;
}

int oph_ids_get_substring_from_string(char *string, int position, int number, char **new_string){

	if(!string || !new_string || !*new_string){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameters\n");		
		return OPH_IDS_ERROR;
	}

	int res;
	if (oph_ids_count_number_of_ids(string,&res)){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to check ID string\n");		
		return OPH_IDS_ERROR;
	}

	if(position >= res || position + number > res || number < 1){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Position or number parameter are invlalid or out of string range. Position must be < %d, Number > 0 and Position + Number <= %d\n", res, res);		
		return OPH_IDS_ERROR;
	}

	int count = 0;
	int a,b;

	char *hyphen;
	char *semicolon;
	char *start;
	char buffer[OPH_IDS_LONGLEN], buffer2[OPH_IDS_LONGLEN];

	char *ptr1, *ptr2;
	ptr1 = ptr2 = NULL;
	int p1=0, p2=0;
	int p1_flag, p2_flag;
	p1_flag = p2_flag = 0;

	start = string;
	while(count < position + number){
		hyphen = strchr(start,OPH_IDS_HYPHEN_CHAR);
		semicolon = strchr(start,OPH_IDS_SEMICOLON_CHAR);
		//If ID is like ...A
		if(hyphen == NULL && semicolon == NULL){
			//If first element
			if(count == position && !p1_flag){			
				a = (int)strtol(start, NULL, 10);
				if (a == 0){	
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while converting ASCII to INT\n");		
					return OPH_IDS_ERROR;
				}
				p1 = a;
				ptr1 = semicolon;
				p1_flag = 1;
			}
			//If last element
			else if(count == position + number - 1 && !p2_flag && p1_flag){			
				a = (int)strtol(start, NULL, 10);
				if (a == 0){	
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while converting ASCII to INT\n");		
					return OPH_IDS_ERROR;
				}
				p2 = a;
				ptr2 = start;
				p2_flag = 1;
			}
			count++;
		}
		//If ID is like ...A-B
		else if(hyphen != NULL && (semicolon == NULL || hyphen < semicolon)){
			if( (hyphen-start) >= OPH_IDS_LONGLEN){	
				pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
				return OPH_IDS_ERROR;
			}
			strncpy(buffer,start,(hyphen-start));
			buffer[(hyphen-start)] = '\0';

			if(semicolon == NULL){
				if( strlen(hyphen+1)+1 >= OPH_IDS_LONGLEN){	
					pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
					return OPH_IDS_ERROR;
				}
				strncpy(buffer2,hyphen+1,strlen(hyphen+1)+1);
				buffer[strlen(start)+1] = '\0';
			}
			else{
				if( (semicolon-hyphen) >= OPH_IDS_LONGLEN){	
					pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
					return OPH_IDS_ERROR;
				}
				strncpy(buffer2,hyphen+1,(semicolon-hyphen));
				buffer[(semicolon-hyphen)] = '\0';
			}
			a = (int)strtol(buffer, NULL, 10);
			b = (int)strtol(buffer2, NULL, 10);
			if (a == 0 || b == 0){	
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while converting ASCII to INT\n");		
				return OPH_IDS_ERROR;
			}

			//If first element (not B)
			if(count + (b-a) > position && !p1_flag){
				p1 = (a + position - count);
				ptr1 = hyphen;
				p1_flag = 1;
			}
			//If first element (is B)
			else if(count + (b-a) == position && !p1_flag){
				p1 = b;
				ptr1 = semicolon;
				p1_flag = 1;
			}
			//If last element (is A)
			if(count == position + number - 1 && !p2_flag && p1_flag){
				p2 = a;
				ptr2 = start;
				p2_flag = 1;
			}
			//If last element (not A)
			else if(count + (b-a+1) > position + number - 1 && !p2_flag && p1_flag){
				p2 = (a + position + number - 1 - count);
				ptr2 = hyphen + 1;
				p2_flag = 1;
			}
			count += (b-a+1);
		}
		//If ID is like ...A;...
		else{
			//If first element
			if(count == position && !p1_flag){			
				if( (semicolon-start) >= OPH_IDS_LONGLEN){	
					pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
					return OPH_IDS_ERROR;
				}
				strncpy(buffer,start,(semicolon-start));
				buffer[(semicolon-start)] = '\0';
				a = (int)strtol(buffer, NULL, 10);
				if (a == 0){	
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while converting ASCII to INT\n");		
					return OPH_IDS_ERROR;
				}
				p1 = a;
				ptr1 = semicolon;
				p1_flag = 1;
			}
			//If last element
			else if(count == position + number - 1 && !p2_flag && p1_flag){			
				if( (semicolon-start) >= OPH_IDS_LONGLEN){	
					pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
					return OPH_IDS_ERROR;
				}
				strncpy(buffer,start,(semicolon-start));
				buffer[(semicolon-start)] = '\0';
				a = (int)strtol(buffer, NULL, 10);
				if (a == 0){	
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while converting ASCII to INT\n");		
					return OPH_IDS_ERROR;
				}
				p2 = a;
				ptr2 = start;
				p2_flag = 1;
			}
			count++;
		}
		start = semicolon;
		if(start == NULL) 
			break;
		start++;
	}

	//Set new ID string
	if(!ptr2 || !ptr1 || number == 1)
		snprintf(*new_string, (floor(log10(abs(p1))) + 1) + 1, "%d", p1);
	else{
		if( (ptr2-ptr1) >= OPH_IDS_LONGLEN){	
			pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
			return OPH_IDS_ERROR;
		}
		strncpy(buffer,ptr1,(ptr2-ptr1));
		buffer[(ptr2-ptr1)] = '\0';
		snprintf(*new_string, (floor(log10(abs(p1))) + 1) + (ptr2-ptr1) + (floor(log10(abs(p2))) + 1) + 1, "%d%s%d", p1, buffer, p2);
	}
	return OPH_IDS_SUCCESS;
}

int oph_ids_count_number_of_ids(char* string, int *res){

  char *start, *end, *hyphen;
  char buffer[OPH_IDS_LONGLEN];
  int a,b;
  int count = 0;

	if(!string || !res){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameters\n");		
		return OPH_IDS_ERROR;
	}

  start = string;
  do{
	//Fetch till ; or fetch all string
	end = strchr(start,OPH_IDS_SEMICOLON_CHAR);	
	if(end != NULL){
		if( (end-start) >= OPH_IDS_LONGLEN){	
			pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
			return OPH_IDS_ERROR;
		}
		strncpy(buffer,start,(end-start));
		buffer[(end-start)] = '\0';
	}
	else{
		if( (strlen(start)+1) >= OPH_IDS_LONGLEN){	
			pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
			return OPH_IDS_ERROR;
		}
		strncpy(buffer,start,(strlen(start)+1));
	}

	//Check if single value or range
	hyphen = strchr(buffer,OPH_IDS_HYPHEN_CHAR);
	if(hyphen == NULL){
		count++;		
	}
	else{
		*hyphen = '\0';
		a = (int)strtol(buffer, NULL, 10);
		b = (int)strtol(hyphen+1, NULL, 10);
		if (a == 0 || b == 0){	
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while converting ASCII to INT\n");		
			return OPH_IDS_ERROR;
		}
		count += b-a+1;		
	}
	start = end+1;
  }while (end != NULL);

  *res = count;
  return OPH_IDS_SUCCESS;
}

int oph_ids_remove_id_from_string(char **string, int remove_id){

	int a,b;
	int len;

	char *hyphen;
	char *semicolon;
	char *start;
	char *pattern;
	char buffer[OPH_IDS_LONGLEN], buffer2[OPH_IDS_LONGLEN];
	char *tmp1, *tmp2;
	tmp1 = tmp2 = NULL;

	if(*string == NULL){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameters\n");		
		return OPH_IDS_ERROR;
	}

	start = *string;
	while(start){
		tmp1 = tmp2 = NULL;
		hyphen = strchr(start,OPH_IDS_HYPHEN_CHAR);
		semicolon = strchr(start,OPH_IDS_SEMICOLON_CHAR);
		if(hyphen == NULL && semicolon == NULL){			
			a = (int)strtol(start, NULL, 10);
			if (a == 0){	
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while converting ASCII to INT\n");		
				return OPH_IDS_ERROR;
			}
			if(remove_id == a){
				if(*(start-1)== OPH_IDS_SEMICOLON_CHAR ) *(start -1) = '\0';				
				else *start = '\0';
				break;
			}
		}
		else if(hyphen != NULL && (semicolon == NULL || hyphen < semicolon)){
			if( (hyphen-start) >= OPH_IDS_LONGLEN){	
				pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
				return OPH_IDS_ERROR;
			}
			strncpy(buffer,start,(hyphen-start));
			buffer[(hyphen-start)] = '\0';

			if(semicolon == NULL){
				if( strlen(hyphen+1)+1 >= OPH_IDS_LONGLEN){	
					pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
					return OPH_IDS_ERROR;
				}
				strncpy(buffer2,hyphen+1,strlen(hyphen+1)+1);
				buffer[strlen(start)+1] = '\0';
			}
			else{
				if( (semicolon-hyphen) >= OPH_IDS_LONGLEN){	
					pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
					return OPH_IDS_ERROR;
				}
				strncpy(buffer2,hyphen+1,(semicolon-hyphen));
				buffer[(semicolon-hyphen)] = '\0';
			}
			a = (int)strtol(buffer, NULL, 10);
			b = (int)strtol(buffer2, NULL, 10);
			if (a == 0 || b == 0){	
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while converting ASCII to INT\n");		
				return OPH_IDS_ERROR;
			}

			if(remove_id >= a && remove_id <= b){
				if(remove_id == a ){	
					if(b - remove_id < 3)
						pattern = OPH_IDS_PATTERN1;
					else
						pattern = OPH_IDS_PATTERN2;
					tmp2 = (char*)calloc(strlen(hyphen+1)+1,sizeof(char));
					if(!tmp2){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocating error\n");		
						return OPH_IDS_ERROR;
					}
					if( strlen(hyphen+1)+1 >= OPH_IDS_LONGLEN){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
						return OPH_IDS_ERROR;
					}
					strncpy(tmp2,hyphen+1,strlen(hyphen+1)+1);
					tmp1 = (char*)calloc((strlen(*string) - strlen(start)+1),sizeof(char));
					if(!tmp1){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocating error\n");		
						free(tmp2);
						return OPH_IDS_ERROR;
					}					
					if( (strlen(*string) - strlen(start))+1 >= OPH_IDS_LONGLEN){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
						return OPH_IDS_ERROR;
					}
					strncpy(tmp1,*string,(strlen(*string) - strlen(start))+1);
					tmp1[(strlen(*string) - strlen(start))] = '\0';
					len = (strlen(*string) - strlen(start)) + strlen(hyphen) + (floor(log10(abs(remove_id+1))) + 1) +1;
					*string = (char*)realloc(*string,len*sizeof(char));
					if(*string == NULL){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocating error\n");		
						free(tmp2);
						free(tmp1);
						return OPH_IDS_ERROR;
					}
					snprintf(*string, len, pattern, tmp1, remove_id+1,tmp2);		
					free(tmp1);
					free(tmp2);
					break;
				}
				else if(remove_id == a + 1){
					if(b - remove_id < 3)
						pattern = OPH_IDS_PATTERN3;
					else
						pattern = OPH_IDS_PATTERN4;
					tmp2 = (char*)calloc(strlen(hyphen+1)+1,sizeof(char));
					if(!tmp2){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocating error\n");		
						return OPH_IDS_ERROR;
					}
					if( strlen(hyphen+1)+1 >= OPH_IDS_LONGLEN){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
						return OPH_IDS_ERROR;
					}
					strncpy(tmp2,hyphen+1,strlen(hyphen+1)+1);
					tmp1 = (char*)calloc((strlen(*string) - strlen(hyphen)+1),sizeof(char));
					if(!tmp1){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocating error\n");		
						free(tmp2);
						return OPH_IDS_ERROR;
					}		
					if( (strlen(*string) - strlen(hyphen))+1 >= OPH_IDS_LONGLEN){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
						return OPH_IDS_ERROR;
					}
					strncpy(tmp1,*string,(strlen(*string) - strlen(hyphen))+1);
					tmp1[(strlen(*string) - strlen(hyphen))] = '\0';
					len = (strlen(*string) - strlen(hyphen+1)) + strlen(hyphen) + (floor(log10(abs(remove_id+1))) + 1) +1;
					*string = (char*)realloc(*string,len*sizeof(char));
					if(*string == NULL){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocating error\n");		
						free(tmp2);
						free(tmp1);			
						return OPH_IDS_ERROR;
					}
					if( remove_id == b -1 ){
						pattern = OPH_IDS_PATTERN5;
						snprintf(*string, len, pattern, tmp1, tmp2);		
					}
					else
						snprintf(*string, len, pattern, tmp1, remove_id+1,tmp2);		
					free(tmp1);
					free(tmp2);
					break;
				}
				else if(remove_id == b){
					if(remove_id - a < 3)
						pattern = OPH_IDS_PATTERN6;		
					else
						pattern = OPH_IDS_PATTERN7;
					tmp2 = (char*)calloc((strlen(hyphen+1) - (floor(log10(abs(remove_id))) + 1) )+1,sizeof(char));
					if(!tmp2){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocating error\n");		
						return OPH_IDS_ERROR;
					}
					if( (strlen(hyphen+1) - (floor(log10(abs(remove_id))) + 1) )+1 >= OPH_IDS_LONGLEN){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
						return OPH_IDS_ERROR;
					}
					strncpy(tmp2,(hyphen+1+(int)floor(log10(abs(remove_id)))+1),(strlen(hyphen+1) - (floor(log10(abs(remove_id))) + 1) )+1);
					tmp1 = (char*)calloc((strlen(*string) - strlen(hyphen))+1,sizeof(char));
					if(!tmp1){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocating error\n");		
						free(tmp2);
						return OPH_IDS_ERROR;
					}		
					if( (strlen(*string) - strlen(hyphen))+1 >= OPH_IDS_LONGLEN){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
						return OPH_IDS_ERROR;
					}
					strncpy(tmp1,*string,(strlen(*string) - strlen(hyphen))+1);
					tmp1[(strlen(*string) - strlen(hyphen))] = '\0';
					len = (strlen(*string) - strlen(hyphen)) + (strlen(hyphen) - (floor(log10(abs(remove_id))) + 1) ) + (floor(log10(abs(remove_id-1))) + 1) +1;
					*string = (char*)realloc(*string,len*sizeof(char));
					if(*string == NULL){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocating error\n");		
						free(tmp2);
						free(tmp1);
						return OPH_IDS_ERROR;
					}
					snprintf(*string, len, pattern, tmp1, remove_id-1,tmp2);		
					free(tmp1);
					free(tmp2);
					break;
				}
				else if(remove_id == b - 1){
					if(remove_id - a < 3)
						pattern = OPH_IDS_PATTERN3;
					else
						pattern = OPH_IDS_PATTERN8;		
					tmp2 = (char*)calloc(strlen(hyphen+1)+1,sizeof(char));
					if(!tmp2){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocating error\n");		
						return OPH_IDS_ERROR;
					}
					if( strlen(hyphen+1)+1 >= OPH_IDS_LONGLEN){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
						return OPH_IDS_ERROR;
					}
					strncpy(tmp2,hyphen+1,strlen(hyphen+1)+1);
					tmp1 = (char*)calloc((strlen(*string) - strlen(hyphen))+1,sizeof(char));
					if(!tmp1){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocating error\n");		
						free(tmp2);
						return OPH_IDS_ERROR;
					}		
					if( (strlen(*string) - strlen(hyphen))+1 >= OPH_IDS_LONGLEN){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
						return OPH_IDS_ERROR;
					}
					strncpy(tmp1,*string,(strlen(*string) - strlen(hyphen))+1);
					tmp1[(strlen(*string) - strlen(hyphen))] = '\0';
					len = (strlen(*string) - strlen(hyphen+1)) + strlen(hyphen) + (floor(log10(abs(remove_id-1))) + 1) +1;
					*string = (char*)realloc(*string,len*sizeof(char));
					if(*string == NULL){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocating error\n");		
						free(tmp2);
						free(tmp1);
						return OPH_IDS_ERROR;
					}
					snprintf(*string, len, pattern, tmp1, remove_id-1,tmp2);		
					free(tmp1);
					free(tmp2);
					break;
				}
				else{
					if(remove_id - a < 3 && b - remove_id < 3)
						pattern = OPH_IDS_PATTERN9;
					else if(b - remove_id < 3)
						pattern = OPH_IDS_PATTERN10;
					else if(remove_id - a < 3)
						pattern = OPH_IDS_PATTERN11;
					else
						pattern = OPH_IDS_PATTERN12;
		
					tmp2 = (char*)calloc(strlen(hyphen+1)+1,sizeof(char));
					if(!tmp2){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocating error\n");		
						return OPH_IDS_ERROR;
					}
					if( strlen(hyphen+1)+1 >= OPH_IDS_LONGLEN){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
						return OPH_IDS_ERROR;
					}
					strncpy(tmp2,hyphen+1,strlen(hyphen+1)+1);
					tmp1 = (char*)calloc((strlen(*string) - strlen(hyphen))+1,sizeof(char));
					if(!tmp1){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocating error\n");		
						free(tmp2);
						return OPH_IDS_ERROR;
					}		
					if( (strlen(*string) - strlen(hyphen))+1 >= OPH_IDS_LONGLEN){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
						return OPH_IDS_ERROR;
					}
					strncpy(tmp1,*string,(strlen(*string) - strlen(hyphen))+1);
					tmp1[(strlen(*string) - strlen(hyphen))] = '\0';
					len = (strlen(*string) - strlen(hyphen+1)) + strlen(hyphen) + (floor(log10(abs(remove_id+1))) + 1) + 1 + (floor(log10(abs(remove_id-1))) + 1) + 1;
					*string = (char*)realloc(*string,len*sizeof(char));
					if(*string == NULL){	
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory allocating error\n");		
						free(tmp2);
						free(tmp1);			
						return OPH_IDS_ERROR;
					}
					snprintf(*string, len, pattern, tmp1, remove_id-1, remove_id+1,tmp2);		
					free(tmp1);
					free(tmp2);
					break;
				}
			}
		}
		else{
			if( (semicolon-start) >= OPH_IDS_LONGLEN){	
				pmesg(LOG_ERROR, __FILE__, __LINE__, "String to be copied is too long. Max length is :%d\n",OPH_IDS_LONGLEN);		
				return OPH_IDS_ERROR;
			}
			strncpy(buffer,start,(semicolon-start));
			buffer[(semicolon-start)] = '\0';
			a = (int)strtol(buffer, NULL, 10);
			if (a == 0){	
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while converting ASCII to INT\n");		
				return OPH_IDS_ERROR;
			}
			if(remove_id == a){
				memmove(start,(semicolon+1),strlen(semicolon+1)+1);				
				break;
			}
		}
		start = semicolon;
		if(start != NULL) 
			start++;
	}
	return OPH_IDS_SUCCESS;
}


int oph_ids_create_new_id_string(char** string, int string_len, int first_id, int last_id){
	if(first_id > last_id){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "First ID is bigger than Last ID\n");		
		return OPH_IDS_ERROR;
	}

	if(*string == NULL){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameters\n");		
		return OPH_IDS_ERROR;
	}
	
	if(first_id == last_id || last_id == 0)
		snprintf(*string, string_len, "%d",first_id);
	else 
		snprintf(*string, string_len, "%d-%d",first_id,last_id);

	return OPH_IDS_SUCCESS;
}
