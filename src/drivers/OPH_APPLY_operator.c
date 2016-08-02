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

#include "drivers/OPH_APPLY_operator.h"

#include <errmsg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

#include <libxml/parser.h>
#include <libxml/valid.h>
#include <libxml/xpath.h>

#include "oph_analytics_operator_library.h"

#include "oph_idstring_library.h"
#include "oph_task_parser_library.h"
#include "oph_dimension_library.h"
#include "oph_pid_library.h"
#include "oph_json_library.h"

#include "debug.h"

#include "oph_input_parameters.h"
#include "oph_log_error_codes.h"
#include "oph_datacube2_library.h"

#define OPH_APPLY_BLACK_LIST {"oph_compress","oph_uncompress"}
#define OPH_APPLY_BLACK_LIST_SIZE 2

#define OPH_APPLY_COMPRESS_MEASURE "oph_compress('','',%s)"
#define OPH_APPLY_UNCOMPRESS_MEASURE "oph_uncompress('','',%s)"

#define OPH_APPLY_PRIMITIVE_ID_STR "id"
#define OPH_APPLY_PRIMITIVE_SIMPLE_STR "simple"
#define OPH_APPLY_PRIMITIVE_AGGREGATE_STR "aggregate"
#define OPH_APPLY_PRIMITIVE_REDUCE_STR "reduce"
#define OPH_APPLY_PRIMITIVE_TOTAL_STR "apex"
#define OPH_APPLY_PRIMITIVE_RETURN_STR "binary-array"

#define OPH_APPLY_DATATYPE_PREFIX "oph_"

// XPath
#define OPH_APPLY_XPATH_RETURN "/primitive/info/return"
#define OPH_APPLY_XPATH_OPERATION "/primitive/info/operation"

// Special chars
#define OPH_APPLY_CHAR_BRAKE_OPEN '('
#define OPH_APPLY_CHAR_BRAKE_CLOSE ')'
#define OPH_APPLY_CHAR_COMMA ','
#define OPH_APPLY_CHAR_APOS '\''
#define OPH_APPLY_CHAR_QUOT '\"'
#define OPH_APPLY_CHAR_SPACE ' '

typedef enum { OPH_APPLY_PRIMITIVE_UNKNOWN, OPH_APPLY_PRIMITIVE_ID, OPH_APPLY_PRIMITIVE_SIMPLE, OPH_APPLY_PRIMITIVE_AGGREGATE, OPH_APPLY_PRIMITIVE_REDUCE, OPH_APPLY_PRIMITIVE_TOTAL } oph_apply_operation_type;

typedef struct
{
	char* name;
	char** param;
	int size;
	oph_apply_operation_type type;
	short return_binary;
	char* input_datatype;
	char* output_datatype;
	int check_datatype;
	int next;
} oph_apply_primitive;

typedef struct
{
	oph_apply_primitive* primitive;
	int size;
} oph_apply_primitives;

int oph_apply_primitives_init(oph_apply_primitives **p, char** name, int size)
{
	if (!p || !name || (size<1)) return 1;
	*p = (oph_apply_primitives*)malloc(sizeof(oph_apply_primitives));
	if (!(*p)) return 2;
	(*p)->primitive = (oph_apply_primitive*)malloc(size*sizeof(oph_apply_primitive));
	if (!((*p)->primitive))
	{
		free(*p);
		return 3;
	}
	(*p)->size = size;
	int i,j;
	for (i=0;i<size;++i)
	{
		(*p)->primitive[i].name = strdup(name[i]);
		if (!((*p)->primitive[i].name))
		{
			for (j=0;j<i;++j) if ((*p)->primitive[i].name) free((*p)->primitive[i].name);
			free(*p);
			return 4;
		}
		(*p)->primitive[i].param = NULL;
		(*p)->primitive[i].size = 0;
		(*p)->primitive[i].type = OPH_APPLY_PRIMITIVE_SIMPLE;
		(*p)->primitive[i].return_binary = 1; // By defualt the primitive return a binary array
		(*p)->primitive[i].input_datatype = NULL;
		(*p)->primitive[i].output_datatype = NULL;
		(*p)->primitive[i].check_datatype = 0; // Force check in case MYSQL_FRAG_MEASURE is a param
		(*p)->primitive[i].next = -1;
	}
	return 0;
}

int oph_apply_primitive_free(oph_apply_primitive *p)
{
	if (!p) return 1;
	if (p->name) free(p->name);
	if (p->param)
	{
		int i;
		for (i=0;i<p->size;++i) if (p->param[i]) free(p->param[i]);
		free(p->param);
	}
	if (p->input_datatype) free(p->input_datatype);
	if (p->output_datatype) free(p->output_datatype);
	return 0;
}

int oph_apply_primitives_free(oph_apply_primitives **p)
{
	if (!p) return 1;
	if (!(*p)) return 0;
	if ((*p)->primitive)
	{
		int i;
		for (i=0;i<(*p)->size;++i) oph_apply_primitive_free(&((*p)->primitive[i]));
		free((*p)->primitive);
	}
	free(*p);
	return 0;
}

int oph_apply_primitives_load(oph_apply_primitives *p, int id, const char* query, int next, const char* target, const char* subtarget)
{
	if (!p || !query) return 1;
	if ((id<0) || (id>=p->size)) return 1;
	char *spointer = strstr(query,p->primitive[id].name), *epointer;
	if (!spointer) return 2; // Not found
	spointer += strlen(p->primitive[id].name);
	if (*spointer!=OPH_APPLY_CHAR_BRAKE_OPEN)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unexpected brake\n");
		return 3;
	}
	int bcount=1,nparam=1,ll=0;
	char special_char=0;
	char squery[strlen(query)];
	for (epointer=1+spointer; *epointer && bcount; epointer++)
	{
		if (special_char)
		{
			if (*epointer==special_char) special_char=0;
			if (bcount==1) continue;
		}
		else if ((*epointer==OPH_APPLY_CHAR_APOS) || (*epointer==OPH_APPLY_CHAR_QUOT))
		{
			special_char=*epointer;
			if (bcount==1) continue;
		}
		else if (*epointer==OPH_APPLY_CHAR_BRAKE_OPEN) bcount++;
		else if (*epointer==OPH_APPLY_CHAR_BRAKE_CLOSE) bcount--;
		else if ((*epointer==OPH_APPLY_CHAR_COMMA) && (bcount==1)) nparam++;
		squery[ll++]=*epointer;
	}
	if (bcount || special_char)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Brake number or the use of special chars are not correct\n");
		return 4;
	}
	squery[ll]=0;
	p->primitive[id].param = (char**)malloc(nparam*sizeof(char*));
	p->primitive[id].size = nparam;
	p->primitive[id].next = next;

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Searching primitives in \"%s\"\n",squery);

	int nid,res;
	for (nid=id+1;nid<p->size;++nid)
		if (!(p->primitive[nid].param))
		{
			res=oph_apply_primitives_load(p,nid,squery,id,target,subtarget);
			if (res>2) return res;
		}

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Number of params of '%s' is %d\n",p->primitive[id].name,nparam);

	int i=0,copy_up,length=strlen(OPH_APPLY_DATATYPE_PREFIX);
	char *mspointer=1+spointer,*mepointer=NULL;
	bcount=0;
	for (epointer=1+spointer; *epointer && (i<nparam); epointer++)
	{
		copy_up=0;
		if (special_char)
		{
			if (*epointer==special_char) special_char=0;
		}
		else if ((*epointer==OPH_APPLY_CHAR_APOS) || (*epointer==OPH_APPLY_CHAR_QUOT)) special_char=*epointer;
		else if (*epointer==OPH_APPLY_CHAR_BRAKE_OPEN)
		{
			if (!bcount) mepointer=epointer;
			bcount++;
		}
		else if (*epointer==OPH_APPLY_CHAR_BRAKE_CLOSE)
		{
			if (bcount) bcount--;
			else copy_up=1;
		}
		else if ((*epointer==OPH_APPLY_CHAR_COMMA) && !bcount) copy_up=1;
		if (copy_up)
		{
			if (!mepointer) mepointer=epointer;
			p->primitive[id].param[i] = strndup(mspointer,mepointer-mspointer);
			mspointer=epointer+1;
			mepointer=NULL;

			if (i<=1) // Data types
			{
				int j,k=0;
				char *pbuff = p->primitive[id].param[i], buff[1+strlen(pbuff)];
				for (j=0;j<(int)strlen(pbuff);++j) if (pbuff[j]!=OPH_APPLY_CHAR_APOS && pbuff[j]!=OPH_APPLY_CHAR_SPACE) buff[k++]=pbuff[j];
				buff[k]=0;
				if (!strncasecmp(buff,OPH_APPLY_DATATYPE_PREFIX,length))
				{
					pmesg(LOG_DEBUG, __FILE__, __LINE__, "Found data type '%s'\n",buff);
					if (!i) p->primitive[id].input_datatype = strdup(buff+length);
					else p->primitive[id].output_datatype = strdup(buff+length);
				}
			}
			if (target && !strcasecmp(p->primitive[id].param[i],target)) p->primitive[id].check_datatype = 1; // Found argument MYSQL_FRAG_MEASURE
			else if (subtarget && !strcasecmp(p->primitive[id].param[i],subtarget)) p->primitive[id].check_datatype = 1; // Found argument MYSQL_DIMENSION
			pmesg(LOG_DEBUG, __FILE__, __LINE__, "\t%d: %s\n",i,p->primitive[id].param[i]);
			i++;
		}
	}
	if (i<nparam)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unexpected number of arguments for primitive '%s'\n",p->primitive[id].name);
		return 5;
	}
	return 0;
}

int _oph_apply_check_datatype(oph_apply_primitives *p, int id, char* measure_type)
{
	int i, found=0, res;
	if (!p->primitive[id].return_binary) // Non-binary arrays are not supported
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Primitive '%s' is not supported\n", p->primitive[id].name);
		return 1;
	}
	if (!p->primitive[id].input_datatype || !p->primitive[id].output_datatype)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Data types of primitive '%s' are not set correctly\n", p->primitive[id].name);
		return 2;
	}
	for (i=id+1;i<p->size;++i) if ((p->primitive[i].return_binary) && (p->primitive[i].next==id))
	{
		if (!p->primitive[i].output_datatype)
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Data types of '%s' are not set\n", p->primitive[i].name);
			return 2;
		}
		if (strcasecmp(p->primitive[id].input_datatype,p->primitive[i].output_datatype))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Input data type of primitive '%s' and output data type of primitive '%s' are different\n", p->primitive[id].name, p->primitive[i].name);
			return 3;
		}
		if ((res = _oph_apply_check_datatype(p, i, measure_type))) return res;
		found=1;
	}
	if ((!found || p->primitive[id].check_datatype) && strcasecmp(p->primitive[id].input_datatype,measure_type)) // Leaf
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Input data type of primitive '%s' is not correct\n", p->primitive[id].name);
		return 3;
	}
	return 0;
}

int oph_apply_check_datatype(oph_apply_primitives *p, char* measure_type)
{
	if (!p || !measure_type)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null pointer\n");
		return 1;
	}
	if (!p->size || !p->primitive) return 0;

	if (_oph_apply_check_datatype(p,0,measure_type)) return 2;
	snprintf(measure_type,OPH_ODB_CUBE_MEASURE_TYPE_SIZE,"%s",p->primitive[0].output_datatype);

	return 0;
}

oph_apply_operation_type _oph_apply_evaluate_query_type(oph_apply_primitives *p, int id)
{
	int i,rf=0,af=0,tf=0;
	oph_apply_operation_type type;
	if (!p->primitive[id].return_binary) return OPH_APPLY_PRIMITIVE_UNKNOWN; // Non-binary arrays are not supported
	if (p->primitive[id].type == OPH_APPLY_PRIMITIVE_TOTAL) return OPH_APPLY_PRIMITIVE_TOTAL;
	for (i=id+1;i<p->size;++i) if (p->primitive[i].next==id)
	{
		type = _oph_apply_evaluate_query_type(p,i);
		switch(type)
		{
			case OPH_APPLY_PRIMITIVE_UNKNOWN:
			case OPH_APPLY_PRIMITIVE_ID:
				break; // it will be excluded
			case OPH_APPLY_PRIMITIVE_SIMPLE: return p->primitive[id].type;
			case OPH_APPLY_PRIMITIVE_AGGREGATE: af=1; break;
			case OPH_APPLY_PRIMITIVE_REDUCE: rf=1; break;
			case OPH_APPLY_PRIMITIVE_TOTAL: tf=1; break;
		}
	}
/*
SIMPLE	rf	af	tf	IN	OUT
x	x	x	x	TOTAL	TOTAL
1	x	x	x	x	IN
0	0	0	0	x	IN
0	0	0	1	x	TOTAL
0	1	0	x	SIMPLE	REDUCE
0	1	0	x	REDUCE	REDUCE
0	1	0	x	AGGREGA	TOTAL
0	0	1	x	SIMPLE	AGGREGATE
0	0	1	x	REDUCE	TOTAL
0	0	1	x	AGGREGA	AGGREGATE
0	1	1	x	x	TOTAL
*/
	if (rf)
	{
		if (af || (p->primitive[id].type == OPH_APPLY_PRIMITIVE_AGGREGATE)) return OPH_APPLY_PRIMITIVE_TOTAL;
		else return OPH_APPLY_PRIMITIVE_REDUCE;
	}
	else if (af)
	{
		if (p->primitive[id].type == OPH_APPLY_PRIMITIVE_REDUCE) return OPH_APPLY_PRIMITIVE_TOTAL;
		else return OPH_APPLY_PRIMITIVE_AGGREGATE;
	}
	else if (tf) return OPH_APPLY_PRIMITIVE_TOTAL;
	else return p->primitive[id].type;
}

int oph_apply_evaluate_query_type(oph_apply_primitives *p, oph_apply_operation_type *type)
{
	if (!p || !type) return 1;
	*type = _oph_apply_evaluate_query_type(p,0);
	return 0;
}

int open_xml_doc(const char* filename, xmlDocPtr *document)
{
	if (!document)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null pointer\n");
		return 1;
	}

	//Open document
	*document = xmlParseFile(filename);
	if (!(*document))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open XML document %s\n", filename);
		return 2;
	}

	//Validate XML document
	xmlValidCtxtPtr ctxt; 
	ctxt = xmlNewValidCtxt();
	if (ctxt == NULL)
	{
		xmlFreeDoc(*document);
		return 3;
	}

	//Parse the DTD file
	xmlDtdPtr dtd = xmlParseDTD(NULL, (xmlChar * )OPH_FRAMEWORK_PRIMITIVE_DTD_PATH);
	if (dtd == NULL)
	{
		xmlFreeDoc(*document);
		xmlFreeValidCtxt(ctxt);
		return 4;
	}

	//Validate document
	if (!xmlValidateDtd(ctxt, *document, dtd))
	{
		xmlFreeDoc(*document);
		xmlFreeValidCtxt(ctxt);
		xmlFreeDtd(dtd);
		return 5;
	}
	xmlFreeDtd(dtd);
	xmlFreeValidCtxt(ctxt);

	return 0;
}

int close_xml_doc(xmlDocPtr document)
{
	if (!document)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null pointer\n");
		return 1;
	}

	// Close document
	xmlFreeDoc(document);

	return 0;
}

int oph_apply_parse_query(oph_operator_struct *handle, char* data_type, const char* target, const char* subtarget, int* use_subtarget)
{
	if (use_subtarget) *use_subtarget=0;

	int is_measure = strcasecmp(target,MYSQL_DIMENSION);
	char* array_operation = is_measure ? ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_operation : ((OPH_APPLY_operator_handle*)handle->operator_handle)->dimension_operation;

	// Pre-parsing: check for clause 'target'
	if (is_measure && !strcasestr(array_operation,target))
	{
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Keyword '%s' is not given in query\n", target);
		logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, "Keyword '%s' is not given in query\n", target );
	}

	// Pre-parsing: check for clause 'subtarget'
	if (subtarget && strcasestr(array_operation,subtarget) && use_subtarget) *use_subtarget=1;

	int i=0;
	if ((is_measure && ((OPH_APPLY_operator_handle*)handle->operator_handle)->set_measure_type) || (!is_measure && ((OPH_APPLY_operator_handle*)handle->operator_handle)->set_dimension_type))
	{
		char tmp[OPH_TP_TASKLEN], *pch=array_operation, special_char=0, write_char;
		while (pch && *pch && (i<OPH_TP_TASKLEN))
		{
			write_char=1;
			if (special_char)
			{
				if (*pch==special_char) special_char=0;
			}
			else if ((*pch==OPH_APPLY_CHAR_APOS) || (*pch==OPH_APPLY_CHAR_QUOT)) special_char=*pch;
			else if (*pch==OPH_APPLY_CHAR_BRAKE_OPEN)
			{
				i+=snprintf(tmp+i,OPH_TP_TASKLEN-i,"('%s%s','%s%s',",OPH_APPLY_DATATYPE_PREFIX,data_type,OPH_APPLY_DATATYPE_PREFIX,data_type);
				write_char=0;
			}
			if (write_char) tmp[i++]=*pch;
			pch++;
		}
		tmp[i]=0;
		free(array_operation);
		array_operation = strdup(tmp);
		if (is_measure) ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_operation = array_operation;
		else ((OPH_APPLY_operator_handle*)handle->operator_handle)->dimension_operation = array_operation;
	}

	// Pre-parsing: check for brackets, spaces and special chars
	int j, l=strlen(array_operation), bcount=0, pcount=0, comma_up=0, scount=0;
	char array_operation2[l+1];
	char special_char_stack[2];
	special_char_stack[0] = special_char_stack[1] = 0;
	for (i=0,j=0;i<l;++i)
	{
		if ((array_operation[i]==OPH_APPLY_CHAR_APOS) || (array_operation[i]==OPH_APPLY_CHAR_QUOT))
		{
			comma_up=0;
			switch (scount)
			{
				case 0:
					special_char_stack[scount++]=array_operation[i];
				break;
				case 1:
					if (special_char_stack[0]==array_operation[i]) scount=0;
					else special_char_stack[scount++]=array_operation[i];
				break;
				case 2:
					if (special_char_stack[1]==array_operation[i]) scount=1;
					else scount++;
				break;
			}
			if (scount>2) break;
		}
		else if (!scount)
		{
			if (array_operation[i]==OPH_APPLY_CHAR_SPACE) continue;
			else if (array_operation[i]==OPH_APPLY_CHAR_COMMA)
			{
				if (!bcount || comma_up) break;
				comma_up=1;
			}
			else
			{
				if (array_operation[i]==OPH_APPLY_CHAR_BRAKE_OPEN)
				{
					bcount++;
					pcount++;
				}
				else if (array_operation[i]==OPH_APPLY_CHAR_BRAKE_CLOSE)
				{
					bcount--;
					if (bcount<0) break;
				}
				comma_up=0;
			}
		}
		array_operation2[j++] = array_operation[i];
	}

	if (bcount || (i<l) || comma_up || scount)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong parameter %s: brackets, commas or special chars are not corrected\n", OPH_IN_PARAM_APPLY_QUERY);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_INVALID_INPUT_STRING );
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	array_operation2[l=j]=0;

	pmesg(LOG_INFO, __FILE__, __LINE__, "%d primitive%s detected\n",pcount,pcount!=1?"s":"");
	if (pcount)
	{
		char array_operation3[l+1];
		strcpy(array_operation3,array_operation2);

		int ll;
		char* primitive[pcount];
		char *saveptr = NULL, *pch = strtok_r(array_operation3,"(,",&saveptr);
		for(j=0; pch && (j<pcount); pch=strtok_r(NULL,"(,",&saveptr))
			if ((ll=strlen(pch)) && (array_operation2[pch+ll-array_operation3]==OPH_APPLY_CHAR_BRAKE_OPEN)) primitive[j++]=pch; // ll=strlen(pch) is OK
		if (!pch || (j<pcount))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong parameter %s: primitive check failed\n", OPH_IN_PARAM_APPLY_QUERY);
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_INVALID_INPUT_STRING );
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		// Extract primitive parameters
		oph_apply_primitives *p = NULL;
		if (oph_apply_primitives_init(&p, primitive, pcount))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_MEMORY_ERROR_INPUT, "primitives");
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_apply_primitives_load(p,0,array_operation2,-1,target,subtarget))
		{
			oph_apply_primitives_free(&p);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong parameter %s: primitive parsing failed\n", OPH_IN_PARAM_APPLY_QUERY);
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_INVALID_INPUT_STRING );
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		char xml[OPH_TP_BUFLEN];
		char filename[2*OPH_TP_BUFLEN];

		xmlDocPtr document;
		xmlXPathContextPtr context;
		xmlXPathObjectPtr result;
		xmlNodeSetPtr nodeset;
		xmlChar* keyword;

		char* black_list[OPH_APPLY_BLACK_LIST_SIZE] = OPH_APPLY_BLACK_LIST;

		for (j=0; j<pcount; ++j)
		{
			for (i=0;i<OPH_APPLY_BLACK_LIST_SIZE;++i) if (!strcmp(primitive[j],black_list[i]))
			{
				oph_apply_primitives_free(&p);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong parameter %s: this primitive is reserved\n", primitive[j]);
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_INVALID_INPUT_STRING );
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}

			if (oph_tp_retrieve_function_xml_file(primitive[j], NULL, OPH_TP_XML_PRIMITIVE_TYPE_CODE, &xml))
			{
				oph_apply_primitives_free(&p);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong parameter %s: unable to find xml file\n", primitive[j]);
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_INVALID_INPUT_STRING );
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}

			snprintf(filename, 2*OPH_TP_BUFLEN, OPH_FRAMEWORK_PRIMITIVE_XML_FILE_PATH_DESC, OPH_ANALYTICS_LOCATION, xml);
			if (open_xml_doc(filename, &document))
			{
				oph_apply_primitives_free(&p);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong parameter %s: unable to open xml file\n", primitive[j]);
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_INVALID_INPUT_STRING );
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}

			pmesg(LOG_DEBUG, __FILE__, __LINE__, "Loaded xml file for '%s'\n", primitive[j]);

			context = xmlXPathNewContext(document);
			result = xmlXPathEvalExpression((const xmlChar *)OPH_APPLY_XPATH_RETURN, context);
			if (!xmlXPathNodeSetIsEmpty(nodeset = result->nodesetval)) // By default a primitive return a "binary-array"
			{
				if (nodeset->nodeNr > 1)
				{
					xmlXPathFreeObject(result);
					xmlXPathFreeContext(context);
					close_xml_doc(document);
					oph_apply_primitives_free(&p);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong parameter %s: xml file is not valid\n", primitive[j]);
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_INVALID_INPUT_STRING );
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				keyword = xmlGetProp(nodeset->nodeTab[0], (const xmlChar *)"type");
				if (keyword) p->primitive[j].return_binary = !strcasecmp((const char *)keyword,OPH_APPLY_PRIMITIVE_RETURN_STR);
				xmlFree(keyword);
			}
			xmlXPathFreeObject(result);
			result = xmlXPathEvalExpression((const xmlChar *)OPH_APPLY_XPATH_OPERATION, context);
			if (!xmlXPathNodeSetIsEmpty(nodeset = result->nodesetval)) // By default the operation of a primitive is "simple"
			{
				if (nodeset->nodeNr > 1)
				{
					xmlXPathFreeObject(result);
					xmlXPathFreeContext(context);
					close_xml_doc(document);
					oph_apply_primitives_free(&p);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong parameter %s: xml file is not valid\n", primitive[j]);
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_INVALID_INPUT_STRING );
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				keyword = xmlGetProp(nodeset->nodeTab[0], (const xmlChar *)"type");
				if (!keyword || !(strcasecmp((const char *)keyword,OPH_APPLY_PRIMITIVE_ID_STR)))
				{
					xmlFree(keyword);
					xmlXPathFreeObject(result);
					xmlXPathFreeContext(context);
					close_xml_doc(document);
					oph_apply_primitives_free(&p);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong parameter %s: reserved primitive\n", primitive[j]);
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_INVALID_INPUT_STRING );
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				else if (!keyword || !(strcasecmp((const char *)keyword,OPH_APPLY_PRIMITIVE_SIMPLE_STR))) p->primitive[j].type = OPH_APPLY_PRIMITIVE_SIMPLE;
				else if (!(strcasecmp((const char *)keyword,OPH_APPLY_PRIMITIVE_AGGREGATE_STR))) p->primitive[j].type = OPH_APPLY_PRIMITIVE_AGGREGATE;
				else if (!(strcasecmp((const char *)keyword,OPH_APPLY_PRIMITIVE_REDUCE_STR))) p->primitive[j].type = OPH_APPLY_PRIMITIVE_REDUCE;
				else if (!(strcasecmp((const char *)keyword,OPH_APPLY_PRIMITIVE_TOTAL_STR))) p->primitive[j].type = OPH_APPLY_PRIMITIVE_TOTAL;
				else
				{
					xmlFree(keyword);
					xmlXPathFreeObject(result);
					xmlXPathFreeContext(context);
					close_xml_doc(document);
					oph_apply_primitives_free(&p);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong parameter %s: bad operation type\n", primitive[j]);
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_INVALID_INPUT_STRING );
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				xmlFree(keyword);
			}
			xmlXPathFreeObject(result);
			xmlXPathFreeContext(context);

			close_xml_doc(document);
		}

		oph_apply_operation_type type = OPH_APPLY_PRIMITIVE_UNKNOWN;
		if (oph_apply_evaluate_query_type(p, &type))
		{
			oph_apply_primitives_free(&p);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong parameter %s: return type cannot be evaluated\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_INVALID_INPUT_STRING );
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if ((is_measure && !((OPH_APPLY_operator_handle*)handle->operator_handle)->set_measure_type) || (!is_measure && !((OPH_APPLY_operator_handle*)handle->operator_handle)->set_dimension_type))
		{
			if (((OPH_APPLY_operator_handle*)handle->operator_handle)->check_measure_type)
			{
				if (oph_apply_check_datatype(p,data_type))
				{
					oph_apply_primitives_free(&p);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong parameter %s: data types are not correct\n", OPH_IN_PARAM_APPLY_QUERY);
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_INVALID_INPUT_STRING );
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}
			else if (p && p->size && p->primitive[0].output_datatype) snprintf(data_type,OPH_ODB_CUBE_MEASURE_TYPE_SIZE,"%s",p->primitive[0].output_datatype);
			else
			{
				oph_apply_primitives_free(&p);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong parameter %s: output data type cannot be set\n", OPH_IN_PARAM_APPLY_QUERY);
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_INVALID_INPUT_STRING );
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}

		oph_apply_primitives_free(&p);

		if (type == OPH_APPLY_PRIMITIVE_UNKNOWN)
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong parameter %s: query must return a binary array\n", OPH_IN_PARAM_APPLY_QUERY);
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_INVALID_INPUT_STRING );
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		if ((type == OPH_APPLY_PRIMITIVE_AGGREGATE) || (type == OPH_APPLY_PRIMITIVE_TOTAL))
		{
			if (!is_measure)
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong parameter %s: query for dimension cannot include aggregate primitives\n", OPH_IN_PARAM_APPLY_QUERY);
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_INVALID_INPUT_STRING );
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
			else ((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size_update=1; 
		}
		((OPH_APPLY_operator_handle*)handle->operator_handle)->operation_type = type;
	  }

	return OPH_ANALYTICS_OPERATOR_SUCCESS;	
}

int env_set (HASHTBL *task_tbl, oph_operator_struct *handle)
{
  if (!handle){
  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
	  return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
  }

  if (!task_tbl){
	  pmesg(LOG_ERROR, __FILE__, __LINE__, "Null operator string\n");
      return OPH_ANALYTICS_OPERATOR_BAD_PARAMETER;
  }

  if (handle->operator_handle){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator handle already initialized\n");
    return OPH_ANALYTICS_OPERATOR_NOT_NULL_OPERATOR_HANDLE;
  }

  if (!(handle->operator_handle = (OPH_APPLY_operator_handle *) calloc (1, sizeof (OPH_APPLY_operator_handle)))){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
    logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_APPLY_MEMORY_ERROR_HANDLE );
    return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
  }

  //1 - Set up struct to empty values
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_datacube = 0;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_datacube = 0;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_job = 0;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container = 0;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_container = 0;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_ids = NULL;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_operation = NULL;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->dimension_operation = NULL;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->objkeys = NULL;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->objkeys_num = -1;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->server = NULL;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->measure = NULL;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->measure_type = NULL;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->dimension_type = NULL;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->compressed = 0;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size = 0;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->impl_size = 0;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size_update = 0;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->impl_size_update = 0;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->sessionid = NULL;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->operation_type = OPH_APPLY_PRIMITIVE_SIMPLE;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->set_measure_type = 0;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->set_dimension_type = 0;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->check_measure_type = 1;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_user = 0;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->num_reference_to_dim = 0;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_values = NULL;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_length = 0;
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->description = NULL;

  //3 - Fill struct with the correct data
  char *datacube_in;
  char *value;

  // retrieve objkeys
  value = hashtbl_get(task_tbl, OPH_IN_PARAM_OBJKEY_FILTER);
  if(!value){
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_OBJKEY_FILTER);
    logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_OBJKEY_FILTER );
    return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  if(oph_tp_parse_multiple_value_param(value, &((OPH_APPLY_operator_handle*)handle->operator_handle)->objkeys, &((OPH_APPLY_operator_handle*)handle->operator_handle)->objkeys_num)){
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Operator string not valid\n");
    logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, "Operator string not valid\n");
    oph_tp_free_multiple_value_param_list(((OPH_APPLY_operator_handle*)handle->operator_handle)->objkeys, ((OPH_APPLY_operator_handle*)handle->operator_handle)->objkeys_num);
    return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }

  // retrieve sessionid
  value = hashtbl_get(task_tbl, OPH_ARG_SESSIONID);
  if(!value){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_SESSIONID);
    logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_ARG_SESSIONID);
	return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  if(!(((OPH_APPLY_operator_handle*)handle->operator_handle)->sessionid = (char *) strndup (value, OPH_TP_TASKLEN))){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
    logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_GENERIC_MEMORY_ERROR_INPUT, "sessionid" );
	return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
  }

	value = hashtbl_get(task_tbl, OPH_IN_PARAM_DATACUBE_INPUT);
	datacube_in = value;
	if(!value){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DATACUBE_INPUT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_APPLY_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DATACUBE_INPUT );
	
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	}

  //For error checking
  int id_datacube_in[3] = { 0, 0, 0 };

  value = hashtbl_get(task_tbl, OPH_ARG_USERNAME);
  if(!value){
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_ARG_USERNAME);
    logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_APPLY_MISSING_INPUT_PARAMETER, OPH_ARG_USERNAME );
    return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  char *username = value;

  value = hashtbl_get(task_tbl, OPH_IN_PARAM_MEASURE_NAME);
  if(!value){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_MEASURE_NAME);
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_APPLY_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_MEASURE_NAME );
	return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  if(!(((OPH_APPLY_operator_handle*)handle->operator_handle)->measure = (char *) strndup (value, OPH_TP_TASKLEN))){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_APPLY_MEMORY_ERROR_HANDLE, "measure name" );
	return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
  }

  value = hashtbl_get(task_tbl, OPH_IN_PARAM_MEASURE_TYPE);
  if(!value){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_MEASURE_TYPE);
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_APPLY_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_MEASURE_TYPE );
	return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  if(!strcmp(value,OPH_COMMON_AUTO_VALUE)) ((OPH_APPLY_operator_handle*)handle->operator_handle)->set_measure_type = 1;

  value = hashtbl_get(task_tbl, OPH_IN_PARAM_DIMENSION_TYPE);
  if(!value){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DIMENSION_TYPE);
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_APPLY_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DIMENSION_TYPE );
	return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  if(!strcmp(value,OPH_COMMON_AUTO_VALUE)) ((OPH_APPLY_operator_handle*)handle->operator_handle)->set_dimension_type = 1;

  value = hashtbl_get(task_tbl, OPH_IN_PARAM_CHECK_TYPE);
  if(!value){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CHECK_TYPE);
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_APPLY_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CHECK_TYPE );
	return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  if(!strcmp(value,OPH_COMMON_NO_VALUE)) ((OPH_APPLY_operator_handle*)handle->operator_handle)->check_measure_type = 0;

  value = hashtbl_get(task_tbl, OPH_IN_PARAM_COMPRESSION);
  if(!value){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_COMPRESSION);
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_APPLY_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_COMPRESSION );
	return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  if(!strcmp(value,OPH_COMMON_YES_VALUE)) ((OPH_APPLY_operator_handle*)handle->operator_handle)->compressed = 1;
  if(!strcmp(value,OPH_COMMON_AUTO_VALUE)) ((OPH_APPLY_operator_handle*)handle->operator_handle)->compressed = -1;

  if(handle->proc_rank == 0)
  {
  		//Only master process has to initialize and open connection to management OphidiaDB
	  ophidiadb *oDB = &((OPH_APPLY_operator_handle*)handle->operator_handle)->oDB;
	  oph_odb_init_ophidiadb(oDB);	

	  if(oph_odb_read_ophidiadb_config_file(oDB)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_APPLY_OPHIDIADB_CONFIGURATION_FILE );
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	  }

	  if( oph_odb_connect_to_ophidiadb(oDB)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_GENERIC_CONTAINER_ID, OPH_LOG_OPH_APPLY_OPHIDIADB_CONNECTION_ERROR );
		return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	  }

	  //Check if datacube exists (by ID container and datacube)
	  int exists = 0;
	  int status = 0;
	  char *uri = NULL;
	  int folder_id = 0;
	  int permission = 0;
	  if(oph_pid_parse_pid(datacube_in, &id_datacube_in[1], &id_datacube_in[0], &uri)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse the PID string\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_APPLY_PID_ERROR, datacube_in );
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
	  }
	  else if((oph_odb_cube_check_if_datacube_not_present_by_pid(oDB, uri, id_datacube_in[1], id_datacube_in[0], &exists)) || !exists){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unknown input container - datacube combination\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_APPLY_NO_INPUT_DATACUBE, datacube_in );
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
	  }
	  else if((oph_odb_cube_check_datacube_availability(oDB, id_datacube_in[0], 0, &status)) || !status){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "I/O nodes storing datacube aren't available\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_APPLY_DATACUBE_AVAILABILITY_ERROR, datacube_in );
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
	  }
	  else if((oph_odb_fs_retrive_container_folder_id(oDB, id_datacube_in[1], 1, &folder_id))){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified datacube or container is hidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_APPLY_DATACUBE_FOLDER_ERROR, datacube_in );
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
	  }
	  else if((oph_odb_fs_check_folder_session(folder_id, ((OPH_APPLY_operator_handle*)handle->operator_handle)->sessionid, oDB, &permission)) || !permission){
		//Check if user can work on datacube
		pmesg(LOG_ERROR, __FILE__, __LINE__, "User %s is not allowed to work on this datacube\n", username);
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_APPLY_DATACUBE_PERMISSION_ERROR, username );
		id_datacube_in[0] = 0;
		id_datacube_in[1] = 0;
	  }
	if(uri) free(uri);
	uri = NULL;

	  if (oph_odb_user_retrieve_user_id(oDB, username, &(((OPH_APPLY_operator_handle*)handle->operator_handle)->id_user)))
	  {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to extract userid.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_USER_ID_ERROR );
		return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
	  }

	id_datacube_in[2] = id_datacube_in[1];
	if (id_datacube_in[1])
	{
		value = hashtbl_get(task_tbl, OPH_IN_PARAM_CONTAINER_INPUT);
		if(!value){
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_CONTAINER_INPUT);
			logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_CONTAINER_INPUT );
			return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
		}
		if (strncmp(value,OPH_COMMON_DEFAULT_EMPTY_VALUE,OPH_TP_TASKLEN))
		{
			if (oph_odb_fs_retrieve_container_id_from_container_name(oDB, folder_id, value, 0, &id_datacube_in[2]))
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve folder of specified container or it is hidden\n");
				logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_GENERIC_DATACUBE_FOLDER_ERROR, value );
				id_datacube_in[0] = 0;
				id_datacube_in[1] = 0;
			}
		}
	}
  }

  //Broadcast to all other processes the fragment relative index
  MPI_Bcast(id_datacube_in,3,MPI_INT,0,MPI_COMM_WORLD);

  //Check if sequential part has been completed
  if (id_datacube_in[0] == 0){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_APPLY_NO_INPUT_DATACUBE, datacube_in );
		
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
  }
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_datacube = id_datacube_in[0];

  if (id_datacube_in[1] == 0){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_APPLY_NO_INPUT_CONTAINER, datacube_in );
		
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
  }
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container = id_datacube_in[1];
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_container = id_datacube_in[2];

  value = hashtbl_get(task_tbl, OPH_IN_PARAM_SCHEDULE_ALGORITHM);
  if(!value){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_SCHEDULE_ALGORITHM);
    logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_APPLY_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_SCHEDULE_ALGORITHM );
	
    return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->schedule_algo = (int)strtol(value, NULL, 10);

  value = hashtbl_get(task_tbl, OPH_IN_PARAM_APPLY_QUERY);
  if(!value){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_APPLY_QUERY);
    logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_APPLY_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_APPLY_QUERY );
	
    return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  if(!(((OPH_APPLY_operator_handle*)handle->operator_handle)->array_operation = (char *) strndup (value, OPH_TP_TASKLEN))){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
	logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_APPLY_MEMORY_ERROR_INPUT, "array operation"  );
	
    return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
  }

  value = hashtbl_get(task_tbl, OPH_IN_PARAM_APPLY_DIM_QUERY);
  if(!value){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_APPLY_DIM_QUERY);
	logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_APPLY_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_APPLY_DIM_QUERY );
	return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  if (strncasecmp(value,OPH_COMMON_NULL_VALUE,OPH_TP_TASKLEN))
  {
	if(!(((OPH_APPLY_operator_handle*)handle->operator_handle)->dimension_operation = (char *) strndup (value, OPH_TP_TASKLEN)))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_APPLY_MEMORY_ERROR_INPUT, "dimension operation"  );
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
  }

  value = hashtbl_get(task_tbl, OPH_ARG_IDJOB);
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_job = value ? (int)strtol(value, NULL, 10) : 0;
  
  value = hashtbl_get(task_tbl, OPH_IN_PARAM_DESCRIPTION);
  if(!value){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Missing input parameter %s\n", OPH_IN_PARAM_DESCRIPTION);
	logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_FRAMEWORK_MISSING_INPUT_PARAMETER, OPH_IN_PARAM_DESCRIPTION );
	return OPH_ANALYTICS_OPERATOR_INVALID_PARAM;
  }
  if (strncmp(value,OPH_COMMON_DEFAULT_EMPTY_VALUE,OPH_TP_TASKLEN)){
	if(!(((OPH_APPLY_operator_handle*)handle->operator_handle)->description = (char *) strndup (value, OPH_TP_TASKLEN))){
		logging(LOG_ERROR, __FILE__, __LINE__, id_datacube_in[1], OPH_LOG_OPH_APPLY_MEMORY_ERROR_INPUT, "description" );
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	}
  }

  return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_init (oph_operator_struct *handle)
{
  if (!handle || !handle->operator_handle){
  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_NULL_OPERATOR_HANDLE );

	  return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
  }

  //For error checking
  int pointer, stream_max_size=9+OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE+5*sizeof(int)+sizeof(long long)+OPH_ODB_CUBE_MEASURE_TYPE_SIZE+OPH_TP_TASKLEN, flush=1;
  char stream[stream_max_size];
  memset(stream, 0, sizeof(stream));
  *stream = 0;
  char *id_string[6], *array_length, *data_type, *query;
  pointer=0; id_string[0]=stream+pointer;
  pointer+=1+OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE; id_string[1]=stream+pointer;
  pointer+=1+sizeof(int); id_string[2]=stream+pointer;
  pointer+=1+sizeof(int); id_string[3]=stream+pointer;
  pointer+=1+sizeof(int); id_string[4]=stream+pointer;
  pointer+=1+sizeof(int); id_string[5]=stream+pointer;
  pointer+=1+sizeof(int); array_length=stream+pointer;
  pointer+=1+sizeof(long long); data_type=stream+pointer;
  pointer+=1+OPH_ODB_CUBE_MEASURE_TYPE_SIZE; query=stream+pointer;

  if(handle->proc_rank == 0)
  {
	// Open the cube
	ophidiadb *oDB = &((OPH_APPLY_operator_handle*)handle->operator_handle)->oDB;
	oph_odb_datacube cube;
	oph_odb_cube_init_datacube(&cube);
	int datacube_id = ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_datacube, use_dim = 0;
	if(oph_odb_cube_retrieve_datacube(oDB, datacube_id, &cube))
	{
		oph_odb_cube_free_datacube(&cube);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while retrieving input datacube\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DATACUBE_READ_ERROR );
		goto __OPH_EXIT_1;
	}

	if (oph_apply_parse_query(handle, cube.measure_type, MYSQL_FRAG_MEASURE, MYSQL_DIMENSION, &use_dim))
	{
		oph_odb_cube_free_datacube(&cube);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in parsing '%s'\n", ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_operation);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_INVALID_INPUT_STRING );
		goto __OPH_EXIT_1;
	}

	  // Change the container id
	  cube.id_container = ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_container;

	  // If given, change the measure name
	  char *old_measure = NULL;
	  if (strncasecmp(((OPH_APPLY_operator_handle*)handle->operator_handle)->measure,OPH_COMMON_NULL_VALUE,OPH_TP_TASKLEN))
	  {
		old_measure = strdup(cube.measure);
		strncpy(cube.measure, ((OPH_APPLY_operator_handle*)handle->operator_handle)->measure, OPH_ODB_CUBE_MEASURE_SIZE);
	  }

	  // Save some parameters
	  ((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size = cube.tuplexfragment;
	  ((OPH_APPLY_operator_handle*)handle->operator_handle)->measure_type = strdup(cube.measure_type);
	  if (((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size_update) cube.tuplexfragment = 1;

	  char* array_operation = ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_operation;

	  // Add 'oph_uncompress'
	  if (cube.compressed)
	  {
		int offset=0;
		char tmp[OPH_TP_TASKLEN], *pch, *residual = array_operation;
		while ((pch = strcasestr(residual, MYSQL_FRAG_MEASURE)))
		{
			snprintf(tmp+offset,pch-residual+1,"%s",residual);
			offset += pch-residual;
			offset += snprintf(tmp+offset,OPH_TP_TASKLEN-offset,OPH_APPLY_UNCOMPRESS_MEASURE,MYSQL_FRAG_MEASURE);
			residual = pch+strlen(MYSQL_FRAG_MEASURE);
		}
		snprintf(tmp+offset,OPH_TP_TASKLEN-offset,"%s",residual);
		free(array_operation);
		((OPH_APPLY_operator_handle*)handle->operator_handle)->array_operation = array_operation = strdup(tmp);
	  }

	  // Change keyword MYSQL_DIMENSION with '?'
	  if (use_dim)
	  {
		int offset=0;
		char tmp[OPH_TP_TASKLEN], *pch, *residual = array_operation;
		while ((pch = strcasestr(residual, MYSQL_DIMENSION)))
		{
			snprintf(tmp+offset,pch-residual+1,"%s",residual);
			offset += pch-residual;
			offset += snprintf(tmp+offset,OPH_TP_TASKLEN-offset,"?");
			residual = pch+strlen(MYSQL_DIMENSION);
			((OPH_APPLY_operator_handle*)handle->operator_handle)->num_reference_to_dim++;
		}
		snprintf(tmp+offset,OPH_TP_TASKLEN-offset,"%s",residual);
		free(array_operation);
		((OPH_APPLY_operator_handle*)handle->operator_handle)->array_operation = array_operation = strdup(tmp);
	  }

	  if (((OPH_APPLY_operator_handle*)handle->operator_handle)->compressed < 0) ((OPH_APPLY_operator_handle*)handle->operator_handle)->compressed = cube.compressed;
	  cube.compressed = ((OPH_APPLY_operator_handle*)handle->operator_handle)->compressed;

	  // Add 'oph_compress'
	  if (cube.compressed)
	  {
		char tmp[OPH_TP_TASKLEN];
		snprintf(tmp,OPH_TP_TASKLEN,OPH_APPLY_COMPRESS_MEASURE,array_operation);
		free(array_operation);
		((OPH_APPLY_operator_handle*)handle->operator_handle)->array_operation = array_operation = strdup(tmp);
	  }

	  //New fields
	  cube.id_source = 0;
	  cube.level++;
	  if (((OPH_APPLY_operator_handle*)handle->operator_handle)->description) snprintf(cube.description,OPH_ODB_CUBE_DESCRIPTION_SIZE,"%s",((OPH_APPLY_operator_handle*)handle->operator_handle)->description);
	  else *cube.description = 0;

	  //Insert new datacube
	  if(oph_odb_cube_insert_into_datacube_partitioned_tables(oDB, &cube, &(((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_datacube))){
		oph_odb_cube_free_datacube(&cube);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update datacube table\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DATACUBE_INSERT_ERROR );
		goto __OPH_EXIT_1;	 
	  }
	  //Copy fragment id relative index set	
	  if(!(((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_ids = (char *) strndup (cube.frag_relative_index_set, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE)))
	  {
		oph_odb_cube_free_datacube(&cube);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_MEMORY_ERROR_INPUT, "fragment ids" );
		goto __OPH_EXIT_1;
	  } 
	  oph_odb_cube_free_datacube(&cube);

  	  oph_odb_cubehasdim *cubedims = NULL;
	  int number_of_dimensions = 0;
	  int last_insertd_id = 0;

	  //Read old cube - dimension relation rows
	  if(oph_odb_cube_retrieve_cubehasdim_list(oDB, datacube_id, &cubedims, &number_of_dimensions)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive datacube - dimension relations.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_CUBEHASDIM_READ_ERROR );
		free(cubedims);
		goto __OPH_EXIT_1;
	  }

	// Pre-parsing: warning if the number of implicit dimension is higher than 1
	int l, number_of_implicit_dimensions = 0, main_impl_dim = -1;

	  //Write new cube - dimension relation rows
	  for(l = number_of_dimensions-1; l>=0; l--)
	  {
		// Pre-parsing: warning if the number of implicit dimension is higher than 1
		if (!cubedims[l].explicit_dim && cubedims[l].size)
		{
			main_impl_dim = l;
			number_of_implicit_dimensions++;
			if (((OPH_APPLY_operator_handle*)handle->operator_handle)->impl_size) ((OPH_APPLY_operator_handle*)handle->operator_handle)->impl_size *= cubedims[l].size;
			else ((OPH_APPLY_operator_handle*)handle->operator_handle)->impl_size = cubedims[l].size;
		}
	  }

	// Pre-parsing: warning if the number of implicit dimension is higher than 1
	if (number_of_implicit_dimensions>1)
	{
		if (use_dim)
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to process '%s': too many implicit dimensions in input cube.\n", MYSQL_DIMENSION);
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_TOO_DIMENSION_ERROR, MYSQL_DIMENSION);
			free(cubedims);
			goto __OPH_EXIT_1;
		}
		else
		{
			pmesg(LOG_WARNING, __FILE__, __LINE__, "The number of implicit dimensions will be set to 1.\n");
			logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_TOO_DIMENSION_WARNING );
		}
	}
	else if (!number_of_implicit_dimensions && use_dim)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to process '%s': no implicit dimension in input cube.\n", MYSQL_DIMENSION);
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_TOO_DIMENSION_ERROR, MYSQL_DIMENSION);
		free(cubedims);
		goto __OPH_EXIT_1;
	}

	if ((use_dim || ((OPH_APPLY_operator_handle*)handle->operator_handle)->dimension_operation) && (main_impl_dim>=0))
	{
		oph_odb_dimension dim;
		oph_odb_dimension_instance dim_inst;
		if (oph_odb_dim_retrieve_dimension_instance(oDB, cubedims[main_impl_dim].id_dimensioninst, &dim_inst, datacube_id))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DIM_READ_ERROR);
			free(cubedims);
			goto __OPH_EXIT_1;
		}
		if (oph_odb_dim_retrieve_dimension(oDB, dim_inst.id_dimension, &dim, datacube_id))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DIM_READ_ERROR);
			free(cubedims);
			goto __OPH_EXIT_1;
		}
		if (((OPH_APPLY_operator_handle*)handle->operator_handle)->dimension_operation)
		{
			char dimension_type[OPH_ODB_DIM_DIMENSION_TYPE_SIZE];
			strcpy(dimension_type,dim.dimension_type);
			if (oph_apply_parse_query(handle, dim.dimension_type, MYSQL_DIMENSION, NULL, NULL))
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in parsing '%s'\n", ((OPH_APPLY_operator_handle*)handle->operator_handle)->dimension_operation);
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_INVALID_INPUT_STRING );
				free(cubedims);
				goto __OPH_EXIT_1;
			}
			if (strncasecmp(dim.dimension_type,dimension_type,OPH_ODB_DIM_DIMENSION_TYPE_SIZE))
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Data type update for dimension is not supported. Check '%s'\n", ((OPH_APPLY_operator_handle*)handle->operator_handle)->dimension_operation);
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_INVALID_INPUT_STRING );
				free(cubedims);
				goto __OPH_EXIT_1;
			}
		}
		if (use_dim)
		{
			oph_odb_db_instance db_;
			oph_odb_db_instance *db = &db_;
			if (oph_dim_load_dim_dbinstance(db))
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DIM_LOAD );
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
			if (oph_dim_connect_to_dbms(db->dbms_instance, 0))
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DIM_CONNECT );
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
			if (oph_dim_use_db_of_dbms(db->dbms_instance, db))
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DIM_USE_DB );
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				goto __OPH_EXIT_1;
			}

			char* dim_row = NULL;
			char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
			snprintf(index_dimension_table_name,OPH_COMMON_BUFFER_LEN,OPH_DIM_TABLE_NAME_MACRO,((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container);
			snprintf(label_dimension_table_name,OPH_COMMON_BUFFER_LEN,OPH_DIM_TABLE_LABEL_MACRO,((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container);

			int compressed = 0; 
			if (oph_dim_read_dimension_data(db, index_dimension_table_name, dim_inst.fk_id_dimension_index, MYSQL_DIMENSION, compressed, &dim_row) || !dim_row)
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DIM_READ_ERROR);    				
				if (dim_row) free(dim_row);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				goto __OPH_EXIT_1;
			}
			if (dim_inst.fk_id_dimension_label)
			{
				if (oph_dim_read_dimension_filtered_data(db, label_dimension_table_name, dim_inst.fk_id_dimension_label, MYSQL_DIMENSION, compressed, &dim_row, dim.dimension_type, dim_inst.size) || !dim_row)
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DIM_READ_ERROR);    				
					if (dim_row) free(dim_row);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					goto __OPH_EXIT_1;
				}
			}
			else strncpy(dim.dimension_type, OPH_DIM_INDEX_DATA_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE); // A reduced dimension is handled by indexes
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);

			((OPH_APPLY_operator_handle*)handle->operator_handle)->array_values = dim_row;
			((OPH_APPLY_operator_handle*)handle->operator_handle)->array_length = dim_inst.size;
			if (!strcasecmp(dim.dimension_type,OPH_COMMON_DOUBLE_TYPE)) ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_length *= sizeof(double);
			else if (!strcasecmp(dim.dimension_type,OPH_COMMON_FLOAT_TYPE)) ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_length *= sizeof(float);
			else if (!strcasecmp(dim.dimension_type,OPH_COMMON_LONG_TYPE)) ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_length *= sizeof(long long);
			else if (!strcasecmp(dim.dimension_type,OPH_COMMON_INT_TYPE)) ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_length *= sizeof(int);
			else if (!strcasecmp(dim.dimension_type,OPH_COMMON_SHORT_TYPE)) ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_length *= sizeof(short);
			else if (!strcasecmp(dim.dimension_type,OPH_COMMON_BYTE_TYPE)) ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_length *= sizeof(char);
			else
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to found data type '%s'.\n", dim.dimension_type);
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_TYPE_ERROR, dim.dimension_type);    				
				free(cubedims);
				goto __OPH_EXIT_1;
			}
		}
	  }

	  free(cubedims);

	  if (oph_odb_meta_copy_from_cube_to_cube(oDB, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_datacube, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_datacube, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_user))
	  {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to copy metadata.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_METADATA_COPY_ERROR );
		goto __OPH_EXIT_1;
	  }

	  if (old_measure)
	  {
		if (oph_odb_meta_update_metadatakeys(oDB, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_datacube, old_measure, ((OPH_APPLY_operator_handle*)handle->operator_handle)->measure))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to copy metadata.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_METADATA_COPY_ERROR );
			goto __OPH_EXIT_1;
		}
		free(old_measure);
		old_measure = NULL;
	  }

	  last_insertd_id = 0;
	  oph_odb_task new_task;
	  new_task.id_outputcube = ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_datacube;
	  new_task.id_job = ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_job;
	  strncpy(new_task.operator, handle->operator_type, OPH_ODB_CUBE_OPERATOR_SIZE);
	  memset(new_task.query, 0, OPH_ODB_CUBE_OPERATION_QUERY_SIZE);
	  snprintf(new_task.query, OPH_ODB_CUBE_OPERATION_QUERY_SIZE, OPH_APPLY_QUERY, MYSQL_FRAG_ID, array_operation, MYSQL_FRAG_MEASURE);
	  new_task.input_cube_number = 1;
	  if(!(new_task.id_inputcube = (int*)malloc(new_task.input_cube_number*sizeof(int))))
	  {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_MEMORY_ERROR_STRUCT, "task" );

		goto __OPH_EXIT_1;
	  }	  
	  new_task.id_inputcube[0] = ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_datacube;

	  if(oph_odb_cube_insert_into_task_table(oDB, &new_task, &last_insertd_id)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new task.\n");
	    logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_TASK_INSERT_ERROR, new_task.operator);

	    free(new_task.id_inputcube);
		goto __OPH_EXIT_1;
	  } 
	  free(new_task.id_inputcube);

	  strncpy(id_string[0], ((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_ids, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE);
	  memcpy(id_string[1], &((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_datacube, sizeof(int));
	  memcpy(id_string[2], &((OPH_APPLY_operator_handle*)handle->operator_handle)->compressed, sizeof(int));
	  memcpy(id_string[3], &((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size_update, sizeof(int));
	  memcpy(id_string[4], &((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size, sizeof(int));
	  memcpy(id_string[5], &((OPH_APPLY_operator_handle*)handle->operator_handle)->num_reference_to_dim, sizeof(int));
	  memcpy(array_length, &((OPH_APPLY_operator_handle*)handle->operator_handle)->array_length, sizeof(long long));

	  strncpy(data_type, ((OPH_APPLY_operator_handle*)handle->operator_handle)->measure_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE);

	  strncpy(query, ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_operation, OPH_TP_TASKLEN);

	flush=0;
  }
__OPH_EXIT_1:
  if (!handle->proc_rank && flush) oph_odb_cube_delete_from_datacube_table(&((OPH_APPLY_operator_handle*)handle->operator_handle)->oDB, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_datacube);
  //Broadcast to all other processes the fragment relative index
  MPI_Bcast(stream,stream_max_size,MPI_CHAR,0,MPI_COMM_WORLD);
  if (*stream == 0){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Master procedure or broadcasting has failed\n");
	    logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_MASTER_TASK_INIT_FAILED);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
  }
  if(handle->proc_rank != 0)
  {
	  if(!(((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_ids = (char *) strndup (id_string[0], OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_MEMORY_ERROR_INPUT, "fragment ids");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	  }
	  if(!(((OPH_APPLY_operator_handle*)handle->operator_handle)->measure_type = (char *) strndup (data_type, OPH_ODB_CUBE_MEASURE_TYPE_SIZE))){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_MEMORY_ERROR_INPUT, "measure type");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	  }
	  if(!(((OPH_APPLY_operator_handle*)handle->operator_handle)->array_operation = (char *) strndup (query, OPH_TP_TASKLEN))){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_MEMORY_ERROR_INPUT, "array operation");
		return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
	  }
	  ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_datacube = *((int*)id_string[1]);
	  ((OPH_APPLY_operator_handle*)handle->operator_handle)->compressed = *((int*)id_string[2]);
	  ((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size_update = *((int*)id_string[3]);
	  ((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size = *((int*)id_string[4]);
	  ((OPH_APPLY_operator_handle*)handle->operator_handle)->num_reference_to_dim = *((int*)id_string[5]);
	  ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_length = *((long long*)array_length);
  }

  if (((OPH_APPLY_operator_handle*)handle->operator_handle)->num_reference_to_dim && ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_length)
  {
	if (handle->proc_rank) ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_values = (char*)malloc(((OPH_APPLY_operator_handle*)handle->operator_handle)->array_length);
	MPI_Bcast(((OPH_APPLY_operator_handle*)handle->operator_handle)->array_values,((OPH_APPLY_operator_handle*)handle->operator_handle)->array_length,MPI_CHAR,0,MPI_COMM_WORLD);
  }

  return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_distribute(oph_operator_struct *handle)
{
  if (!handle || !handle->operator_handle){
  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_NULL_OPERATOR_HANDLE);
	  return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
  }

  int id_number;
  char new_id_string[OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE];

  //Get total number of fragment IDs
  if(oph_ids_count_number_of_ids(((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_ids, &id_number)){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get total number of IDs\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_RETREIVE_IDS_ERROR);    
    return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
  }

  //All processes compute the fragment number to work on
  int div_result = (id_number)/(handle->proc_number);
  int div_remainder = (id_number)%(handle->proc_number);

  //Every process must process at least divResult
  ((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_number = div_result;

  if(div_remainder != 0)
  {
	//Only some certain processes must process an additional part
	if (handle->proc_rank/div_remainder == 0)
		((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_number++;
  }

  int i;
  //Compute fragment IDs starting position
  if(((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_number == 0){
	// In case number of process is higher than fragment number
    ((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_id_start_position = -1;
  }
  else{
	((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_id_start_position = 0;
	for(i = handle->proc_rank-1; i >= 0; i--)
	{
		if(div_remainder != 0)
			((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_id_start_position += (div_result + (i/div_remainder == 0 ? 1 : 0) );
		else
			((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_id_start_position += div_result;
	}
	if(((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_id_start_position >= id_number) ((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_id_start_position = -1;	
  }

  if(((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank!= 0)
	return OPH_ANALYTICS_OPERATOR_SUCCESS;

  //Partition fragment relative index string
  char *new_ptr = new_id_string;
  if(oph_ids_get_substring_from_string(((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_ids, ((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_id_start_position, ((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_number, &new_ptr)){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to split IDs fragment string\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_ID_STRING_SPLIT_ERROR);    
	return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
  }

  free(((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_ids);
  if(!(((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_ids = (char *) strndup (new_id_string, OPH_ODB_CUBE_FRAG_REL_INDEX_SET_SIZE))){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocating memory\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_MEMORY_ERROR_INPUT, "fragment ids");    
    return OPH_ANALYTICS_OPERATOR_MEMORY_ERR;
  }

  return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_execute(oph_operator_struct *handle)
{
  if (!handle || !handle->operator_handle){
  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_NULL_OPERATOR_HANDLE);    
	  return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
  }

  if(((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_id_start_position < 0 && handle->proc_rank!= 0)
	return OPH_ANALYTICS_OPERATOR_SUCCESS;

  int i, j, k;
  int id_datacube_out = ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_datacube; 
  char *array_operation = ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_operation;
  int id_datacube_in = ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_datacube;

  oph_odb_fragment_list frags;
  oph_odb_db_instance_list dbs;
  oph_odb_dbms_instance_list dbmss;

	//Each process has to be connected to a slave ophidiadb
  ophidiadb oDB_slave;
  oph_odb_init_ophidiadb(&oDB_slave);	

  if(oph_odb_read_ophidiadb_config_file(&oDB_slave)){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read OphidiaDB configuration\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_OPHIDIADB_CONFIGURATION_FILE );
	return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
  }

  if( oph_odb_connect_to_ophidiadb(&oDB_slave)){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to OphidiaDB. Check access parameters.\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_OPHIDIADB_CONNECTION_ERROR );
    oph_odb_free_ophidiadb(&oDB_slave);
	return OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
  }

  //retrieve connection string
  if(oph_odb_stge_fetch_fragment_connection_string(&oDB_slave, id_datacube_in, ((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_ids, &frags, &dbs, &dbmss)){
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive connection strings\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_CONNECTION_STRINGS_NOT_FOUND);    
    oph_odb_free_ophidiadb(&oDB_slave);
	return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
  }

  char frag_name_out[OPH_ODB_STGE_FRAG_NAME_SIZE];
  int frag_count = 0, first=1, tuplexfragment;
  int result = OPH_ANALYTICS_OPERATOR_SUCCESS;
  int size = ((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size;
  long long size_;

  if(oph_dc2_setup_dbms(&(((OPH_APPLY_operator_handle*)handle->operator_handle)->server), (dbmss.value[0]).io_server_type))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to initialize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_IOPLUGIN_SETUP_ERROR, (dbmss.value[0]).id_dbms);    
		result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

  //For each DBMS
  for(i = 0; (i < dbmss.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS);  i++){

	if(oph_dc2_connect_to_dbms(((OPH_APPLY_operator_handle*)handle->operator_handle)->server, &(dbmss.value[i]) ,0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to connect to DBMS. Check access parameters.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DBMS_CONNECTION_ERROR, (dbmss.value[i]).id_dbms);    
		result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}
	
	//For each DB
	for(j = 0; (j < dbs.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); j++){
		//Check DB - DBMS Association
		if(dbs.value[j].dbms_instance != &(dbmss.value[i])) continue;

		if(oph_dc2_use_db_of_dbms(((OPH_APPLY_operator_handle*)handle->operator_handle)->server, &(dbmss.value[i]), &(dbs.value[j])))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to use the DB. Check access parameters.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DB_SELECTION_ERROR, (dbs.value[j]).db_name);    
			result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
			break;
		}

		//For each fragment
		for(k = 0; (k < frags.size) && (result == OPH_ANALYTICS_OPERATOR_SUCCESS); k++){
			//Check Fragment - DB Association
			if(frags.value[k].db_instance != &(dbs.value[j])) continue;

			if (((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size_update)
			{
				tuplexfragment = frags.value[k].key_end-frags.value[k].key_start+1; // Under the assumption that IDs are consecutive without any holes
				if (frags.value[k].key_end && ( (tuplexfragment<size) || (tuplexfragment%size) ) )
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_OPH_APPLY_TUPLES_CONSTRAINT_FAILED,size,tuplexfragment);
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_TUPLES_CONSTRAINT_FAILED,size,tuplexfragment );		    				
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
			}

			if(oph_dc2_generate_fragment_name(NULL, id_datacube_out, handle->proc_rank, (frag_count+1), &frag_name_out))
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Size of frag  name exceed limit.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_STRING_BUFFER_OVERFLOW, "fragment name", frag_name_out);    				
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}

			//Apply operation to fragment
			size_ = size;
			if (((OPH_APPLY_operator_handle*)handle->operator_handle)->num_reference_to_dim && ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_values && ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_length)
			{
				if(oph_dc2_create_fragment_from_query_with_params(((OPH_APPLY_operator_handle*)handle->operator_handle)->server, &(frags.value[k]), frag_name_out, array_operation, 0, ((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size_update ? &size_ : 0, 0, ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_values, ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_length, ((OPH_APPLY_operator_handle*)handle->operator_handle)->num_reference_to_dim))
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new fragment.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_NEW_FRAG_ERROR, frag_name_out);    				
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
			}
			else if(oph_dc2_create_fragment_from_query(((OPH_APPLY_operator_handle*)handle->operator_handle)->server, &(frags.value[k]), frag_name_out, array_operation, 0, ((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size_update ? &size_ : 0, 0))
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert new fragment.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_NEW_FRAG_ERROR, frag_name_out);    				
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}

			//Change fragment fields
			frags.value[k].id_datacube = id_datacube_out;
			strncpy(frags.value[k].fragment_name, frag_name_out, OPH_ODB_STGE_FRAG_NAME_SIZE);
			if (((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size_update && frags.value[k].key_end)
			{
				frags.value[k].key_start = 1 + (frags.value[k].key_start-1) / size;
				frags.value[k].key_end = 1 + (frags.value[k].key_end-1) / size;
			}

			// Extract the number of elements of resulting array
			if (!handle->proc_rank && first)
			{
				long long old_impl_size=((OPH_APPLY_operator_handle*)handle->operator_handle)->impl_size, new_impl_size=0;
				if (oph_dc2_get_number_of_elements_in_fragment_row(((OPH_APPLY_operator_handle*)handle->operator_handle)->server, &(frags.value[k]), ((OPH_APPLY_operator_handle*)handle->operator_handle)->measure_type, ((OPH_APPLY_operator_handle*)handle->operator_handle)->compressed, &new_impl_size) || !new_impl_size)
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to extract the number of element of resulting rows.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_FRAGMENT_READ_ERROR);    				
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				if (new_impl_size != old_impl_size)
				{
					((OPH_APPLY_operator_handle*)handle->operator_handle)->impl_size_update=1;
					((OPH_APPLY_operator_handle*)handle->operator_handle)->impl_size = (int)new_impl_size;
				}

				long long new_expl_size=0;
				if (oph_dc2_get_total_number_of_rows_in_fragment(((OPH_APPLY_operator_handle*)handle->operator_handle)->server, &(frags.value[k]), ((OPH_APPLY_operator_handle*)handle->operator_handle)->measure_type, &new_expl_size) || !new_expl_size)
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to extract the number of rows of resulting fragment.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_FRAGMENT_READ_ERROR);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}
				if (((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size_update)
				{
					if (new_expl_size!=1)
					{
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Indexes of fragments are corrupted.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_FRAGMENT_INDEX_ERROR);
						result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						break;
					}
				}
				else if (((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size < new_expl_size)
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Metadata are corrupted: expl_size %lld is greater than the expected value %d\n",new_expl_size,((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size);
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_METADATA_SET_ERROR);
					result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					break;
				}

				first=0;
			}

			//Insert new fragment
			if(oph_odb_stge_insert_into_fragment_table(&oDB_slave, &(frags.value[k]))){
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update fragment table.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_FRAGMENT_INSERT_ERROR, frag_name_out);    				
				result = OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				break;
			}
			frag_count++;
		}
	}
	oph_dc2_disconnect_from_dbms(((OPH_APPLY_operator_handle*)handle->operator_handle)->server, &(dbmss.value[i]));

  }

	if(oph_dc2_cleanup_dbms(((OPH_APPLY_operator_handle*)handle->operator_handle)->server))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to finalize IO server.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_IOPLUGIN_CLEANUP_ERROR, (dbmss.value[0]).id_dbms);    
		result = OPH_ANALYTICS_OPERATOR_MYSQL_ERROR;
	}

  oph_odb_stge_free_fragment_list(&frags);
  oph_odb_stge_free_db_list(&dbs);
  oph_odb_stge_free_dbms_list(&dbmss);
  oph_odb_free_ophidiadb(&oDB_slave);
  
  if(handle->proc_rank == 0 && (result == OPH_ANALYTICS_OPERATOR_SUCCESS))
  {
	  //Master process print output datacube PID
    char *tmp_uri = NULL;
	if (oph_pid_get_uri(&tmp_uri) ){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retrieve web server URI.\n");
		logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_PID_URI_ERROR );
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if(oph_pid_show_pid(((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_container, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_datacube, tmp_uri)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to print PID string\n");
		logging(LOG_WARNING, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_PID_SHOW_ERROR );
		free(tmp_uri);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	char jsonbuf[OPH_COMMON_BUFFER_LEN];
	memset(jsonbuf,0,OPH_COMMON_BUFFER_LEN);
	snprintf(jsonbuf,OPH_COMMON_BUFFER_LEN, OPH_PID_FORMAT, tmp_uri, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_container, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_datacube);
		
	// ADD OUTPUT PID TO JSON AS TEXT
	if (oph_json_is_objkey_printable(((OPH_APPLY_operator_handle*)handle->operator_handle)->objkeys,((OPH_APPLY_operator_handle*)handle->operator_handle)->objkeys_num,OPH_JSON_OBJKEY_APPLY)) {
		if (oph_json_add_text(handle->operator_json,OPH_JSON_OBJKEY_APPLY,"Output Cube",jsonbuf)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "ADD TEXT error\n");
			logging(LOG_WARNING,__FILE__,__LINE__, OPH_GENERIC_CONTAINER_ID,"ADD TEXT error\n");
			free(tmp_uri);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}

	// ADD OUTPUT PID TO NOTIFICATION STRING
	char tmp_string[OPH_COMMON_BUFFER_LEN];
	snprintf(tmp_string, OPH_COMMON_BUFFER_LEN, "%s=%s;", OPH_IN_PARAM_DATACUBE_INPUT, jsonbuf);
	if (handle->output_string)
	{
		strncat(tmp_string, handle->output_string, OPH_COMMON_BUFFER_LEN-strlen(tmp_string));
		free(handle->output_string);
	}
	handle->output_string = strdup(tmp_string);

	free(tmp_uri);
  }

  if (!handle->proc_rank && (result != OPH_ANALYTICS_OPERATOR_SUCCESS)) oph_odb_cube_delete_from_datacube_table(&((OPH_APPLY_operator_handle*)handle->operator_handle)->oDB, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_datacube);

  return result;
}

int task_reduce(oph_operator_struct *handle)
{
  if (!handle || !handle->operator_handle){
  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_NULL_OPERATOR_HANDLE);    
	  return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
  }

  if(!handle->proc_rank)
  {
	ophidiadb *oDB = &((OPH_APPLY_operator_handle*)handle->operator_handle)->oDB;
	int datacube_id = ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_datacube;

	oph_odb_cubehasdim *cubedims = NULL;
	int l, number_of_dimensions = 0, last_insertd_id = 0, first=-1, reduced_impl_dim=-1, reduced_expl_dim=-1;
	int size = ((OPH_APPLY_operator_handle*)handle->operator_handle)->impl_size, residual_size = ((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size;

	//Read old cube - dimension relation rows
	if(oph_odb_cube_retrieve_cubehasdim_list(oDB, datacube_id, &cubedims, &number_of_dimensions))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to retreive datacube - dimension relations.\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_CUBEHASDIM_READ_ERROR );
		free(cubedims);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	oph_odb_dimension dim[number_of_dimensions];
	oph_odb_dimension_instance dim_inst[number_of_dimensions];
	for (l=0;l<number_of_dimensions;++l)
	{
		if (oph_odb_dim_retrieve_dimension_instance(oDB, cubedims[l].id_dimensioninst, &(dim_inst[l]), datacube_id))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DIM_READ_ERROR);
			free(cubedims);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (oph_odb_dim_retrieve_dimension(oDB, dim_inst[l].id_dimension, &(dim[l]), datacube_id))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading dimension information.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DIM_READ_ERROR);
			free(cubedims);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}

	oph_odb_db_instance db_;
	oph_odb_db_instance *db = &db_;
	if (oph_dim_load_dim_dbinstance(db))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading dimension db paramters\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DIM_LOAD );
		oph_dim_unload_dim_dbinstance(db);		
		free(cubedims);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (oph_dim_connect_to_dbms(db->dbms_instance, 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while connecting to dimension dbms\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DIM_CONNECT );
		oph_dim_disconnect_from_dbms(db->dbms_instance);
		oph_dim_unload_dim_dbinstance(db);
		free(cubedims);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}
	if (oph_dim_use_db_of_dbms(db->dbms_instance, db))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while opening dimension db\n");
		logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DIM_USE_DB );
		oph_dim_disconnect_from_dbms(db->dbms_instance);
		oph_dim_unload_dim_dbinstance(db);
		free(cubedims);
		return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
	}

	short is_reduce = (((OPH_APPLY_operator_handle*)handle->operator_handle)->operation_type == OPH_APPLY_PRIMITIVE_REDUCE) || (((OPH_APPLY_operator_handle*)handle->operator_handle)->operation_type == OPH_APPLY_PRIMITIVE_TOTAL);

	//Write new cube - dimension relation rows
	for(l=number_of_dimensions-1; l>=0; l--)
	{
		// Change iddatacube in cubehasdim
		cubedims[l].id_datacube = ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_datacube;

		if (cubedims[l].size)
		{
			if (cubedims[l].explicit_dim)
			{
				// Adaptation due to table size modification
				if (((OPH_APPLY_operator_handle*)handle->operator_handle)->expl_size_update)
				{
					if (cubedims[l].size > residual_size)
					{
						if (cubedims[l].size % residual_size)
						{
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update dimension information\n");
							logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_SIZE_COMPUTE_ERROR );
							oph_dim_disconnect_from_dbms(db->dbms_instance);
							oph_dim_unload_dim_dbinstance(db);
							free(cubedims);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
						reduced_expl_dim = l;
						cubedims[l].size /= residual_size;
						dim_inst[l].concept_level = OPH_COMMON_CONCEPT_LEVEL_UNKNOWN;
						break;
					}
					else // This dimension will be collapsed, so there is another size to be reduced
					{
						if (residual_size % cubedims[l].size)
						{
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to update dimension information\n");
							logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_SIZE_COMPUTE_ERROR );
							oph_dim_disconnect_from_dbms(db->dbms_instance);
							oph_dim_unload_dim_dbinstance(db);
							free(cubedims);
							return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
						}
						residual_size /= cubedims[l].size;
						cubedims[l].size = cubedims[l].level = 0;
						dim_inst[l].concept_level = OPH_COMMON_ALL_CONCEPT_LEVEL;
						if (residual_size==1) break;
					}
				}
			}
			else
			{
				// Adaptation due to array size modification
				if (((OPH_APPLY_operator_handle*)handle->operator_handle)->impl_size_update || is_reduce)
				{
					if (first) 
					{
						if ((size==1) && is_reduce)
						{
							cubedims[l].size=cubedims[l].level=0;
							dim_inst[l].concept_level = OPH_COMMON_ALL_CONCEPT_LEVEL;
						}
						else
						{
							cubedims[l].size=size;
							cubedims[l].level=1;
							if (is_reduce) dim_inst[l].concept_level = OPH_COMMON_CONCEPT_LEVEL_UNKNOWN;
							reduced_impl_dim = l; // Values of dimensions will be changed even if no reduction is called, e.g. in case of subsetting
						}
						first=0;
					}
					else // Other implicit dimensions will be collapsed
					{
						cubedims[l].size=cubedims[l].level=0;
						dim_inst[l].concept_level = OPH_COMMON_ALL_CONCEPT_LEVEL;
					}
				}
			}
		}
	}

	char* dim_row;
	int updated_dim = -1, compressed = 0, n; 

	char index_dimension_table_name[OPH_COMMON_BUFFER_LEN], label_dimension_table_name[OPH_COMMON_BUFFER_LEN], operation[OPH_COMMON_BUFFER_LEN];
	snprintf(index_dimension_table_name,OPH_COMMON_BUFFER_LEN,OPH_DIM_TABLE_NAME_MACRO,((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container);
	snprintf(label_dimension_table_name,OPH_COMMON_BUFFER_LEN,OPH_DIM_TABLE_LABEL_MACRO,((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container);
	char o_index_dimension_table_name[OPH_COMMON_BUFFER_LEN], o_label_dimension_table_name[OPH_COMMON_BUFFER_LEN];
	snprintf(o_index_dimension_table_name,OPH_COMMON_BUFFER_LEN,OPH_DIM_TABLE_NAME_MACRO,((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_container);
	snprintf(o_label_dimension_table_name,OPH_COMMON_BUFFER_LEN,OPH_DIM_TABLE_LABEL_MACRO,((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_container);

	for(l=number_of_dimensions-1; l>=0; l--)
	{
		if (!dim_inst[l].fk_id_dimension_index)
		{
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Dimension FK not set in cubehasdim.\n");
			break;
		}
		dim_row = NULL;

		if (cubedims[l].size) // Extract the subset only in case the dimension is not collapsed
		{
			// Build the vector of labels
			// Only the implicit dimension will be treated
			// The update concerns the unique implicit dimension to be reduced or the first dimension found in scanning
			if (((OPH_APPLY_operator_handle*)handle->operator_handle)->dimension_operation && !cubedims[l].explicit_dim && (updated_dim<0) && ((l == reduced_impl_dim) || (reduced_impl_dim<0)))
			{
				updated_dim = l;

				// Load input indexes
				if (compressed)
				{
					n = snprintf(operation, OPH_COMMON_BUFFER_LEN, OPH_APPLY_UNCOMPRESS_MEASURE, MYSQL_DIMENSION);
					if(n >= OPH_COMMON_BUFFER_LEN)
					{
						pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_STRING_BUFFER_OVERFLOW, "MySQL operation name", operation);    		
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
				}
				else strncpy(operation, MYSQL_DIMENSION, OPH_COMMON_BUFFER_LEN);
				if (oph_dim_read_dimension_data(db, index_dimension_table_name, dim_inst[l].fk_id_dimension_index, operation, 0, &dim_row) || !dim_row)
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DIM_READ_ERROR);    				
					if (dim_row) free(dim_row);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
				// Load input labels
				if (dim_inst[l].fk_id_dimension_label)
				{
					if (oph_dim_read_dimension_filtered_data(db, label_dimension_table_name, dim_inst[l].fk_id_dimension_label, operation, 0, &dim_row, dim[l].dimension_type, dim_inst[l].size) || !dim_row)
					{
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
						logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DIM_READ_ERROR);    				
						if (dim_row) free(dim_row);
						oph_dim_disconnect_from_dbms(db->dbms_instance);
						oph_dim_unload_dim_dbinstance(db);
						free(cubedims);
						return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
					}
				}
				else strncpy(dim[l].dimension_type, OPH_DIM_INDEX_DATA_TYPE, OPH_ODB_DIM_DIMENSION_TYPE_SIZE); // A reduced dimension is handled by indexes

				// Store output labels
				if (oph_dim_insert_into_dimension_table_from_query(db, o_label_dimension_table_name, dim[l].dimension_type, dim_inst[l].size, ((OPH_APPLY_operator_handle*)handle->operator_handle)->dimension_operation, dim_row, &(dim_inst[l].fk_id_dimension_label)))
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new row in dimension table.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DIM_ROW_ERROR);    				
					if (dim_row) free(dim_row);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}

				if (dim_row) free(dim_row);
				dim_row = NULL;
			}

			if ((l == reduced_expl_dim) || (l == reduced_impl_dim) || (l == updated_dim))
			{
				n = snprintf(operation, OPH_COMMON_BUFFER_LEN, MYSQL_DIM_INDEX_ARRAY, OPH_DIM_INDEX_DATA_TYPE, 1, cubedims[l].size);
				if(n >= OPH_COMMON_BUFFER_LEN)
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "MySQL operation name exceed limit.\n");
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_STRING_BUFFER_OVERFLOW, "MySQL operation name", operation);    		
					if (dim_row) free(dim_row);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}

				if (l != updated_dim) dim_inst[l].fk_id_dimension_label = 0;
			}
			else strncpy(operation, MYSQL_DIMENSION, OPH_COMMON_BUFFER_LEN);

			if (oph_dim_read_dimension_data(db, index_dimension_table_name, dim_inst[l].fk_id_dimension_index, operation, compressed, &dim_row))
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in reading a row from dimension table.\n");
				logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DIM_READ_ERROR);
				if (dim_row) free(dim_row);
				oph_dim_disconnect_from_dbms(db->dbms_instance);
				oph_dim_unload_dim_dbinstance(db);
				free(cubedims);
				return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
			}
		}
		else
		{
			dim_inst[l].fk_id_dimension_label = 0;
			if (dim[l].calendar && strlen(dim[l].calendar)) // Time dimension (the check can be improved by checking hierarchy name)
			{
				if (oph_odb_meta_put(oDB, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_datacube, NULL, OPH_ODB_TIME_FREQUENCY, 0, OPH_COMMON_FULL_REDUCED_DIM))
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_LOG_GENERIC_METADATA_UPDATE_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_GENERIC_METADATA_UPDATE_ERROR);    				
					if (dim_row) free(dim_row);
					oph_dim_disconnect_from_dbms(db->dbms_instance);
					oph_dim_unload_dim_dbinstance(db);
					free(cubedims);
					return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
				}
			}
		}

		dim_inst[l].size = cubedims[l].size;

		if (oph_dim_insert_into_dimension_table(db, o_index_dimension_table_name, OPH_DIM_INDEX_DATA_TYPE, dim_inst[l].size, dim_row, &(dim_inst[l].fk_id_dimension_index)))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new row in dimension table.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DIM_ROW_ERROR);
			if (dim_row) free(dim_row);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(cubedims);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}

		dim_inst[l].id_grid = 0; 
		if (oph_odb_dim_insert_into_dimensioninstance_table(oDB, &(dim_inst[l]), &(cubedims[l].id_dimensioninst), ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_datacube, NULL, NULL))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in inserting a new dimension instance\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_DIM_INSTANCE_STORE_ERROR);
			if (dim_row) free(dim_row);
			oph_dim_disconnect_from_dbms(db->dbms_instance);
			oph_dim_unload_dim_dbinstance(db);
			free(cubedims);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
		if (dim_row) free(dim_row);
	}
	oph_dim_disconnect_from_dbms(db->dbms_instance);
	oph_dim_unload_dim_dbinstance(db);

	for(l=0;l<number_of_dimensions; l++)
	{
		//Change iddatacube in cubehasdim
		cubedims[l].id_datacube = ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_output_datacube;
		if(oph_odb_cube_insert_into_cubehasdim_table(oDB, &(cubedims[l]), &last_insertd_id))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to insert datacube - dimension relations.\n");
			logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_CUBEHASDIM_INSERT_ERROR );
			free(cubedims);
			return OPH_ANALYTICS_OPERATOR_UTILITY_ERROR;
		}
	}
	free(cubedims);
  }

  return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int task_destroy(oph_operator_struct *handle)
{
  if (!handle || !handle->operator_handle){
  	pmesg(LOG_ERROR, __FILE__, __LINE__, "Null Handle\n");
	logging(LOG_ERROR, __FILE__, __LINE__, ((OPH_APPLY_operator_handle*)handle->operator_handle)->id_input_container, OPH_LOG_OPH_APPLY_NULL_OPERATOR_HANDLE);    
	  return OPH_ANALYTICS_OPERATOR_NULL_OPERATOR_HANDLE;
  }
  
  return OPH_ANALYTICS_OPERATOR_SUCCESS;
}

int env_unset(oph_operator_struct *handle)
{
  //If NULL return success; it's already free
  if (!handle || !handle->operator_handle)
    return OPH_ANALYTICS_OPERATOR_SUCCESS;    
 
  //Only master process has to close and release connection to management OphidiaDB
  if(handle->proc_rank == 0){
	  oph_odb_free_ophidiadb(&((OPH_APPLY_operator_handle*)handle->operator_handle)->oDB);
  }
  if(((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_ids){
	  free((char *)((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_ids); 
	  ((OPH_APPLY_operator_handle*)handle->operator_handle)->fragment_ids = NULL;
  }
  if(((OPH_APPLY_operator_handle*)handle->operator_handle)->array_operation){
	  free((char *)((OPH_APPLY_operator_handle*)handle->operator_handle)->array_operation); 
	  ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_operation = NULL;
  }
  if(((OPH_APPLY_operator_handle*)handle->operator_handle)->dimension_operation){
	  free((char *)((OPH_APPLY_operator_handle*)handle->operator_handle)->dimension_operation); 
	  ((OPH_APPLY_operator_handle*)handle->operator_handle)->dimension_operation = NULL;
  }
  if(((OPH_APPLY_operator_handle*)handle->operator_handle)->measure){
	  free((char *)((OPH_APPLY_operator_handle*)handle->operator_handle)->measure); 
	  ((OPH_APPLY_operator_handle*)handle->operator_handle)->measure = NULL;
  }
  if(((OPH_APPLY_operator_handle*)handle->operator_handle)->measure_type){
	  free((char *)((OPH_APPLY_operator_handle*)handle->operator_handle)->measure_type);
	  ((OPH_APPLY_operator_handle*)handle->operator_handle)->measure_type = NULL;
  }
  if(((OPH_APPLY_operator_handle*)handle->operator_handle)->dimension_type){
	  free((char *)((OPH_APPLY_operator_handle*)handle->operator_handle)->dimension_type);
	  ((OPH_APPLY_operator_handle*)handle->operator_handle)->dimension_type = NULL;
  }
  if(((OPH_APPLY_operator_handle*)handle->operator_handle)->array_values){
	  free((char *)((OPH_APPLY_operator_handle*)handle->operator_handle)->array_values);
	  ((OPH_APPLY_operator_handle*)handle->operator_handle)->array_values = NULL;
  }
  if(((OPH_APPLY_operator_handle*)handle->operator_handle)->objkeys){
      oph_tp_free_multiple_value_param_list(((OPH_APPLY_operator_handle*)handle->operator_handle)->objkeys, ((OPH_APPLY_operator_handle*)handle->operator_handle)->objkeys_num);
      ((OPH_APPLY_operator_handle*)handle->operator_handle)->objkeys = NULL;
  }
  if(((OPH_APPLY_operator_handle*)handle->operator_handle)->sessionid){
	  free((char *)((OPH_APPLY_operator_handle*)handle->operator_handle)->sessionid);
	  ((OPH_APPLY_operator_handle*)handle->operator_handle)->sessionid = NULL;
  }
  if(((OPH_APPLY_operator_handle*)handle->operator_handle)->description){
	  free((char *)((OPH_APPLY_operator_handle*)handle->operator_handle)->description);
	  ((OPH_APPLY_operator_handle*)handle->operator_handle)->description = NULL;
  }
  free((OPH_APPLY_operator_handle*)handle->operator_handle);
  handle->operator_handle = NULL;
  
  return OPH_ANALYTICS_OPERATOR_SUCCESS;
}
