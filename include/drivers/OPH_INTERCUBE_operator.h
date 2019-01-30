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

#ifndef __OPH_INTERCUBE_OPERATOR_H
#define __OPH_INTERCUBE_OPERATOR_H

//Operator specific headers
#include "oph_ophidiadb_main.h"
#include "oph_common.h"
#include "oph_ioserver_library.h"

#define OPH_INTERCUBE_OPERATION_SUM  "sum"
#define OPH_INTERCUBE_OPERATION_SUB  "sub"
#define OPH_INTERCUBE_OPERATION_MUL  "mul"
#define OPH_INTERCUBE_OPERATION_DIV  "div"
#define OPH_INTERCUBE_OPERATION_ABS  "abs"
#define OPH_INTERCUBE_OPERATION_ARG  "arg"
#define OPH_INTERCUBE_OPERATION_CORR "corr"
#define OPH_INTERCUBE_OPERATION_MASK "mask"
#define OPH_INTERCUBE_OPERATION_MAX "max"
#define OPH_INTERCUBE_OPERATION_MIN "min"
#define OPH_INTERCUBE_OPERATION_ARG_MAX "arg_max"
#define OPH_INTERCUBE_OPERATION_ARG_MIN "arg_min"

#define OPH_INTERCUBE_QUERY_COMPR_SUM OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_compress('','',oph_sum_array('oph_%s|oph_%s','oph_%s', oph_uncompress('','',fact_in1.%s),oph_uncompress('','',fact_in2.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")
#define OPH_INTERCUBE_QUERY_SUM OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_sum_array('oph_%s|oph_%s','oph_%s', fact_in1.%s,fact_in2.%s, %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")

#define OPH_INTERCUBE_QUERY_COMPR_SUB OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_compress('','',oph_sub_array('oph_%s|oph_%s','oph_%s', oph_uncompress('', '',fact_in1.%s),oph_uncompress('', '',fact_in2.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")
#define OPH_INTERCUBE_QUERY_SUB OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_sub_array('oph_%s|oph_%s','oph_%s', fact_in1.%s,fact_in2.%s, %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")

#define OPH_INTERCUBE_QUERY_COMPR_MUL OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_compress('','',oph_mul_array('oph_%s|oph_%s','oph_%s', oph_uncompress('','',fact_in1.%s),oph_uncompress('','',fact_in2.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")
#define OPH_INTERCUBE_QUERY_MUL OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_mul_array('oph_%s|oph_%s','oph_%s', fact_in1.%s,fact_in2.%s, %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")

#define OPH_INTERCUBE_QUERY_COMPR_DIV OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_compress('','',oph_div_array('oph_%s|oph_%s','oph_%s', oph_uncompress('','',fact_in1.%s),oph_uncompress('','',fact_in2.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")
#define OPH_INTERCUBE_QUERY_DIV OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_div_array('oph_%s|oph_%s','oph_%s', fact_in1.%s,fact_in2.%s, %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")

#define OPH_INTERCUBE_QUERY_COMPR_ABS OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_compress('','',oph_abs_array('oph_%s|oph_%s','oph_%s', oph_uncompress('','',fact_in1.%s),oph_uncompress('','',fact_in2.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")
#define OPH_INTERCUBE_QUERY_ABS OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_abs_array('oph_%s|oph_%s','oph_%s', fact_in1.%s,fact_in2.%s, %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")

#define OPH_INTERCUBE_QUERY_COMPR_ARG OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_compress('','',oph_arg_array('oph_%s|oph_%s','oph_%s', oph_uncompress('','',fact_in1.%s),oph_uncompress('','',fact_in2.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")
#define OPH_INTERCUBE_QUERY_ARG OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_arg_array('oph_%s|oph_%s','oph_%s', fact_in1.%s,fact_in2.%s, %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")

#define OPH_INTERCUBE_QUERY_COMPR_MASK OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_compress('','',oph_mask_array('oph_%s|oph_%s','oph_%s', oph_uncompress('','',fact_in1.%s),oph_uncompress('','',fact_in2.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")
#define OPH_INTERCUBE_QUERY_MASK OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_mask_array('oph_%s|oph_%s','oph_%s', fact_in1.%s,fact_in2.%s, %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")

#define OPH_INTERCUBE_QUERY_COMPR_MAX OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_compress('','',oph_max_array('oph_%s|oph_%s','oph_%s', oph_uncompress('','',fact_in1.%s),oph_uncompress('','',fact_in2.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")
#define OPH_INTERCUBE_QUERY_MAX OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_max_array('oph_%s|oph_%s','oph_%s', fact_in1.%s,fact_in2.%s, %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")

#define OPH_INTERCUBE_QUERY_COMPR_MIN OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_compress('','',oph_min_array('oph_%s|oph_%s','oph_%s', oph_uncompress('','',fact_in1.%s),oph_uncompress('','',fact_in2.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")
#define OPH_INTERCUBE_QUERY_MIN OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_min_array('oph_%s|oph_%s','oph_%s', fact_in1.%s,fact_in2.%s, %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")

#define OPH_INTERCUBE_QUERY_COMPR_ARG_MAX OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_compress('','',oph_arg_max_array('oph_%s|oph_%s','oph_%s', oph_uncompress('','',fact_in1.%s),oph_uncompress('','',fact_in2.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")
#define OPH_INTERCUBE_QUERY_ARG_MAX OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_arg_max_array('oph_%s|oph_%s','oph_%s', fact_in1.%s,fact_in2.%s, %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")

#define OPH_INTERCUBE_QUERY_COMPR_ARG_MIN OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_compress('','',oph_arg_min_array('oph_%s|oph_%s','oph_%s', oph_uncompress('','',fact_in1.%s),oph_uncompress('','',fact_in2.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")
#define OPH_INTERCUBE_QUERY_ARG_MIN OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_arg_min_array('oph_%s|oph_%s','oph_%s', fact_in1.%s,fact_in2.%s, %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")

#define OPH_INTERCUBE_QUERY_COMPR_CORR OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_compress('','',oph_gsl_correlation('oph_%s|oph_%s','oph_%s', oph_uncompress('','',fact_in1.%s),oph_uncompress('','',fact_in2.%s)))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")
#define OPH_INTERCUBE_QUERY_CORR OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "fact_out") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "fact_in1.%s|oph_gsl_correlation('oph_%s|oph_%s','oph_%s', fact_in1.%s,fact_in2.%s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "fact_in1|fact_in2")  OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "fact_in1.%s = fact_in2.%s")

#ifdef OPH_DEBUG_MYSQL
#define OPH_INTERCUBE_QUERY2_COMPR_MYSQL_SUM "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_compress(oph_sum_array(oph_uncompress(%s.%s),oph_uncompress(%s.%s),'oph_%s','oph_%s', %s)) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"
#define OPH_INTERCUBE_QUERY2_MYSQL_SUM "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_sum_array(%s.%s,%s.%s,'oph_%s','oph_%s', %s) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"

#define OPH_INTERCUBE_QUERY2_COMPR_MYSQL_SUB "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_compress(oph_sub_array(oph_uncompress(%s.%s),oph_uncompress(%s.%s),'oph_%s','oph_%s', %s)) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"
#define OPH_INTERCUBE_QUERY2_MYSQL_SUB "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_sub_array(%s.%s,%s.%s,'oph_%s','oph_%s', %s) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"

#define OPH_INTERCUBE_QUERY2_COMPR_MYSQL_MUL "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_compress(oph_mul_array(oph_uncompress(%s.%s),oph_uncompress(%s.%s),'oph_%s','oph_%s', %s)) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"
#define OPH_INTERCUBE_QUERY2_MYSQL_MUL "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_mul_array(%s.%s,%s.%s,'oph_%s','oph_%s', %s) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"

#define OPH_INTERCUBE_QUERY2_COMPR_MYSQL_DIV "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_compress(oph_div_array(oph_uncompress(%s.%s),oph_uncompress(%s.%s),'oph_%s','oph_%s', %s)) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"
#define OPH_INTERCUBE_QUERY2_MYSQL_DIV "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_div_array(%s.%s,%s.%s,'oph_%s','oph_%s', %s) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"

#define OPH_INTERCUBE_QUERY2_COMPR_MYSQL_ABS "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_compress(oph_abs_array(oph_uncompress(%s.%s),oph_uncompress(%s.%s),'oph_%s','oph_%s', %s)) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"
#define OPH_INTERCUBE_QUERY2_MYSQL_ABS "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_abs_array(%s.%s,%s.%s,'oph_%s','oph_%s', %s) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"

#define OPH_INTERCUBE_QUERY2_COMPR_MYSQL_ARG "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_compress(oph_arg_array(oph_uncompress(%s.%s),oph_uncompress(%s.%s),'oph_%s','oph_%s', %s)) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"
#define OPH_INTERCUBE_QUERY2_MYSQL_ARG "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_arg_array(%s.%s,%s.%s,'oph_%s','oph_%s', %s) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"

#define OPH_INTERCUBE_QUERY2_COMPR_MYSQL_MASK "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_compress(oph_mask_array(oph_uncompress(%s.%s),oph_uncompress(%s.%s),'oph_%s','oph_%s', %s)) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"
#define OPH_INTERCUBE_QUERY2_MYSQL_MASK "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_mask_array(%s.%s,%s.%s,'oph_%s','oph_%s', %s) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"

#define OPH_INTERCUBE_QUERY2_COMPR_MYSQL_MAX "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_compress(oph_max_array(oph_uncompress(%s.%s),oph_uncompress(%s.%s),'oph_%s','oph_%s', %s)) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"
#define OPH_INTERCUBE_QUERY2_MYSQL_MAX "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_max_array(%s.%s,%s.%s,'oph_%s','oph_%s', %s) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"

#define OPH_INTERCUBE_QUERY2_COMPR_MYSQL_MIN "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_compress(oph_min_array(oph_uncompress(%s.%s),oph_uncompress(%s.%s),'oph_%s','oph_%s', %s)) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"
#define OPH_INTERCUBE_QUERY2_MYSQL_MIN "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_min_array(%s.%s,%s.%s,'oph_%s','oph_%s', %s) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"

#define OPH_INTERCUBE_QUERY2_COMPR_MYSQL_ARG_MAX "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_compress(oph_arg_max_array(oph_uncompress(%s.%s),oph_uncompress(%s.%s),'oph_%s','oph_%s', %s)) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"
#define OPH_INTERCUBE_QUERY2_MYSQL_ARG_MAX "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_arg_max_array(%s.%s,%s.%s,'oph_%s','oph_%s', %s) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"

#define OPH_INTERCUBE_QUERY2_COMPR_MYSQL_ARG_MIN "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_compress(oph_arg_min_array(oph_uncompress(%s.%s),oph_uncompress(%s.%s),'oph_%s','oph_%s', %s)) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"
#define OPH_INTERCUBE_QUERY2_MYSQL_ARG_MIN "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_arg_min_array(%s.%s,%s.%s,'oph_%s','oph_%s', %s) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"

#define OPH_INTERCUBE_QUERY2_COMPR_MYSQL_CORR "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_compress(oph_gsl_correlation(oph_uncompress(%s.%s),oph_uncompress(%s.%s),'oph_%s','oph_%s')) AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"
#define OPH_INTERCUBE_QUERY2_MYSQL_CORR "CREATE TABLE %s (%s integer, %s longblob) ENGINE=MyISAM DEFAULT CHARSET=latin1 AS SELECT %s.%s AS %s, oph_gsl_correlation(%s.%s,%s.%s,'oph_%s','oph_%s') AS %s FROM %s.%s, %s.%s WHERE %s.%s = %s.%s"
#endif

#define OPH_INTERCUBE_FRAG1 "frag1"
#define OPH_INTERCUBE_FRAG2 "frag2"

#define OPH_INTERCUBE_QUERY2_COMPR_SUM OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_compress('','',oph_sum_array('oph_%s|oph_%s','oph_%s', oph_uncompress('','',%s.%s),oph_uncompress('','',%s.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")
#define OPH_INTERCUBE_QUERY2_SUM OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_sum_array('oph_%s|oph_%s','oph_%s',%s.%s,%s.%s,%s)") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")

#define OPH_INTERCUBE_QUERY2_COMPR_SUB OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_compress('','',oph_sub_array('oph_%s|oph_%s','oph_%s',oph_uncompress('','',%s.%s),oph_uncompress('','',%s.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")
#define OPH_INTERCUBE_QUERY2_SUB OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_sub_array('oph_%s|oph_%s','oph_%s',%s.%s,%s.%s,%s)") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")

#define OPH_INTERCUBE_QUERY2_COMPR_MUL OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_compress('','',oph_mul_array('oph_%s|oph_%s','oph_%s',oph_uncompress('','',%s.%s),oph_uncompress('','',%s.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")
#define OPH_INTERCUBE_QUERY2_MUL OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_mul_array('oph_%s|oph_%s','oph_%s',%s.%s,%s.%s,%s)") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")

#define OPH_INTERCUBE_QUERY2_COMPR_DIV OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_compress('','',oph_div_array('oph_%s|oph_%s','oph_%s',oph_uncompress('','',%s.%s),oph_uncompress('','',%s.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")
#define OPH_INTERCUBE_QUERY2_DIV OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_div_array('oph_%s|oph_%s','oph_%s',%s.%s,%s.%s,%s)") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")

#define OPH_INTERCUBE_QUERY2_COMPR_ABS OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_compress('','',oph_abs_array('oph_%s|oph_%s','oph_%s',oph_uncompress('','',%s.%s),oph_uncompress('','',%s.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")
#define OPH_INTERCUBE_QUERY2_ABS OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_abs_array('oph_%s|oph_%s','oph_%s',%s.%s,%s.%s,%s)") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")

#define OPH_INTERCUBE_QUERY2_COMPR_ARG OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_compress('','',oph_arg_array('oph_%s|oph_%s','oph_%s',oph_uncompress('','',%s.%s),oph_uncompress('','',%s.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")
#define OPH_INTERCUBE_QUERY2_ARG OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_arg_array('oph_%s|oph_%s','oph_%s',%s.%s,%s.%s,%s)") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")

#define OPH_INTERCUBE_QUERY2_COMPR_MASK OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_compress('','',oph_mask_array('oph_%s|oph_%s','oph_%s',oph_uncompress('','',%s.%s),oph_uncompress('','',%s.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")
#define OPH_INTERCUBE_QUERY2_MASK OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_mask_array('oph_%s|oph_%s','oph_%s',%s.%s,%s.%s,%s)") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")

#define OPH_INTERCUBE_QUERY2_COMPR_MAX OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_compress('','',oph_max_array('oph_%s|oph_%s','oph_%s',oph_uncompress('','',%s.%s),oph_uncompress('','',%s.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")
#define OPH_INTERCUBE_QUERY2_MAX OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_max_array('oph_%s|oph_%s','oph_%s',%s.%s,%s.%s,%s)") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")

#define OPH_INTERCUBE_QUERY2_COMPR_MIN OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_compress('','',oph_min_array('oph_%s|oph_%s','oph_%s',oph_uncompress('','',%s.%s),oph_uncompress('','',%s.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")
#define OPH_INTERCUBE_QUERY2_MIN OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_min_array('oph_%s|oph_%s','oph_%s',%s.%s,%s.%s,%s)") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")

#define OPH_INTERCUBE_QUERY2_COMPR_ARG_MAX OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_compress('','',oph_arg_max_array('oph_%s|oph_%s','oph_%s',oph_uncompress('','',%s.%s),oph_uncompress('','',%s.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")
#define OPH_INTERCUBE_QUERY2_ARG_MAX OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_arg_max_array('oph_%s|oph_%s','oph_%s',%s.%s,%s.%s,%s)") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")

#define OPH_INTERCUBE_QUERY2_COMPR_ARG_MIN OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_compress('','',oph_arg_min_array('oph_%s|oph_%s','oph_%s',oph_uncompress('','',%s.%s),oph_uncompress('','',%s.%s), %s))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")
#define OPH_INTERCUBE_QUERY2_ARG_MIN OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_arg_min_array('oph_%s|oph_%s','oph_%s',%s.%s,%s.%s,%s)") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")

#define OPH_INTERCUBE_QUERY2_COMPR_CORR OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_compress('','',oph_gsl_correlation('oph_%s|oph_%s','oph_%s',oph_uncompress('','',%s.%s),oph_uncompress('','',%s.%s)))") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")
#define OPH_INTERCUBE_QUERY2_CORR OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_OPERATION, OPH_IOSERVER_SQ_OP_CREATE_FRAG_SELECT) OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FRAG, "%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD, "%s.%s|oph_gsl_correlation('oph_%s|oph_%s','oph_%s',%s.%s,%s.%s)") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FIELD_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM, "%s.%s|%s.%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_FROM_ALIAS, "%s|%s") OPH_IOSERVER_SQ_BLOCK(OPH_IOSERVER_SQ_ARG_WHERE, "%s.%s = %s.%s")

#define OPH_INTERCUBE_OP_SUM "oph_sum_array"
#define OPH_INTERCUBE_OP_SUB "oph_sub_array"
#define OPH_INTERCUBE_OP_MUL "oph_mul_array"
#define OPH_INTERCUBE_OP_DIV "oph_div_array"
#define OPH_INTERCUBE_OP_ABS "oph_abs_array"
#define OPH_INTERCUBE_OP_ARG "oph_arg_array"
#define OPH_INTERCUBE_OP_MASK "oph_mask_array"
#define OPH_INTERCUBE_OP_CORR "oph_gsl_correlation"
#define OPH_INTERCUBE_OP_MAX "oph_max_array"
#define OPH_INTERCUBE_OP_MIN "oph_min_array"
#define OPH_INTERCUBE_OP_ARG_MAX "oph_arg_max_array"
#define OPH_INTERCUBE_OP_ARG_MIN "oph_arg_min_array"

/**
 * \brief Structure of parameters needed by the operator OPH_INTERCUBE. It executes an operation on two cubes materializing a new datacube
 * \param oDB Contains the parameters and the connection to OphidiaDB
 * \param id_input_datacube IDs of the input datacubes to operate on
 * \param id_input_container ID of the output container to operate on
 * \param id_output_datacube ID of the output datacube to operate on
 * \param id_job ID of the job related to the task
 * \param schedule_algo Number of the distribution algorithm to use 
 * \param fragment_ids Contains the string of fragment relative index
 * \param fragment_number Number of fragments that a process has to manage
 * \param fragment_id_start_position First fragment in the relative index set to work on
 * \param compressed If the data array stored is compressed (1) or not (0)
 * \param objkeys OPH_JSON objkeys to be included in output JSON file.
 * \param objkeys_num Number of objkeys.
 * \param server Pointer to I/O server handler
 * \param measure Name of the resulting measure
 * \param operation Type of operation to be executed
 * \param sessionid SessionID
 * \param id_user ID of submitter
 * \param description Free description to be associated with output cube
 * \param ms Conventional value for missing values
 * \param execute_error Flag set to 1 in case of error has to be handled in destroy
 */
struct _OPH_INTERCUBE_operator_handle {
	ophidiadb oDB;
	int id_input_datacube[2];
	int id_input_container;
	int id_output_datacube;
	int id_output_container;
	int id_job;
	int schedule_algo;
	char *fragment_ids;
	int fragment_number;
	int fragment_id_start_position;
	char *measure_type;
	int compressed;
	char **objkeys;
	int objkeys_num;
	oph_ioserver_handler *server;
	char *measure;
	char *operation;
	char *sessionid;
	int id_user;
	char *description;
	double ms;
	short int execute_error;
};
typedef struct _OPH_INTERCUBE_operator_handle OPH_INTERCUBE_operator_handle;

#endif				//__OPH_INTERCUBE_OPERATOR_H
