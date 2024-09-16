/*
    Ophidia Analytics Framework
    Copyright (C) 2012-2024 CMCC Foundation

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

//gsoap oph  schema namespace:          urn:oph

typedef char *xsd__anyURI;
typedef char xsd__boolean;
typedef char *xsd__date;
typedef time_t xsd__dateTime;
typedef double xsd__double;
typedef char *xsd__duration;
typedef float xsd__float;
typedef char *xsd__time;
typedef char *xsd__decimal;
typedef char *xsd__integer;
typedef long long xsd__long;
typedef long xsd__int;
typedef short xsd__short;
typedef char xsd__byte;
typedef char *xsd__nonPositiveInteger;
typedef char *xsd__negativeInteger;
typedef char *xsd__nonNegativeInteger;
typedef char *xsd__positiveInteger;
typedef unsigned long long xsd__unsignedLong;
typedef unsigned long xsd__unsignedInt;
typedef unsigned short xsd__unsignedShort;
typedef unsigned char xsd__unsignedByte;
typedef char *xsd__string;
typedef char *xsd__normalizedString;
typedef char *xsd__token;

//gsoap oph  service method-protocol:   oph_notify SOAP
//gsoap oph  service method-style:      oph_notify document
//gsoap oph  service method-action:     oph_notify ""
//gsoap oph  service method-documentation: oph_notify Notify a job status update

int oph__oph_notify(xsd__string oph_notify_data, xsd__string oph_notify_json, xsd__int * oph_notify_response);
