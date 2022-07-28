#
#    Ophidia Analytics Framework
#    Copyright (C) 2012-2022 CMCC Foundation
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

AC_PREREQ([2.60])
AC_INIT([ophidia-analytics-framework], [1.7.1], [ophidia-info@cmcc.it])

AC_CONFIG_AUX_DIR([config])

AM_INIT_AUTOMAKE([foreign])

AC_CONFIG_HEADERS([config.h])
AC_CONFIG_SRCDIR([src])

AC_CONFIG_MACRO_DIR([m4])

LT_CONFIG_LTDL_DIR([libltdl])
LT_INIT[dlopen]
LTDL_INIT

AC_SUBST(INCLTDL)
AC_SUBST(LIBLTDL)

AC_PROG_CC
AM_PROG_CC_STDC
AM_PROG_CC_C_O
AC_PROG_CXX


if test -z "$CC" && test -n "$MPICC"; then
   CC="$MPICC"
fi
AX_MPI(AC_MSG_NOTICE([MPI library found!]),AC_MSG_ERROR([MPI library not found! Aborting configure!]))
CC="$MPICC"
LIBS="$MPILIBS $LIBS"

AM_CONDITIONAL([HAVE_MPI], [test -n "$MPILIBS"])

AC_CHECK_HEADERS([stddef.h],  ,AC_MSG_ERROR([Cannot find stddef.h]))

AC_CHECK_FUNCS([floor])
AC_CHECK_FUNCS([strchr strtol strcasecmp strdup])
AC_CHECK_FUNCS([memmove])
AC_FUNC_MALLOC
AC_FUNC_REALLOC

AC_CHECK_HEADER([dlfcn.h],  ,AC_MSG_ERROR([Cannot find dlfcn.h]))

case "$host_os" in
	*bsd*)
		AC_CHECK_LIB(c, dlopen,,AC_MSG_ERROR([Cannot find -lc]))
	;; *)
		AC_CHECK_LIB(dl, dlopen,,AC_MSG_ERROR([Cannot find -ldl]))
	;;
esac

CFLAGS=""
CPPFLAGS=""
CXXFLAGS=""

AX_LIB_MYSQL([5.6])
if test -n $HAVE_MYSQL; then
  AC_MSG_NOTICE([MySQL libraries available])
else
	AC_MSG_ERROR([MySQL libraries not found! Aborting configure!])
fi

AC_CHECK_LIB(pthread, pthread_create)
if test "$ac_cv_lib_pthread_pthread_create" = yes ; then
   CFLAGS="$CFLAGS -D_REENTRANT"
else
   AC_CHECK_LIB(pthreads, pthread_create)
   if test "$ac_cv_lib_pthreads_pthread_create" = yes ; then
      CFLAGS="$CFLAGS -D_REENTRANT"
   fi
fi

optimization="no"
AC_ARG_ENABLE(optimization,
        [  --enable-optimization          turn on optimization flags],
        [optimization="yes"
        AC_MSG_WARN("Optimization activated")],
        [optimization="no"]
        )

benchmark=
OPT=""
AC_ARG_ENABLE(benchmark,
       [  --enable-benchmark    Enable support for taking execution times.],
       [
            if test "x$enableval" = "xno"; then
                benchmark="no"
            else
                benchmark="yes"
            fi
       ],
       [ benchmark="no"]
       )
if test "x${benchmark}" = "xyes"; then
    AC_MSG_NOTICE("Enabled benchmark mode")
    OPT+=" -DBENCHMARK"
else
    OPT+=""
fi

AC_ARG_WITH(web_server_path,
	   [  --with-web-server-path=PATH  Set the web server path (default /var/www/html)],
	   [
		web_server_path="$withval"
	   ],
	   [
		web_server_path="/var/www/html"
	   ]
	   )
OPH_WEB_SERVER_LOCATION=$web_server_path
AC_SUBST(OPH_WEB_SERVER_LOCATION)

AC_ARG_WITH(web_server_url,
	   [  --with-web-server-url=URL  Set the web server URL (default http://127.0.0.1)],
	   [
		web_server_url="$withval"
	   ],
	   [
		web_server_url="http://127.0.0.1"
	   ]
	   )
OPH_WEB_SERVER=$web_server_url
AC_SUBST(OPH_WEB_SERVER)

debug="0"
AC_ARG_WITH(debug-level,
	   [  --with-debug-level=LEVEL	Set debug level. (Disabled by default)],
	   [ debug="$withval"],
       [debug="0"]
	   )
if (test "x$debug" = "x1") || (test "x$debug" = "x2"); then
	AC_MSG_WARN("Debug activated")
	OPT+=" -g3 -Wall -Wextra  -O0"
 	AC_SUBST(OPT)
else
	debug="0"
	OPT+=" -O3"
	AC_SUBST(OPT)
fi
AM_CONDITIONAL(DEBUG_1, test "x$debug" = "x1")
AM_CONDITIONAL(DEBUG_2, test "x$debug" = "x2")

AC_ARG_WITH(driver_file_path,
	   [  --with-driver-file-path=PATH  Set the driver path (default :$prefix/lib/drivers)],
	   [
		driver_path="$withval"
	   ],
	   [
		if test "x${prefix}" != "xNONE"; then
			driver_path="${prefix}/lib/drivers"
		else  driver_path="/usr/local/lib/drivers"
		fi
	   ]
	   )
DRIVER_PATH=$driver_path
AC_SUBST(DRIVER_PATH)

AC_ARG_WITH(ioserver_file_path,
	   [  --with-ioserver-file-path=PATH  Set the storage ioserver path (default :$prefix/lib/ioservers)],
	   [
		ioserver_path="$withval"
	   ],
	   [
		if test "x${prefix}" != "xNONE"; then
			ioserver_path="${prefix}/lib/ioservers"
		else  ioserver_path="/usr/local/lib/ioservers"
		fi
	   ]
	   )
IOSERVER_PATH=$ioserver_path
AC_SUBST(IOSERVER_PATH)


PKG_CHECK_MODULES(GSL,[gsl >= 1.8])
AC_ARG_WITH(gsl_header_path,
       [  --with-gsl-header-path=PATH        Location of GSL includes directory],
       [
        gsl_header_path="-I$withval"
       ],
     [
    gsl_header_path="$GSL_CFLAGS"
     ])
AC_ARG_WITH(gsl_lib_path,
       [  --with-gsl-lib-path=PATH       Location of GSL lib],
       [
        gsl_lib_path="-L$withval"
       ],
     [
   gsl_lib_path="$GSL_LIBS"
     ])
GSL_INCLUDE="$gsl_header_path"
AC_SUBST(GSL_INCLUDE)
GSL_LIB="$gsl_lib_path"
AC_SUBST(GSL_LIB)

GSL_SUPPORT=
AC_ARG_ENABLE(gsl_lib,
	   [  --disable-gsl-lib
          Disable GSL. (Enabled by default)],
	   [ gsl_lib="no"
          AC_MSG_WARN("GSL support disabled")],
     [ gsl_lib="yes"
          AC_MSG_NOTICE("GSL support enabled")]
	   )
AM_CONDITIONAL([GSL_SUPPORT], [test "x$gsl_lib" = "xyes"])


PKG_CHECK_MODULES(XML, libxml-2.0 >= 2.4)

AC_ARG_WITH(libxml_header_path,
	   [  --with-libxml-header-path=PATH 		Location of libxml2 includes directory],
	   [
		libxml_header_path="-I$withval"
	   ],
     [
    libxml_header_path="$XML_CFLAGS"
     ])
AC_ARG_WITH(libxml_lib_path,
	   [  --with-libxml-lib-path=PATH 		Location of libxml2.a],
	   [
		libxml_lib_path="-L$withval"
	   ],
     [
    libxml_lib_path="$XML_LIBS"
     ])

LIBXML_INCLUDE="$libxml_header_path"
AC_SUBST(LIBXML_INCLUDE)
LIBXML_LIB="$libxml_lib_path"
AC_SUBST(LIBXML_LIB)

PKG_CHECK_MODULES(OPENSSL, openssl >= 1.0.0)

AC_ARG_WITH(libopenssl_header_path,
	   [  --with-libopenssl-header-path=PATH 		Location of openssl includes directory],
	   [
		libopenssl_header_path="-I$withval"
	   ],
     [
    libopenssl_header_path="$OPENSSL_CFLAGS"
     ])
AC_ARG_WITH(libopenssl_lib_path,
	   [  --with-libopenssl-lib-path=PATH 		Location of openssl],
	   [
		libopenssl_lib_path="-L$withval"
	   ],
     [
    libopenssl_lib_path="$OPENSSL_LIBS"
     ])
LIBSSL_INCLUDE="$libopenssl_header_path"
AC_SUBST(LIBSSL_INCLUDE)
LIBSSL_LIB="$libopenssl_lib_path"
AC_SUBST(LIBSSL_LIB)

have_netcdf=no
NETCDF_CONFIG=
AC_ARG_WITH(netcdf_path,
           [  --with-netcdf-path=PATH  Set the netcdf root dir],
           [
                netcdf_path="$withval"
		have_netcdf=yes
		NETCDF_CONFIG="${withval}/bin/nc-config"
           ],
           [
			AC_PATH_PROG([NETCDF_CONFIG], nc-config)
			if test -n "$NETCDF_CONFIG" ; then
				have_netcdf=yes
                	fi
           ]
           )
if test "x${have_netcdf}" = "xyes"; then
	NETCDF_CFLAGS="`$NETCDF_CONFIG --cflags`"
	NETCDF_LIBS="`$NETCDF_CONFIG --libs`"
	if test "x${NETCDF_LIBS}" != "x"; then
		AC_SUBST(NETCDF_CFLAGS)
		AC_SUBST(NETCDF_LIBS)
		AC_MSG_NOTICE([NetCDF support enabled])
	else
		AC_MSG_WARN([NetCDF not found! NetCDF support automatically disabled!])
		have_netcdf=no
	fi
fi
AM_CONDITIONAL([HAVE_NETCDF], [test "x$have_netcdf" = "xyes"])

have_esdm=no
ESDM_CFLAGS=
ESDM_LIBS=
AC_ARG_WITH(esdm_path,
           [  --with-esdm-path=PATH  Set the esdm root dir],
           [
				have_esdm=yes
				ESDM_CFLAGS="-I$withval/include"
				ESDM_LIBS="-L$withval/lib -lesdm -lesdmmpi -Wl,-rpath -Wl,$withval/lib"
				AC_SUBST(ESDM_CFLAGS)
				AC_SUBST(ESDM_LIBS)
				AC_MSG_NOTICE([ESDM support enabled])
           ],
           [
				have_esdm=no
           ]
           )
AM_CONDITIONAL([HAVE_ESDM], [test "x$have_esdm" = "xyes"])

#Check if ESDM PAV kernels are installed
have_esdm_pav_kernels="no"
AC_ARG_ENABLE(esdm_pav_kernels,
        [  --enable-esdm-pav-kernels
            Turn on ESDM PAV kernels],
        [have_esdm_pav_kernels="yes"
            AC_MSG_NOTICE(ESDM PAV kernels enabled)],
        [have_esdm_pav_kernels="no"
            AC_MSG_NOTICE(ESDM PAV kernels disabled)]
        )
AM_CONDITIONAL([HAVE_ESDM_PAV_KERNELS], [test "x$have_esdm_pav_kernels" = "xyes"])

#Check if parallel netcdf is installed
parallel_netcdf="no"
AC_ARG_ENABLE(parallel_netcdf,
        [  --enable-parallel-netcdf
            Turn on parallel NetCDF flags],
        [parallel_netcdf="yes"
            AC_MSG_NOTICE("Parallel NetCDF support enabled")],
        [parallel_netcdf="no"
            AC_MSG_WARN("Parallel NetCDF support disabled")]
        )
AM_CONDITIONAL([HAVE_PARALLEL_NETCDF], [test "x$parallel_netcdf" = "xyes"])

#Check if parallel esdm is installed
parallel_esdm="no"
AC_ARG_ENABLE(parallel_esdm,
        [  --enable-parallel-esdm
            Turn on parallel ESDM flags],
        [parallel_esdm="yes"
            AC_MSG_NOTICE("Parallel ESDM support enabled")],
        [parallel_esdm="no"
            AC_MSG_WARN("Parallel ESDM support disabled")]
        )
AM_CONDITIONAL([HAVE_PARALLEL_ESDM], [test "x$parallel_esdm" = "xyes"])

#Check if cfitsio is installed
cfitsio="no"
AC_ARG_ENABLE(cfitsio,
        [  --enable-cfitsio
            Turn on CFITSIO flags],
        [cfitsio="yes"
            AC_MSG_NOTICE("CFITSIO support enabled")],
        [cfitsio="no"]
        )
AM_CONDITIONAL([HAVE_CFITSIO], [test "x$cfitsio" = "xyes"])

if test "x${cfitsio}" = "xyes"; then

        PKG_CHECK_MODULES(CFITSIO, cfitsio >= 3.34)

        AC_ARG_WITH(libcfitsio_header_path,
                [  --with-libcfitsio-header-path=PATH            Location of libcfitsio includes directory],
                [
                        libcfitsio_header_path="-I$withval"
                ],
                [
                        libcfitsio_header_path="$CFITSIO_CFLAGS"
                ])
        AC_ARG_WITH(libcfitsio_lib_path,
                [  --with-libcfitsio-lib-path=PATH               Location of libcfitsio],
                [
                        libcfitsio_lib_path="-L$withval"
                ],
                [
                        libcfitsio_lib_path="$CFITSIO_LIBS"
                ])

        LIBCFITSIO_INCLUDE="$libcfitsio_header_path"
        AC_SUBST(LIBCFITSIO_INCLUDE)
        LIBCFITSIO_LIB="$libcfitsio_lib_path"
        AC_SUBST(LIBCFITSIO_LIB)
fi

#Check if curl is installed
curl="yes"
AC_ARG_ENABLE(curl,
        [  --disable-curl
            Turn off cURL flags (Enabled by default)],
        [curl="no"
            AC_MSG_NOTICE("cURL support disabled. Some operators will not be installed!")],
        [curl="yes"]
        )
AM_CONDITIONAL([HAVE_CURL], [test "x$curl" = "xyes"])

if test "x${curl}" = "xyes"; then

	PKG_CHECK_MODULES(CURL, libcurl >= 7.19)
	AC_ARG_WITH(libcurl_header_path,
		   [  --with-libcurl-header-path=PATH 		Location of libcurl includes directory],
		   [ libcurl_header_path="-I$withval" ],
		   [ libcurl_header_path="$CURL_CFLAGS"]
		  )
	AC_ARG_WITH(libcurl_lib_path,
		   [  --with-libcurl-lib-path=PATH 		Location of libcurl.a],
		   [ libcurl_lib_path="-L$withval" ],
		   [ libcurl_lib_path="$CURL_LIBS"]
		  )

	LIBCURL_INCLUDE="$libcurl_header_path"
	AC_SUBST(LIBCURL_INCLUDE)
	LIBCURL_LIB="$libcurl_lib_path"
	AC_SUBST(LIBCURL_LIB)
fi

#Enable stand-alone mode
standalone_mode="no"
AC_ARG_ENABLE(standalone_mode,
        [  --enable-standalone-mode
            Turn on standalone mode],
        [standalone_mode="yes"
            AC_MSG_WARN("Standalone mode enabled")],
        [standalone_mode="no" ]
        )
AM_CONDITIONAL([STANDALONE_MODE], [test "x$standalone_mode" = "xyes"])


PKG_CHECK_MODULES(JANSSON, jansson >= 2.3)
AC_ARG_WITH(jansson_header_path,
       [  --with-jansson-header-path=PATH        Location of Jansson includes directory],
       [
        jansson_header_path="-I$withval"
       ],
     [
    jansson_header_path="$JANSSON_CFLAGS"
     ])
AC_ARG_WITH(jansson_lib_path,
       [  --with-jansson-lib-path=PATH       Location of jansson lib],
       [
        jansson_lib_path="-L$withval"
       ],
     [
    jansson_lib_path="$JANSSON_LIBS"
     ])
JANSSON_INCLUDE="$jansson_header_path"
AC_SUBST(JANSSON_INCLUDE)
JANSSON_LIB="$jansson_lib_path"
AC_SUBST(JANSSON_LIB)
JANSSON_LIBDIR=`pkg-config --variable=libdir jansson`
AC_SUBST(JANSSON_LIBDIR)

ophidiaio_server_support=no
OPHIDIAIO_SERVER_PATH=
AC_ARG_WITH(ophidiaio_server_path,
           [  --with-ophidiaio-server-path=PATH   Set Ophidia IO server lib dir (Not available by default)],
           [
                ophidiaio_server_path="$withval"
                ophidiaio_server_support=yes
           ],
           [
                AC_MSG_NOTICE("Ophidia IO server support disabled.")
           ]
           )
if test "x${ophidiaio_server_support}" = "xyes"; then
        if test "x${ophidiaio_server_path}" != "x"; then
                OPHIDIAIO_SERVER_CFLAGS="-I${ophidiaio_server_path}/include"
                OPHIDIAIO_SERVER_LIBS="-L${ophidiaio_server_path}/lib"
                AC_SUBST(OPHIDIAIO_SERVER_CFLAGS)
                AC_SUBST(OPHIDIAIO_SERVER_LIBS)
                AC_MSG_NOTICE("Ophidia IO server support enabled.")
        else
                AC_MSG_ERROR("Ophidia IO server install dir not found!")
                ophidiaio_server_support=no
        fi
fi
AM_CONDITIONAL([OPHIDIAIO_SERVER_SUPPORT], [test "x$ophidiaio_server_support" = "xyes"])

AC_ARG_WITH(interface_type,
	   [  --with-interface-type=TYPE 		Interface type: SSL],
	   [
		interface_type="$withval"
	   ],
	   [
		interface_type="ssl"
	   ])
if test "x${interface_type}" = "xssl"; then
INTERFACE_TYPE=$interface_type
elif test "x${interface_type}" = "xgsi"; then
INTERFACE_TYPE=$interface_type
else
AC_MSG_ERROR(Unsupported interface type)
fi
AC_SUBST(INTERFACE_TYPE)
AM_CONDITIONAL([INTERFACE_TYPE_IS_SSL], [test "x$interface_type" = "xssl"])
AM_CONDITIONAL([INTERFACE_TYPE_IS_GSI], [test "x$interface_type" = "xgsi"])

AC_ARG_WITH(globus-libs,
                                [  --with-globus-libs=GLOBUS_LIB_LOCATION   Set the Globus library location],
                                [GLOBUS_LIB="$withval"]
                                )
AC_SUBST(GLOBUS_LIB)
if test -n $GLOBUS_LIB; then
        AC_MSG_NOTICE([Globus library in: $GLOBUS_LIB])
fi

AC_ARG_WITH(globus-include,
                                [  --with-globus-include=GLOBUS_INCLUDE_LOCATION   Set the Globus header location],
                                [GLOBUS_INCLUDE="$withval"]
                                )
AC_SUBST(GLOBUS_INCLUDE)
if test -n $GLOBUS_INCLUDE; then
       AC_MSG_NOTICE([Globus includes: $GLOBUS_INCLUDE])
fi

#Enable VOMS
voms_enable=0
AC_ARG_ENABLE(voms,
            [  --enable-voms  Enables support for VOMS on GLITE],
                     [voms_enable=1],
                     [voms_enable=0])
VOMS=$voms_enable
AC_SUBST(VOMS)

#Set VOMS libs path
AC_ARG_WITH(voms-libs,
            [  --with-voms-libs=VOMS_LIBS       Sets the VOMS libraries path],
                    [
                             VOMS_LIBS="$withval"
                    ]
            )
if test -z "$VOMS_LIBS" && test $voms_enable = 1; then
        AC_MSG_ERROR([no VOMS libraries directory found])
else
	AC_MSG_NOTICE([VOMS libraries directory: $VOMS_LIBS])
	VOMS_DEFINE="GLITE_VOMS"
fi
AC_SUBST(VOMS_LIBS)
AC_SUBST(VOMS_DEFINE)

#Set VOMS include path
AC_ARG_WITH(voms-include,
            [  --with-voms-include=VOMS_INCLUDE       Sets the VOMS include path],
                    [
                             VOMS_INCLUDE="$withval"
                    ]
            )
if test -z "$VOMS_INCLUDE" && test $voms_enable = 1; then
        AC_MSG_ERROR([no VOMS include directory found])
else
	AC_MSG_NOTICE([VOMS includes: $VOMS_INCLUDE])
fi
AC_SUBST(VOMS_INCLUDE)

# Set servers default listening port number
number_port=11732
AC_ARG_WITH(listening_port,
	   [  --with-plugin-default-port=NUMBER     Set servers default listening  port number (default: 11732)],
	   [
	    number_port="$withval"
           ]
           )
PLUGIN_DEFAULT_PORT=$number_port
AC_SUBST(PLUGIN_DEFAULT_PORT)

plugin_host_path="no"
AC_ARG_WITH(plugin_hostname,
	   [  --with-plugin-hostname=HOSTNAME       Set plugin server FQDN],
	   [
	    plugin_host_path="$withval"
           ]
           )
if test "$plugin_host_path" = "no"; then
	   PLUGIN_DEFAULT_HOSTNAME=$HOSTNAME
	   else
           PLUGIN_DEFAULT_HOSTNAME=$mds_host_path
fi
AC_SUBST(PLUGIN_DEFAULT_HOSTNAME)

OPT="-Wno-error -Wno-format-security"
case "${host}" in
        *-*-solaris*)   PLATFORM=SUN_OS
                        SAMPLE_EXTRA_LIBS="-lxnet -lsocket -lnsl"
                        ;;
        *-*-sysv5*)     PLATFORM=UNIXWARE ;;
        *-*-UnixWare*)  PLATFORM=UNIXWARE ;;
        *-*-unixware*)  PLATFORM=UNIXWARE ;;
        *-*-sco3.2v5*)  PLATFORM=OPENSERVER
                        SAMPLE_EXTRA_LIBS="-lsocket"
                        ;;
        *-*-linux*)     PLATFORM=LINUX
                                    if (test "x$debug" = "x1") || (test "x$debug" = "x2"); then
										OPT+=" -g3 -Wall -Wextra -O0"
									else
										OPT+=" -O3"
									fi
			            if test "x$optimization" = "xyes" ; then
			                 CFLAGS="-O3 -finline-functions -funswitch-loops -fgcse-after-reload -ftree-loop-linear -ftree-loop-im -ftree-loop-ivcanon -fivopts -ftree-vectorize -ftracer -fgcse-sm -fgcse-las -fgcse-after-reload -funroll-loops -freorder-blocks-and-partition -fweb"
			            fi
			            ;;
                *-*-cygwin*)    PLATFORM=CYGWIN
                        ;;
        *-*-freebsd*)   PLATFORM=FREEBSD ;;
        *-*-irix*)      PLATFORM=IRIX
                        ;;
        *-*-aix*)       PLATFORM=AIX
                      case "${host}" in
                         *-*-aix4.1*)   osver=AIX41;;
                         *-*-aix4.2*)   osver=AIX42;;
                         *-*-aix4.3*)   osver=AIX43;;
                         *-*-aix4.*)    osver=AIX43;;
                      esac;;
        *-sequent-*)    PLATFORM=PTX ;;
        *-*-hp*)        PLATFORM=HP_UX ;
                        case "${host}" in
                            *-*-hpux11*)  osver=HPUX11;;
                            *-*-hpux10*)  osver=HPUX10;;
                            *)            osver=HPUX11;;
                        esac;;
        *-*-mvs*)       PLATFORM=OS390 ;;
        *-*-os400*)     PLATFORM=OS400 ;;
        *-*-OS400*)     PLATFORM=OS400 ;;
        *-*-osf*)       PLATFORM=TRU64
				        OPT+=" -g3"
				        REENTRANT="1"
                               ;;
        *-apple-*)      PLATFORM=__APPLE__
				        OPT+=" -g3"
				        ;;
        *)              PLATFORM=UNKNOWN ;;
esac

AC_SUBST(SAMPLE_EXTRA_LIBS)
AC_SUBST(PLATFORM)
AC_SUBST(OPT)
AC_SUBST(REENTRANT)

AM_CONDITIONAL(VOMS, test $voms_enable = 1)

if test "x${have_esdm_pav_kernels}" = "xyes" && test -d "$srcdir/esdm-pav-analytical-kernels"; then
    AC_CONFIG_SUBDIRS([esdm-pav-analytical-kernels])
fi

AC_OUTPUT([Makefile etc/oph_configuration src/Makefile src/oph_gsoap/Makefile src/clients/Makefile src/ioservers/Makefile src/drivers/Makefile etc/oph_analytics_driver etc/oph_io_server])
