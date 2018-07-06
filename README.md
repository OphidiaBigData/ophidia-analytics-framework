# Ophidia Analytics Framework

### Description

The Ophidia Analytics Framework is an efficient big data analytics framework. 
It includes parallel operators for data analysis and mining (subsetting, reduction, metadata processing, etc.) that can run over a cluster.
The framework is fully integrated with Ophidia Server: it receives commands from the server and sends back notifications so that workflows can be executed efficiently.

### Requirements

In order to compile and run the Ophidia Analytics Framework, make sure you have the following packages (all available through CentOS official repositories and the epel repository) properly installed:

1. mpich, mpich-devel and mpich-autoload
2. jansson and jansson-devel
3. libxml2 and libxml2-devel
4. libssh2 and libssh2-devel
5. openssl and openssl-devel
6. mysql-community-server
7. nectdf and netcdf-devel
8. globus-common-devel (only for GSI support)
9. globus-gsi-credential-devel (only for GSI support)
10. globus-gsi-proxy-core-devel (only for GSI support)
11. globus-gssapi-gsi-devel (only for GSI support)
12. voms-devel (only for GSI support)

**Note**:
This product includes software developed by the OpenSSL Project for use in the OpenSSL Toolkit.

### How to Install

If you are building from git, you also need automake, autoconf, libtool, libtool-ltdl and libtool-ltdl-devel packages. To prepare the code for building run:

```
$ ./bootstrap 
```

The source code has been packaged with GNU Autotools, so to install simply type:

```
$ ./configure --prefix=prefix
$ make
$ make install
```

Type:

```
$ ./configure --help
```

to see all available options.

If you want to use the program system-wide, remember to add its installation directory to your PATH.

### How to Launch

```
$ oph_analytics_framework "operator=value;param=value;..."
```

Further information can be found at [http://ophidia.cmcc.it/documentation/admin/](http://ophidia.cmcc.it/documentation/admin/).
