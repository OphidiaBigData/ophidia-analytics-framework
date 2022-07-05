
## Next release

### Added:

- Add XML description of 'oph_filter' primitive [#108](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/108)

## v1.7.0 - 2022-07-01

### Fixed:

- Bug in evaluating 'nhost' automatically for import operators
- Bug in processing input parameters of importnc operators
- Optimization to write output data by OPH_EXPORTNC2
- Setting of unlimited dimension parameter for the I/O server
- Detection of time dimension in case of subsetting for OPH_IMPORTNC and OPH_CONCATNC

### Added:

- Include the package ESDM-PAV kernels [#106](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/106)
- OPH_CONCATESDM2 operator [#105](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/105)
- ESDM-based operators: OPH_IMPORTESDM, OPH_IMPORTESDM2, OPH_CONCATESDM, OPH_EXPORTESDM, OPH_EXPORTESDM2 [#104](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/104)
- OPH_IMPORTNCS operator to load multiple files at once [#104](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/104)
- OPH_INTERCUBE2 operator [#103](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/103)
- Add support for compression in export operators [#102](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/102)

### Changed:

- Improved data I/O functionalities [#104](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/104)
- Parsing of the parameter 'base_time' from NetCDF attirbutes
- OPH_METADATA to update metadata "by key" besides "by id"
- New parameter 'execute' to OPH_RESUME operator
- New subset parameters to OPH_WAIT operator
- New 'measure' ans aubset parameters to OPH_FS operator

## v1.6.1 - 2021-08-03

### Fixed:

- Skip import/export of metadata 'bounds' and '_NCProperties' [#99](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/99)
- Some memory leaks in oph_datacube_library

### Removed:

- OPH_SUBSET2 from operator list [#98](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/98)

### Changed:

- 'missingvalue' argument to read the related metadata value by default [#101](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/101)
- Operators XML descriptions with argument 'save' for saving the JSON response [#100](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/100)

## v1.6.0 - 2021-03-05

### Fixed:

- Bug in parsing multivalue arguments
- Bug in setting _Fillvalue in exportnc operators
- Bug in setting total fragment number in OPH_IMPORTNC2
- Framework core functions to support longer input strings for operators [#94](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/94)
- Bug when inserting new dimension if another one with the same name already exists [#90](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/90)
- Bug in selection of default host partition [#89](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/89)
- Bug in assign elements to groups to be reduced by OPH_REDUCE2 in case new concept level is '3 hour', '6 hour' or 'quarter'
- Bug [#84](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/84) when querying OphidiaDB
- Bug [#83](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/83)
- Bug [#82](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/82)

### Added:

- Signal handler for the client
- Argument 'exec_mode' to OPH_WAIT
- New values '365_day' and '366_day' for argument 'calendar'
- New argument 'action' to OPH_B2DROP to support also file download [#95](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/95)
- Creation date of host partitions to output of OPH_CLUSTER  [#93](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/93)
- New parameter in import and randcube operators to set node selection policy [#92](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/92)
- New argument 'space' to OPH_SCRIPT [#91](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/91)
- Configuration option ENABLE_UNREGISTERED_SCRIPT to allow unregistered scripts [#87](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/87)
- Support for new types of date formats in dimensions
- New argument 'order' to mergecubes operators [#86](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/86)
- Support to enable/disable users using OPH_SERVICE [#85](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/85)
- New argument 'cubes' to OPH_INTERCUBE

### Changed:

- Default output path of OPH_EXPORTNC and OPH_EXPORTNC2 set to CDD
- Setting of output name for OPH_EXPORTNC and OPH_EXPORTNC2
- OPH_IMPORTNC2 to exploit block size upon nc file opening [#97](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/97)
- OPH_EXPLORECUBE to remove row limit in case of base64 option is used [#96](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/96); it also fixes [#62] (https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/62)
- Interface of OPH_SET and OPH_INPUT to allow 0 as a value for the 'id' parameter
- OPH_INSTANCE operator when option 'auto' is used for argument 'host_partition'
- Time hierarchy to support concept level 'c' as 'second' [#88](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/88)

### Removed:

- Error log line printed when time dimension check fails in case a new container has to created during the execution of OPH_IMPORTNC or OPH_IMPORTNC2


## v1.5.1 - 2019-04-16

### Fixed:

- Bug [#81](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/81) related to the use of a new container for the output datacube 
- Bug [#80](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/80) in OPH_INTERCUBE operator 
- Bug [#79](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/79) in OPH_MERGE operator 
- Bug [#78](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/78) when thread number is smaller than host number

### Added:

- New parameter 'recursive' to OPH_TASKS and OPH_SEARCH

### Changed:

- Default value of parameter 'action' in OPH_MANAGE_SESSION


## v1.5.0 - 2019-01-24

### Fixed:

- Usage of MySQL client library in operators and libraries in case of multi-thread execution
- Warnings when building on Ubuntu 18
- Data operators to return the proper code when global errors occur [#64](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/64)
- Enforced UNIQUE constraints in several OphidiaDB tables [#61](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/61)
- Bug [#60](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/60) related to OphidiaDB tables lock
- Minor bugs in oph_nc_library 
- Bug in multi-thread operators using mysql IO server type [#56](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/56)

### Added:

- New operators OPH_CONCATNC and OPH_CONCATNC2  [#75](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/75)
- New operator OPH_RANDCUBE2 [#70](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/70)
- XML for new primitives: oph_affine [#68](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/68),   oph_arg_max_array and oph_arg_min_array [#66](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/66) 
- New options 'arg_max' and 'arg_min' to  OPH_INTERCUBE [#66](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/66) 
- New argument 'container_pid' to OPH_DELETECONTAINER [#63](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/63)

### Changed:

- Soap interface files with gSOAP version 2.8.76 [#77](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/77)
- Default value of argument nhost of OPH_CLUSTER
- XML of OPH_CLUSTER with new actions for cluster information [#74](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/74)
- OphidiaDB to address better scalability for node selection queries and all multi-thread operators in order to limit number of connections to OphidiaDB [#73](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/73)
- XML of predicate primitives to consider 'INDEX' value [#72](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/72)
- Multi-threaded operators to adjust nthreads when bigger than fragments available [#71](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/71)
- Extended OPH_RANDCUBE with a new algorithm [#70](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/70)
- XML of OPH_SET with new arguments to specify subsets [#69](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/69) 
- Exportnc operators to add cube identifier as suffix of file names [#67](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/67) 
- Extended OPH_CUBESIZE to estimate the size without performing the computation [#65](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/65)
- Checks in OPH_DELETE operator to support deletion of datacubes even when I/O nodes are not available

### Removed:

- Support for hidden containers and drop OPH_RESTORECONTAINER operator [#76](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/76)
- 'ndb', 'ndbms' and 'fs_type' arguments from various operators [#73](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/73)


## v1.4.0 - 2018-07-27

### Fixed:

- Bug in OPH_EXPORTNC2 [#55](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/55)
- Operators to set JSON reponse only after global error check [#45](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/45)
- Return message printed in log of OPH_SCRIPT
- Bug in usage of 'unlimited' property for dimensions [#44](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/44)
- Submissing string check in case of long strings
- Bug in setting 'dim_name' of OPH_MERGECUBES2

### Added:

- New operator OPH_B2DROP [#53](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/53)
- 'dim_type' argument to OPH_MERGECUBES2 operator [#50](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/50)
- new environment variables to OPH_SCRIPT operator (OPH_SCRIPT_SERVER_HOST, OPH_SCRIPT_SERVER_PORT and OPH_SCRIPT_USER) [#49](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/49)
- Support to extract seasons with OPH_SUBSET and OPH_EXPLORECUBE operators [#48](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/48)
- Summary info to OPH_INSTANCES [#47](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/47)

### Changed:

- several operators and roll-back procedure to support multi-threaded execution [#54](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/54)
- OPH_DELETECONTAINER operator in order to delete also non-empty containers [#20](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/20)
- Exportnc operators to support 'local' keyword for 'output_path' argument 
- Extend OPH_CUBESCHEMA operator to compute number of cube elements [#52](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/52)
- Extend OPH_EXPORTNC2 to allow also postponed metadata saving [#51](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/51)
- Extend operators associated with of oph_normalize
- Extend values of argument 'comparison' of oph_predicate [#46](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/46) [#43](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/43)
- XML description of OPH_CLUSTER
- Remove constraint for hostpartition.idjob in order to enable dynamic clustering

## v1.3.0 - 2018-06-18

### Fixed:

- Bug in handling idjob for massive operations
- Link to output of OPH_SCRIPT in session space
- OphidiaDB cube library to allow exploration of cubes without dimensions
- Code building when standalone configuration option is enabled [#30](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/30)
- OPH_APPLY operator to avoid updating dimension metadata in case the array size is not changed

### Added:

- New operator OPH_IMPORTNC2 [#42](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/42)
- Functionalities to manage reserved host partitions [#40](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/40)
- XML file of OPH_CLUSTER  [#40](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/40)
- XML for new primitive oph_sequence [#35](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/35)
- New operator OPH_CONTAINERSCHEMA [#34](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/34)

### Changed:

- Definition of OphidiaDB 'hostpartition', 'hashost' and 'job' tables
- OPH_REDUCE2 operator and some common libraries to support multi-threaded execution [#42](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/42)
- Job tracking for accounting purposes [#41](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/41)
- OPH_INSTANCES operator to manage user-defined host partitions [#39](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/39)
- OPH_IMPORTNC operator to allow different number of fragments per DB [#38](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/38)
- OPH_METADATA operator to filter also on variable name [#37](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/37)
- XML of oph_gsl_quantile primitive [#36](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/36)
- OPH_CUBESCHEMA operator to also add dimensions [#33](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/33)
- OPH_APPLY operator to update dimension values according to the new parameter 'on_reduce'

## v1.2.0 - 2018-02-16

### Fixed:

- Improve metadata management to prevent deadlocks [#22](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/22)
- Drop old reference to autocommit
- Bug in handling parameter 'realpath' of OPH_FS
- Dimension type check in OPH_IMPORTNC
- Change auto-setting of concept level to monthly
- Bug [#17](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/17)
- XML description of OPH_IMPORTFITS

### Added:

- Add support for rollback in case of errors [#27](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/27)
- Add optional flag for grid check [#26](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/26)
- New primitive oph_normalize [#25](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/25)
- New primitive oph_replace [#23](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/23)
- Fields 'category' and 'multivalue' to XML descriptions
- Calendar '365_day'
- New primitive oph_padding
- New options to handle directories to OPH_FS
- Option OPH_CDD to OPH_GET_CONFIG
- Print input command and output in text log of OPH_SCRIPT

### Changed:

- OphidiaDB to count number of data cubes managed by the hosts [#29](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/29)
- Allow to import data without dimension values [#28](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/28)
- Extend subset_types to allow index/coord per dimension [#24](https://github.com/OphidiaBigData/ophidia-analytics-framework/pull/24)
- Change VFS category name
- Relax error on folder creation in OPH_EXPORTNC
- Allow to merge one cube with OPH_MERGECUBES
- Adapt OPH_SCRIPT to include stdout and stderr in JSON Response
- Remove check for maximum query size from OPH_MERGECUBE
- Queries of OPH_SEARCH

## v1.1.0 - 2017-07-28

### Fixed:

- Bug in OPH_IMPORTNC to re-use the same default container when no name is specified
- Bug in selecting DBMS instance with OPH_PRIMITIVE_LIST
- Some bugs in OPH_RANDCUBE, OPH_MERGECUBES, OPH_INTERCUBE, OPH_MERGECUBES2, OPH_METADATA and OPH_SEARCH
- OPH_SCRIPT behavior in case session folder is not available
- Some warnings
- Bug [#15](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/15)
- Bug [#14](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/14)
- Bug in strncpy calls

### Added:

- OPH_IMPORTFITS new operator (INDIGO-DataCloud Project)
- Level 0 option to OPH_EXPLORENC to read file attributes, dimensions and variable lists
- sessionid and markerid to framework notification messages
- Entry in table 'task' for OPH_IMPORTNC and OPH_RANDCUBE
- 'hold_values' and 'number' parameters in OPH_MERGECUBES operator
- Parameter 'forward' to OPH_IF 
- XML files for new primitives: oph_concat2, oph_append
- Multi-user mode and USER_SPACE option in several operators
- arg_max and arg_min options in reduction operators
- Add current data directory (cdd) parameter to several operators
- OPH_FS new operator
- subet_type argument to OPH_SUBSET and OPH_EXPLORECUBE
- Some alias for known calendars

### Changed:

- OPH_RESUME operator to allow workflow progress monitoring
- OPH_IMPORTNC operator to allow import of files without dimension values and to support a more configurable fragmentation
- OPH_MERGECUBES to allow appending time series with different sizes
- Disable framework from saving JSON Response
- OPH_SUBSET2 operator is now deprecated

## v1.0.0 - 2017-03-23

### Fixed:

- Typos in OPH_EXPLORENC, OPH_MERGECUBES and OPH_MERGECUBES2 XML files
- Subset library to handle large filters
- Bug in evaluating monthly reductions

### Added:

- 'week' concept level to oph_time hierarchy

### Changed:

- Library name from oph_datacube2_library to oph_datacube_library
- Default value of 'operation' argument in OPH_INTERCUBE to 'sub' 
- Default value of 'compressed' argument in OPH_RANDCUBE to 'no' 
- Operator name from OPH_IMPORTNC3 to OPH_IMPORTNC
- Code indentation style

### Removed:

- Operator OPH_INTERCOMPARISON

## v0.11.0 - 2017-01-31

### Fixed:

- Bug regarding metadatakey table update on cube deletion
- Memory leak in OPH_APPLY
- Bug [#9](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/9)
- Example in OPH_INSTANCE xml file
- Bug [#7](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/7)
- Bug [#5](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/5)

### Added:

- Max and min operations to OPH_INTERCUBE operator
- Argument 'status_filter' to OPH_RESUME operator XML file
- Support for missing values to several operators
- Support for base64-encoded dimensions [#10](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/10)
- Support for missing values to several primitives XML descriptions
- Support for Ophidia native IO server

### Changed:

- OPH_CUBESIZE query to compute data fragment size
- OPH_EXPORTNC2 to handle multiple row selection and export
- XML descriptions of the primitives with two input measures
- OPH_INTERCUBE operator to allow comparison of cubes stored on different IO nodes
- OPH_SERVICE xml file for listing running tasks
- Configuration parameters names in oph_configuration
- Query used in exportnc operators to optimize retrieval from IO server
- Multi-insert query to optimize operations on IO server side
- OPH_APPLY query used to count fragment rows

### Removed:

- oph_dim_configuration file (unified with oph_configuration)
- OPH_IMPORTNC
- OPH_IMPORTNC2

## v0.10.7 - 2016-11-15

### Fixed

- Useless logging message in OPH_EXPORTNC
- Bug [#8](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/8)

### Changed:

- Adapt storage library functions to handle fixed host partition
- Enable BASE_SRC_PATH as prefix of output paths for OPH_EXPORTNC

## v0.10.6 - 2016-10-20

### Fixed

- Bug in OPH_MERGE operator
- Warnings when building

### Added:

- Argument to define type of operation in OPH_CANCEL XML file
- Argument to define subset offset in OPH_IMPORTNCx and OPH_SUBSETx operators 
- XML files for: OPH_INPUT and OPH_WAIT (INDIGO-DataCloud Project)
- Support for time-based subsetting in OPH_IMPORTNCx operators

### Changed:

- OPH_STATUS_START_ERROR to OPH_STATUS_RUNNING_ERROR
- OPH_FOR and OPH_SET input parameter "name" to "key" 
- Constraints on bounds when selecting part of a NC variable

## v0.10.5 - 2016-08-24

### Fixed

- OPH_SUBSET2 and OPH_EXPLORECUBE to handle empty cubes
- Bug [#6](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/6)
- Memory leak in OPH_EXPORTNC2
- Bug in setting metadata keys when measure is changed by the operator
- Bug in export metadata when more cores are used in OPH_EXPORTNC2
- Several warnings when building

### Added:

- Priority field to select the hosts to be used for cube creation
- base folder (BASE_SRC_PATH) for files admitted to be imported
- Support for base64 encoding in OPH_CUBESCHEMA and OPH_EXPLORECUBE
- Support for selection statement (INDIGO-DataCloud Project)

### Changed:

- Pre-processing of src_path of OPH_IMPORTNCx
- OPH_SCRIPT in order to execute only pre-registered scripts

## v0.10.1 - 2016-06-27

### Fixed:
 
- Version number in files

## v0.10.0 - 2016-06-23

### Fixed:

- Bug in OPH_APPLY: check of explicit dimension size in case of unbalanced input cubes
- Bug in OPH_CUBESCHEMA: original data types were shown for dimensions even after a partial reduction
- Typos in XML files of OPH_SCRIPT and OPH_EXTEND
- Bug in copying metadata from input to output cube
- Typos in some primitives XML files for long data type
- Issue [#4](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/4)
- Bug [#3](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/3)
- Bug in file matching during directory scanning
- Bug [#2](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/2)
- Bug [#1](https://github.com/OphidiaBigData/ophidia-analytics-framework/issues/1)

### Added:

- New operator OPH_MERGECUBES2
- New argument OPH_SCRIPT_SESSION_CODE in OPH_SCRIPT operator
- Support for building on CentOS7 and Ubuntu
- New feature to add description to data cube
- XML files for new primitives: oph_predicate2, oph_extend, oph_expand
- New option oph_count in OPH_AGGREGATE operators
- Support for numeric data types in metadata

### Changed:

- Adapted behavior of OPH_MERGECUBES in order to create a new implicit dimension

## v0.9.0 - 2016-02-01

- Initial public release
