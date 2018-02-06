
## v1.2.0 - 2018-02-06

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
