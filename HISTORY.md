
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
