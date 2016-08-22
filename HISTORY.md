
## v0.10.4 - 2016-08-22

### Added:

- Add BASE_SRC_PATH to configuration parameters

### Changed:

- Pre-processing of src_path of OPH_IMPORTNCx
- OPH_SCRIPT in order to execute only pre-registered scripts

## v0.10.3 - 2016-07-26

### Fixed

- Bug in setting metadata keys when measure is changed by the operator

## v0.10.2 - 2016-07-19

### Fixed

- Bug in export metadata when more cores are used in OPH_EXPORTNC2
- Several warnings when building

### Added:

- Support for base64 encoding in OPH_CUBESCHEMA and OPH_EXPLORECUBE
- Support for selection statement

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
