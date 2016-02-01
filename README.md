<h1>Ophidia Analytics Framework</h1>

<h3>Description</h3>
The Ophidia Analytics Framework is an efficient big data analytics framework.</br>
It includes parallel operators for data analysis and mining (subsetting, reduction, metadata processing, etc.) that can run over a cluster.</br>
The framework is fully integrated with Ophidia Server: it receives commands from the server and sends back notifications so that workflows can be executed efficiently.

<h3>Requirements</h3>
In order to compile and run the Ophidia Analytics Framework, make sure you have the following packages (all available through CentOS official repositories and the epel repository) properly installed:
<ol>
  <li>mpich, mpich-devel and mpich-autoload</li>
  <li>jansson and jansson-devel</li>
  <li>libxml2 and libxml2-devel</li>
  <li>libssh2 and libssh2-devel</li>
  <li>openssl and openssl-devel</li>
  <li>mysql-community-server</li>
  <li>nectdf and netcdf-devel</li>
  <li>globus-common-devel (only for GSI support)</li>
  <li>globus-gsi-credential-devel (only for GSI support)</li>
  <li>globus-gsi-proxy-core-devel (only for GSI support)</li>
  <li>globus-gssapi-gsi-devel (only for GSI support)</li>
  <li>voms-devel (only for GSI support)</li>
</ol>
<b>Note</b>:</br>
This product includes software developed by the OpenSSL Project for use in the OpenSSL Toolkit.

<h3>How to Install</h3>
The source code has been packaged with GNU Autotools, so look at the INSTALL file or simply type:</br></br>
<code>
./configure</br>
make</br>
make install</br></br></code>
Type:</br></br>
<code>./configure --help</code></br></br>
to see all available options, like --prefix for explicitly specifying the installation base directory.</br>
If you want to use the program system-wide, remember to add its installation directory to your PATH.</br>

<h3>How to Launch</h3>
<code>oph_analytics_framework "operator=value;param=value;..."</code></br></br>

Further information can be found at <a href="http://ophidia.cmcc.it/documentation">http://ophidia.cmcc.it/documentation</a>.

