# D2RQ/Update and D2R/Update Server
NOT SUPPORTED. Developed during my M.Sc. studies in 2010.

# Prototype extensions of D2RQ and D2R Server for SPARQL/Update

The popular open source RDB to RDF mapping platform - D2RQ provides only a read-only RDF view on relational databases. It enables executing SPARQL queries on data in relational databases, mapped to RDF. The goal of D2RQ/Update is to enable executing SPARQL/Update statements, such as INSERT and DELETE, on the mapped data. It is a prototype extension to D2RQ platform that can be used for demonstrations and experimenting with Semantic Web technologies. It was tested only with MySQL and only on the example database and mapping that appear in the D2RQ Mapping Language .

The work is done as part of my M.Sc. studies at Department of Computer Science, Technion - Israel Institute of Technology, Haifa, Israel, under supervision of Dr. Yaron Kanza. The work on D2RQ/Update was largely influenced by the paper of M. Hert, G. Reif and H. C. Gall "Updating Relational Data via SPARQL/Update" .

D2RQ/Update uses the same mapping used by D2RQ, no additional information is required. The extension takes into account the database constraints, such as primary/foreign keys and non-nullable columns. D2RQ/Update uses constraints information from the database schema and tries to operate according to the constraints, in particular, to return meaningful errors in case of constraints violation. The algorithm, that describes the D2RQ/Update operation, is here.

Currently, the version of SPARQL/Update from 22 October 2009 is used (with MODIFY keyword).

# Publications

Poster to be presented on WWW2012, April 16-21, Lyon

# Build D2R/Update Server

Build [D2R/Update Server](https://github.com/VadimEisenberg/d2rUpdateServer) (Apache License 2.0).

D2RQ/Update uses JSQLParser, (LGPL license and Apache License 2.0) that can be downloaded from https://github.com/JSQLParser/JSqlParser. Just copy jsqlparser.jar to the lib directory inside the root directory of D2R/Update Server
D2RQ/Update uses JGraphT, (LGPL license) that can be downloaded from http://www.jgrapht.org/ . Just copy the jgrapht-jdk1.6.jar to the lib directory inside the root directory of D2R/Update Server.

# License
The code is under Apache License 2.0. In order to build the code, several jars are needed, as described in the [Download D2R/Update Server section](#download-d2rupdate-server)

See [LICENSES.txt](https://github.com/VadimEisenberg/d2rqUpdate/blob/master/LICENSES.txt)

# Source Code
[D2RQ/Update](https://github.com/VadimEisenberg/d2rqUpdate)

[D2R/Update Server](https://github.com/VadimEisenberg/d2rUpdateServer)

# Tests

The tests of D2Q/Update are [here](https://github.com/VadimEisenberg/d2rqUpdateDBUnitTests). In addition to the LGPL jars specified in the Download D2R/Update Server section, the tests use a JUnit extension - DBUnit.

# Tutorial

The tutorial that explains how to install and run D2RQ/Update, including example database, mapping and SPARQL/Update statements is [here](http://vadimeisenberg.github.io/d2rqUpdate/tutorial.html).

# Support

No support is provided.
