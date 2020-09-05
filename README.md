# Presto Gateway
Presto Gateway is a Policy based query router for PrestoDB/PrestoSQL query. 
It sits in front of multiple or single presto clusters and becomes the interface for users. 
It can be used as a Load Balancer, to achieve high availability  as well as a proxy. Its support for secure Presto clusters as well
as user authentication makes it fit for Production environment. 

There are several advantages of using Presto Gateway:
- It also provides a single UI interface for all the queries executed across the clusters.
- Ensuring High Availability in case of planned as well as accidental downtime.
- Load balance your clusters using rules which balances the load across the cluster.
- Having Rule based approach can also help in giving priority to a particular cluster. 


See the 
[REST API](https://github.com//current/) for rest API documentation\n
[DOCKER API](https://github.com//current/) for Docker Deployment
[DOCKER API](https://github.com//current/) for SSL Settings

## Requirements

* Mac OS X or Linux
* Java 11, 64-bit
* Python 2.6+ (for running with the launcher script)

## Building Presto

Presto is a standard Maven project. Simply run the following command from the project root directory:

    ./mvnw clean install

On the first build, Maven will download all the dependencies from the internet and cache them in the local repository (`~/.m2/repository`), which can take a considerable amount of time. Subsequent builds will be faster.

Presto has a comprehensive set of unit tests that can take several minutes to run. You can disable the tests when building:

    ./mvnw clean install -DskipTests

## Running PrestoGateway in your IDE

### Overview

After building Presto Gateway for the first time, you can load the project into your IDE and run the server. We recommend using [IntelliJ IDEA](http://www.jetbrains.com/idea/). Because Presto is a standard Maven project, you can import it into your IDE using the root `pom.xml` file. In IntelliJ, choose Open Project from the Quick Start box or choose Open from the File menu and select the root `pom.xml` file.

After opening the project in IntelliJ, double check that the Java SDK is properly configured for the project:

* Open the File menu and select Project Structure
* In the SDKs section, ensure that JDK 11 is selected (create one if none exist)
* In the Project section, ensure the Project language level is set to 8 (Presto does not yet use Java 11 language features)

Presto comes with sample configuration that should work out-of-the-box for development. Use the following options to create a run configuration:

* Main Class: `io.prestosql.gateway.PrestoGateway`
* VM Options: `-ea -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -Xmx2G -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties -Djdk.attach.allowAttachSelf=true`
* Working directory: `$MODULE_DIR$`
* Use classpath of module: `presto-gateway-main`

The working directory should be the `presto-gateway-main` subdirectory. In IntelliJ, using `$MODULE_DIR$` accomplishes this automatically.
   
Additionally, the meta store must be configured with the location of your PostgreSQL instance. The configuration
for meta store can be provided in the file hikaricp.properties:
    
    minimumIdle=5
    connectionTestQuery=SELECT 1
    autoCommit=true
    maximumPoolSize=10
    jdbcUrl=jdbc:postgresql://127.0.0.1:5432/prestogateway
    username=prestogateway
    password=root123
    driverClassName=org.postgresql.Driver

## Development

### Code Style

We recommend you use IntelliJ as your IDE. The code style template for the project can be found in the [codestyle](https://github.com/airlift/codestyle) repository along with our general programming and Java guidelines. In addition to those you should also adhere to the following:

* Alphabetize sections in the documentation source files (both in the table of contents files and other regular documentation files). In general, alphabetize methods/variables/sections if such ordering already exists in the surrounding code.
* When appropriate, use the stream API. However, note that the stream implementation does not perform well so avoid using it in inner loops or otherwise performance sensitive sections.
* Categorize errors when throwing exceptions. For example, PrestoException takes an error code as an argument, `PrestoException(HIVE_TOO_MANY_OPEN_PARTITIONS)`. This categorization lets you generate reports so you can monitor the frequency of various failures.
* Ensure that all files have the appropriate license header; you can generate the license by running `mvn license:format`.
* Consider using String formatting (printf style formatting using the Java `Formatter` class): `format("Session property %s is invalid: %s", name, value)` (note that `format()` should always be statically imported). Sometimes, if you only need to append something, consider using the `+` operator.
* Avoid using the ternary operator except for trivial expressions.
* Use an assertion from Airlift's `Assertions` class if there is one that covers your case rather than writing the assertion by hand. Over time we may move over to more fluent assertions like AssertJ.
* When writing a Git commit message, follow these [guidelines](https://chris.beams.io/posts/git-commit/).

### Additional IDE configuration

When using IntelliJ to develop Presto, we recommend starting with all of the default inspections,
with some modifications.

Enable the following inspections:

- ``Java | Internationalization | Implicit usage of platform's default charset``,
- ``Java | Class structure | Utility class is not 'final'``,
- ``Java | Class structure | Utility class with 'public' constructor``,
- ``Java | Class structure | Utility class without 'private' constructor``.

Disable the following inspections:

- ``Java | Performance | Call to 'Arrays.asList()' with too few arguments``,
- ``Java | Abstraction issues | 'Optional' used as field or parameter type``.

### Building the Web UI
To build the UI for gateway, following steps need to carried out before the code is built. 

Prerequisites:  Node package manager(npm) should be pre-installed. 

- Install dependencies needed for gateway UI
```
$ npm install presto-gateway-main/src/main/ngapp/
```
- Build gateway UI
```
$ npm run-script build --prefix=presto-gateway-main/src/main/ngapp/ 
``` 
