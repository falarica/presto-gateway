# HTTPS for Gateway and Presto Server 

## Generating HTTPS Key and Certificate

HTTPS server needs to have a key pair. And HTTP Client needs to have a certificate to connect with the HTTP server. For our use case, we have Gateway server and Presto server as HTTPS server. So we would need keypair and certificate for Gateway server and for every presto server. 

Generate a key and certificate for the prestoserver:
```bash 
# Replace PATH_FOR_KEYSTORE, PRESTO_KEYSTORE_PASSWORD, PRESTO_HOSTNAME in the command below
# PRESTO_HOSTNAME is needed while verifying the key by the client. 
# For test environments, specify any name and then update your /etc/hosts with this name pointing to presto server
# Server key would be stored in PATH_FOR_KEYSTORE as prestoserverkeystore.jks

keytool -genkeypair -alias prestoserver  -keyalg RSA -keystore PATH_FOR_KEYSTORE/prestoserverkeystore.jks  -storepass PRESTO_KEYSTORE_PASSWORD  -dname "CN=PRESTO_HOSTNAME"

# use the following command to generate the keypair if you want to use IP 
keytool -genkeypair -alias prestoserver    -ext SAN=IP:PRESTOSERVER_IP  -keyalg RSA -keystore PATH_FOR_KEYSTORE/prestoserverkeystore.jks   -storepass PRESTO_KEYSTORE_PASSWORD 


# Replace PATH_FOR_KEYSTORE, PRESTO_KEYSTORE_PASSWORD, PATH_FOR_CERTIFICATE in the command below
# Client certificate would be stored in PATH_FOR_CERTIFICATE as prestoserver_clientcertificate.jks

keytool -export -alias prestoserver -keystore PATH_FOR_KEYSTORE/prestoserverkeystore.jks  -storepass PRESTO_KEYSTORE_PASSWORD -rfc -file PATH_FOR_CERTIFICATE/prestoserver_clientcertificate.jks
```

Similarly generate a key and certificate for gateway server 

```bash 
# Replace PATH_FOR_KEYSTORE, GATEWAY_KEYSTORE_PASSWORD, GATEWAY_HOSTNAME in the command below
# GATEWAY_HOSTNAME is needed while verifying the key by the client. 
# For test environments, specify any name and then update your /etc/hosts with this name pointing to gateway  server
# Server key would be stored in PATH_FOR_KEYSTORE as gatewaykeystore.jks

keytool -genkeypair -alias gatewayserver  -keyalg RSA -keystore PATH_FOR_KEYSTORE/gatewaykeystore.jks  -storepass GATEWAY_KEYSTORE_PASSWORD  -dname "CN=GATEWAY_HOSTNAME"

# use the following command to generate the keypair if you want to use IP 
keytool -genkeypair -alias prestoserver    -ext SAN=IP:GATEWAYSERVER_IP  -keyalg RSA -keystore PATH_FOR_KEYSTORE/prestoserverkeystore.jks   -storepass PRESTO_KEYSTORE_PASSWORD 


# Replace PATH_FOR_KEYSTORE, GATEWAY_KEYSTORE_PASSWORD, PATH_FOR_CERTIFICATE in the command below
# Client certificate would be stored in PATH_FOR_CERTIFICATE as gateway_clientcertificate.jks

keytool -export -alias gatewayserver -keystore PATH_FOR_KEYSTORE/gatewaykeystore.jks  -storepass GATEWAY_KEYSTORE_PASSWORD -rfc -file PATH_FOR_CERTIFICATE/gateway_clientcertificate.jks
```

## Enabling HTTPS for Presto Server 

For now, we will enable both HTTPS and HTTP server. Having both HTTP server and HTTPS server has the advantage that HTTP can be used for internal communication and it remains fast while HTTPS is used for external communication. As per presto documentation - "You should consider using an firewall to limit access to the HTTP endpoint to only those hosts that should be allowed to use it.". In our K8s environment, this would be true. 

To do this, the following would be the properties needed in the coordinator config.properties file. 

```bash
# Replace PRESTO_KEYSTORE_PASSWORD and PATH_FOR_KEYSTORE with actual values
http-server.http.enabled=true
http-server.https.enabled=true
http-server.https.port=8082
http-server.http.port=8081
http-server.https.keystore.path=PATH_FOR_KEYSTORE/prestoserverkeystore.jks
http-server.https.keystore.key=PRESTO_KEYSTORE_PASSWORD
```
Let the discover server run on the HTTP port. No changes are needed for the presto worker. 

## Enabling HTTPS for Gateway server

For Gateway server, we will disable HTTP server and will only keep HTTPS server. The following properties need to be added to the config.properties file of the Gateway server.

```bash
# Replace GATEWAY_KEYSTORE_PASSWORD and PATH_FOR_KEYSTORE with actual values
http-server.http.enabled=false
http-server.https.enabled=true
http-server.https.port=8080
http-server.https.keystore.path=PATH_FOR_KEYSTORE/gatewaykeystore.jks
http-server.https.keystore.key=GATEWAY_KEYSTORE_PASSWORD
```

## Connecting Gateway server to a secured Presto Server

Now the Gateway server and Presto server are secured but to let gateway connect to a secured presto server, we need to provide the certificate of the Presto server to the Gateway server as a httpclient property. The following properties are needed in the config.properties file. 

```bash 
gateway.http-client.trust-store-path=PATH_FOR_CERTIFICATE/prestoserver_clientcertificate.jks
# Not needed as of now
#gateway.http-client.trust-store-password=`
```

One thing that needs to be ensured is that presto server hostname that the Gateway uses is same as what was specified as PRESTO_HOSTNAME while generating the key pair. Otherwise, it will throw an exception "Unverified hostname".

## Connecting JDBC Clients to a secured Presto or Gateway Server

To connect JDBC clients to a secured Gateway server, following code would be needed. 

```java
// GATEWAY_HOSTNAME should be same as what was specified while generating the key pair
String url = "jdbc:presto://GATEWAY_HOSTNAME:8080/tpch";
Properties properties = new Properties();
properties.setProperty("user", "admin");
properties.setProperty("SSL", "true");
properties.setProperty("SSLTrustStorePath", "PATH_FOR_CERTIFICATE/gateway_clientcertificate.jks");
// Not needed as of now
// properties.setProperty("SSLTrustStorePassword", "");
Connection connection = DriverManager.getConnection(url, properties);
```

JDBC client can be be connected to a secured presto server by specifying the hostname and certificate of presto server in the code above.

## UI of Presto server 

UI of presto server is disabled for HTTPS without an authenticator. Once we have that in authenticator in place, we will revisit this. 

