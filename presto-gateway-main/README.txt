# Gateway service for Presto

This Gateway server allows clients to securely access multiple remote
Presto server without having direct access to the server.
For example, if the Presto server is behind a firewall, only the gateway
needs to be exposed to the client.

The gateway natively understands the Presto protocol and rewrites the
"nextUri" in response payloads to point to the gateway rather than the
remote server.

The routing of requests can be controlled by implementing RoutingRule.
By default, it has location based, random and static routing rules.

If so configured, the gateway will generate JWT access tokens containing
the principal from the TLS client certificate presented to the gateway.
This allows using the gateway in secure environments without needing
the gateway to perform explicit validation of the username and principal.

Note that the gateway does not attempt to hide the remote URIs from
clients. Hostnames and IP addresses will be visible to the client.
