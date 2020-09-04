#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM centos:centos7
LABEL maintainer="Presto community <https://prestosql.io/community.html>"

ENV JAVA_HOME /usr/lib/jvm/java-11
RUN \
    set -xeu && \
    yum -y -q update && \
    yum -y -q install java-11-openjdk-devel nc less && \
    yum -q clean all && \
    rm -rf /var/cache/yum && \
    rm -rf /tmp/* /var/tmp/* && \
    groupadd presto --gid 1000 && \
    useradd presto --uid 1000 --gid 1000 && \
    mkdir -p /usr/lib/presto-gateway-steerd /data/presto /config/templates && \
    chown -R "presto:presto" /usr/lib/presto-gateway-steerd /data/presto /config/templates

# Copy the google cloud repo file
COPY google-cloud-sdk.repo /etc/yum.repos.d/

# Install gcloud sdk
RUN \
    yum install -y google-cloud-sdk \

# Copy the kubectl repo file
COPY  kubernetes.repo /etc/yum.repos.d/

# Install kubectl
RUN \
    yum install -y kubectl \

# Install openssl, helm requires
RUN \
    yum install openssl openssl-devel -y

# Install helm from, https://helm.sh/docs/intro/install/
RUN \
    curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 && \
    chmod 700 get_helm.sh && \
    ./get_helm.sh

ARG GATEWAY_VERSION
COPY --chown=presto:presto presto-gateway-steerd-${GATEWAY_VERSION} /usr/lib/presto-gateway-steerd

COPY values.yaml /config/templates/values.yaml
COPY presto.yaml /config/templates/presto.yaml

EXPOSE 8080
USER presto:presto
ENV LANG en_US.UTF-8
CMD ["/usr/lib/presto-gateway-steerd/bin/run-presto-gateway-steerd"]
