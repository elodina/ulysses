# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM centos:7

MAINTAINER support@elodina.net

ENV VERSION=1.6
ENV OS=linux
ENV ARCH=amd64

RUN yum install -y wget git

RUN wget https://storage.googleapis.com/golang/go$VERSION.$OS-$ARCH.tar.gz
RUN tar -C /usr/local -xzf go$VERSION.$OS-$ARCH.tar.gz

ENV GLIDE_VERSION 0.10.2
RUN wget https://github.com/Masterminds/glide/releases/download/${GLIDE_VERSION}/glide-${GLIDE_VERSION}-linux-amd64.tar.gz && \
  tar -xzf glide-${GLIDE_VERSION}-linux-amd64.tar.gz && \
  mv linux-amd64 /usr/local/glide && \
  rm glide-${GLIDE_VERSION}-linux-amd64.tar.gz

ENV GOROOT /usr/local/go
ENV GOPATH /
ENV PATH $GOROOT/bin:$GOPATH/bin:/usr/local/glide:$PATH

ADD . /src/github.com/elodina/ulysses
RUN cd /src/github.com/elodina/ulysses
WORKDIR /src/github.com/elodina/ulysses
RUN glide install --update-vendored
CMD go build schema_repo.go
ENTRYPOINT /bin/bash