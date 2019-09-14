#
# Copyright 2017 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM golang:1.10.2-alpine as builder
ARG DEP_VERSION="0.4.1"
RUN apk update && apk add bash git
ADD https://github.com/golang/dep/releases/download/v${DEP_VERSION}/dep-linux-amd64 /usr/bin/dep
RUN chmod +x /usr/bin/dep

WORKDIR ${GOPATH}/src/github.com/GoogleCloudPlatform/spark-on-k8s-operator
COPY Gopkg.toml Gopkg.lock ./
RUN dep ensure -vendor-only
COPY . ./
RUN go generate && go build -o /usr/bin/spark-operator


FROM kubespark/spark-base:v2.2.0-kubernetes-0.5.0
COPY --from=builder /usr/bin/spark-operator /usr/bin/
COPY hack/gencerts.sh /usr/bin/
RUN apk add --no-cache openssl curl tini
ENTRYPOINT ["/usr/bin/spark-operator"]
