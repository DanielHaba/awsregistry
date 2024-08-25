FROM golang:1.22.6-alpine AS build

COPY . /workdir
WORKDIR /workdir
RUN go build -o awsregistry .


FROM amazon/aws-cli:2.17.37 
COPY --from=build /workdir/awsregistry /usr/local/bin/awsregistry
