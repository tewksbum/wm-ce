FROM golang:1.12.1-stretch

COPY ./ /go/src/gitlab.com/wemade/ms/tmphackbrowseridserver

WORKDIR /go/src/gitlab.com/wemade/ms/tmphackbrowseridserver
RUN go get ./...
