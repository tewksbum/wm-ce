FROM golang:1.14.1-stretch

COPY ./ /go/src/gitlab.com/wemade/ms/tmphackbrowseridserver

WORKDIR /go/src/gitlab.com/wemade/ms/tmphackbrowseridserver
RUN go get ./...

CMD go run main.go
