exception report runs as systemd services

- in `/etc/systemd/system`, symlink to the two service files in the repo
- in `/etc/rsyslog.d`, symlink to the conf file

A typical deploy process:
```
cd github
git pull origin dev
cd compute-engine/file-report/src
go build -o exception-report .
sudo systemctl daemon-reload
sudo systemctl restart exception-report-dev
sudo systemctl restart exception-report-prod
```