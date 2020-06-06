#!/bin/bash
input=env-dev.txt
while IFS= read -r line
do
  export $line
done < "$input"
./dev-exception-report&