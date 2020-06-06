#!/bin/bash
input=env-prod.txt
while IFS= read -r line
do
  export $line
done < "$input"
./prod-exception-report&