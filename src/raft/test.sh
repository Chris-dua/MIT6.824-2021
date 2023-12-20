#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Usage: $0 <total_runs>"
    exit 1
fi

total_runs=$1
pass_count=0
fail_count=0

for ((i=1; i<=$total_runs; i++)); do
    # echo "Running test $i/$total_runs..."
    
    # Run the test command, you might need to adjust the actual command here
    result=$(go test -run 2D -race 2>&1)
   #  echo "$result"  # Print the entire result
   echo "$result"

    if echo "$result" | grep -q "FAIL"; then
        echo "Failed"
        
        echo "$result" | grep -A 2 "FAIL"
        ((fail_count++))
    else
        ((pass_count++))
    fi
    echo "Done $i/$total_runs; $pass_count ok, $fail_count failed"
done


