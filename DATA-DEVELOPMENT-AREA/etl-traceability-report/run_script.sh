#!/bin/bash

#run main.py, display logs in the terminal, and capture its output
output=$(python3 -u main.py 2>&1 | tee /proc/1/fd/1)

#get additional parameter from output main.py
report_custom_date=$(echo "$output" | grep "REPORT_CUSTOM_DATE=" | head -n 1 | cut -d '=' -f 2 | xargs)
report_recipients=$(echo "$output" | grep "REPORT_RECIPIENTS=" | head -n 1 | cut -d '=' -f 2 | xargs)

#status memori after daily report
echo ""
echo "Memori after daily report:"
free -h

#evaluate condition for monitoring
if [[ -z "$report_custom_date" && "$report_recipients" == "prd" ]]; then
    echo ""
    echo "report_custom_date is empty and report_recipients is 'prd', running monitoring.py"
    python3 monitoring.py
    #status memori after monitoring
    echo ""
    echo "Memori after monitoring:"
    free -h
else
    echo ""
    echo "monitoring.py will not run, because:"
    if [[ -n "$report_custom_date" ]]; then
        echo "report_custom_date is not empty ('$report_custom_date')"
    fi
    if [[ "$report_recipients" != "prd" ]]; then
        echo "report_recipients is not 'prd' ('$report_recipients')"
    fi
fi
