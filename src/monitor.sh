#!/bin/bash

# Load environment variables from .env file for the Python program
set -a
source .env
set +a

# Execute the Python script
nohup python3 process_insert_update_weather_data.py >> ../log/nohup.out 2>&1 &

# Get the PID of the Python script
python_pid=$!

# Set a trap to kill the Python script when this shell script is terminated
trap "kill $python_pid; aws sns publish --topic-arn $TRANSFER_TOPIC_ARN --message 'The monitor.sh program was terminated. It also terminated the process_insert_update_weather_data.py program.'" EXIT

# Wait for the Python script to finish
wait $python_pid
exit_code=$?

# Check the exit code and send email notification using AWS SNS if it does not equal to 0
if [ $exit_code -ne 0 ]; then
    aws sns publish --topic-arn $TRANSFER_TOPIC_ARN --message 'The process_insert_update_weather_data.py program was terminated unexpectedly.'
fi