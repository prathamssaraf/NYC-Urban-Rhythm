#!/bin/bash

# NYC Data Collection Script
# This script collects NYC data from various sources for 2020-2025

# Set up log file
LOG_FILE="nyc_data_collection.log"
echo "Starting NYC data collection: $(date)" > $LOG_FILE

# Function to log messages
log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a $LOG_FILE
}

# Function to handle errors
handle_error() {
  log "ERROR: $1 command failed with exit code $2"
}

# Create data directory if it doesn't exist
DATA_DIR="./nyc_data"
mkdir -p $DATA_DIR
cd $DATA_DIR

# 1. Collect Weather Data (year by year to avoid rate limits)
log "Starting Weather data collection..."
for year in {2020..2025}
do
  start_date="${year}-01-01"
  end_date="${year}-12-31"
  
  # Adjust end date for current year
  if [ "$year" == "2025" ]; then
    end_date="2025-04-10" # Or current date
  fi
  
  log "Fetching weather data for $start_date to $end_date"
  python3 -m backend.etl.fetch_weather --start-date "$start_date" --end-date "$end_date" --save-csv
  
  if [ $? -ne 0 ]; then
    handle_error "Weather data" $?
  else
    log "Successfully collected weather data for $year"
  fi
  
  # Sleep to avoid API rate limits
  sleep 5
done

# 2. Collect TLC Taxi Data (month by month)
log "Starting TLC Taxi data collection..."
for year in {2020..2023} # Limited to 2023 as that's the last year with confirmed dataset IDs
do
  for month in {1..12}
  do
    log "Fetching TLC taxi data for $year-$month"
    python3 -m backend.etl.fetch_tlc --year $year --month $month --save-csv
    
    if [ $? -ne 0 ]; then
      handle_error "TLC data for $year-$month" $?
    else
      log "Successfully collected TLC data for $year-$month"
    fi
    
    # Sleep to avoid API rate limits
    sleep 5
  done
done

# Try 2024 data (might need manual dataset ID updates)
log "Attempting to fetch 2024 TLC data (may require dataset ID updates)..."
for month in {1..3} # Through March 2024
do
  python3 -m backend.etl.fetch_tlc --year 2024 --month $month --save-csv
  
  if [ $? -ne 0 ]; then
    log "WARNING: 2024-$month TLC data might not be available or needs dataset ID update"
  else
    log "Successfully collected TLC data for 2024-$month"
  fi
  
  sleep 5
done

# 3. Collect 311 Service Request Data (quarterly to avoid huge API requests)
log "Starting 311 Service Request data collection..."
for year in {2020..2025}
do
  for quarter in {1..4}
  do
    case $quarter in
      1) start_month="01"; end_month="03" ;;
      2) start_month="04"; end_month="06" ;;
      3) start_month="07"; end_month="09" ;;
      4) start_month="10"; end_month="12" ;;
    esac
    
    start_date="${year}-${start_month}-01"
    
    # Set end date based on quarter
    if [ "$quarter" == "1" ]; then
      end_date="${year}-03-31"
    elif [ "$quarter" == "2" ]; then
      end_date="${year}-06-30"
    elif [ "$quarter" == "3" ]; then
      end_date="${year}-09-30"
    else # Quarter 4
      end_date="${year}-12-31"
    fi
    
    # Adjust end date for current year/quarter
    if [ "$year" == "2025" ] && [ "$quarter" == "2" ]; then
      end_date="2025-04-10" # Or current date
      # Skip remaining quarters of 2025
      break
    fi
    
    log "Fetching 311 data for $start_date to $end_date"
    python3 -m backend.etl.fetch_311 --start-date "$start_date" --end-date "$end_date" --save-csv
    
    if [ $? -ne 0 ]; then
      handle_error "311 data for $start_date to $end_date" $?
    else
      log "Successfully collected 311 data for $start_date to $end_date"
    fi
    
    # Sleep to avoid API rate limits
    sleep 10
  done
done

# 4. Collect MTA Subway Ridership Data (monthly to avoid rate limits)
log "Starting MTA Subway data collection..."
for year in {2020..2025}
do
  for month in {1..12}
  do
    # Format month with leading zero
    month_padded=$(printf "%02d" $month)
    
    start_date="${year}-${month_padded}-01"
    
    # Calculate end date (last day of month)
    if [ "$month" == "2" ]; then
      # Handle February and leap years
      if ([ "$year" == "2020" ] || [ "$year" == "2024" ]); then
        end_date="${year}-02-29" # Leap years
      else
        end_date="${year}-02-28"
      fi
    elif [ "$month" == "4" ] || [ "$month" == "6" ] || [ "$month" == "9" ] || [ "$month" == "11" ]; then
      end_date="${year}-${month_padded}-30"
    else
      end_date="${year}-${month_padded}-31"
    fi
    
    # Adjust end date for current year/month
    if [ "$year" == "2025" ] && [ "$month" == "4" ]; then
      end_date="2025-04-10" # Or current date
      # Skip remaining months of 2025
      break
    fi
    
    log "Fetching MTA data for $start_date to $end_date"
    python3 -m backend.etl.fetch_mta --start-date "$start_date" --end-date "$end_date" --save-csv
    
    if [ $? -ne 0 ]; then
      handle_error "MTA data for $start_date to $end_date" $?
    else
      log "Successfully collected MTA data for $start_date to $end_date"
    fi
    
    # Sleep to avoid API rate limits
    sleep 10
  done
done

# 5. Collect NYC Events Data (quarterly to avoid large API requests)
log "Starting NYC Events data collection..."
for year in {2020..2025}
do
  for quarter in {1..4}
  do
    case $quarter in
      1) start_month="01"; end_month="03" ;;
      2) start_month="04"; end_month="06" ;;
      3) start_month="07"; end_month="09" ;;
      4) start_month="10"; end_month="12" ;;
    esac
    
    start_date="${year}-${start_month}-01"
    
    # Set end date based on quarter
    if [ "$quarter" == "1" ]; then
      end_date="${year}-03-31"
    elif [ "$quarter" == "2" ]; then
      end_date="${year}-06-30"
    elif [ "$quarter" == "3" ]; then
      end_date="${year}-09-30"
    else # Quarter 4
      end_date="${year}-12-31"
    fi
    
    # Adjust end date for current year/quarter
    if [ "$year" == "2025" ] && [ "$quarter" == "2" ]; then
      end_date="2025-04-10" # Or current date
      # Skip remaining quarters of 2025
      break
    fi
    
    log "Fetching NYC Events data for $start_date to $end_date"
    python3 -m backend.etl.fetch_events --start-date "$start_date" --end-date "$end_date" --save-csv
    
    if [ $? -ne 0 ]; then
      handle_error "Events data for $start_date to $end_date" $?
    else
      log "Successfully collected Events data for $start_date to $end_date"
    fi
    
    # Sleep to avoid API rate limits
    sleep 10
  done
done

log "Data collection process completed: $(date)"

# Summary report
echo "===== NYC DATA COLLECTION SUMMARY =====" | tee -a $LOG_FILE
echo "Completed at: $(date)" | tee -a $LOG_FILE
echo "Check $LOG_FILE for detailed information" | tee -a $LOG_FILE

# Return to original directory
cd - > /dev/null