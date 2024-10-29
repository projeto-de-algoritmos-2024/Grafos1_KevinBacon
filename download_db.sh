#!/bin/bash

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a $LOGFILE
}

log "Starting the script."

log "Downloading dataset from Kaggle."

if ! curl -L -o ~/Downloads/archive.zip https://www.kaggle.com/api/v1/datasets/download/darinhawley/imdb-films-by-actor-for-10k-actors; then
    log "Error: Failed to download the dataset."
    exit 1
fi
log "Download completed."

# Unzip the downloaded file
log "Unzipping the downloaded archive."

if ! unzip -o ~/Downloads/archive.zip; then
    log "Error: Failed to unzip the archive."
    exit 1
fi

if ! rm -f ~/Downloads/archive.zip; then
  log "Error: Failed to delete archive"
fi
log "Unzip completed."

# Run the Python script
log "Running the conversion script."

if ! python3 ./convert_script.py; then
    log "Error: Conversion script failed."
    exit 1
fi
log "Conversion script completed successfully."

log "Script finished."
