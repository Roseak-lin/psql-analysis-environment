#!/bin/bash

TARGET_DIR="DB"

if [ -d "$TARGET_DIR" ]; then
    echo "Changing directory to $TARGET_DIR..."
    cd "$TARGET_DIR"
else
    echo "Error: Directory '$TARGET_DIR' does not exist."
    exit 1
fi

files=(
    "title.ratings.tsv.gz"
    "title.episode.tsv.gz"
    "title.crew.tsv.gz"
    "title.basics.tsv.gz"
    "name.basics.tsv.gz"
    "title.akas.tsv.gz"
    "title.principals.tsv.gz"
)

for file in "${files[@]}"; do
    if [ -f "$file" ]; then
        echo "Decompressing $file..."
        gunzip "$file"
    else
        echo "Warning: $file not found in $TARGET_DIR."
    fi
done

echo "Decompression complete."