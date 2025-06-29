#!/bin/bash

# Deduplicate ticket JSON files - keep only the latest version of each ticket
# Usage: ./deduplicate_tickets.sh [--dry-run] [--export-path PATH]

set -euo pipefail

# Default values
DRY_RUN=false
EXPORT_PATH="export"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --export-path)
            EXPORT_PATH="$2"
            shift 2
            ;;
        *)
            echo "Usage: $0 [--dry-run] [--export-path PATH]"
            echo "  --dry-run: Show what would be deleted without actually deleting"
            echo "  --export-path: Path to export directory (default: export)"
            exit 1
            ;;
    esac
done

echo "=== Ticket Deduplication Script ==="
echo "Export path: $EXPORT_PATH"
echo "Dry run: $DRY_RUN"
echo

# Check if export path exists
if [[ ! -d "$EXPORT_PATH" ]]; then
    echo "Error: Export path '$EXPORT_PATH' does not exist"
    exit 1
fi

# Step 1: Extract ticket data in parallel
echo "Step 1: Scanning JSON files..."
TEMP_FILE=$(mktemp)
find "$EXPORT_PATH" -name "*.json" -print0 | \
    xargs -0 -P $(nproc) -I {} sh -c 'jq -r "(.ticket.id | tostring) + \":\" + (.ticket.updated_at // \"null\") + \":\" + \"{}\"" "{}" 2>/dev/null || echo "error:{}"' > "$TEMP_FILE"

# Filter out errors and count files
TOTAL_FILES=$(grep -v "^error:" "$TEMP_FILE" | wc -l)
ERROR_FILES=$(grep -c "^error:" "$TEMP_FILE" 2>/dev/null || echo "0")

echo "Found $TOTAL_FILES valid JSON files"
if [[ $ERROR_FILES -gt 0 ]]; then
    echo "Warning: $ERROR_FILES files had errors and were skipped"
fi

# Step 2: Process duplicates using sort and simple bash
echo
echo "Step 2: Finding duplicates..."

# Clean up any previous temp files
rm -f /tmp/files_to_remove /tmp/ticket_analysis /tmp/latest_tickets

# Sort by ticket ID, then by timestamp (descending to get latest first)
# Note: We need to handle the colon-separated format carefully since file paths contain colons
grep -v "^error:" "$TEMP_FILE" | sort -t: -k1,1n -k2,2r > /tmp/ticket_analysis

echo "=== Duplicate Analysis ==="

# Find the latest version of each ticket (first occurrence after sort)
awk -F: '!seen[$1]++ {print $0}' /tmp/ticket_analysis > /tmp/latest_tickets

# Find files to remove (everything not in latest_tickets)
files_to_remove=0
files_to_keep=0

echo "Processing duplicates..."

# For each ticket, find all versions and mark older ones for removal
while IFS=: read -r ticket_id timestamp rest; do
    # Reconstruct the full filepath (everything after the second colon)
    filepath="${rest}"
    # Count total versions of this ticket
    total_versions=$(grep "^$ticket_id:" /tmp/ticket_analysis | wc -l)

    if [[ $total_versions -gt 1 ]]; then
        echo "Ticket $ticket_id: $total_versions versions found"
        echo "  Latest: $filepath ($timestamp)"

        # Find and mark older versions for removal
        grep "^$ticket_id:" /tmp/ticket_analysis | tail -n +2 | while IFS=: read -r _ old_timestamp rest; do
            # Reconstruct the full filepath (everything after the second colon)
            old_filepath="${rest}"
            echo "  REMOVE: $old_filepath ($old_timestamp)"
            echo "$old_filepath" >> /tmp/files_to_remove
        done

        files_to_remove=$((files_to_remove + total_versions - 1))
        files_to_keep=$((files_to_keep + 1))
        echo
    else
        files_to_keep=$((files_to_keep + 1))
    fi
done < /tmp/latest_tickets

echo "Summary: $files_to_remove files to remove, $files_to_keep files to keep"

# Step 3: Remove duplicates (if not dry run)
if [[ -f "/tmp/files_to_remove" ]]; then
    FILES_TO_REMOVE=$(wc -l < /tmp/files_to_remove 2>/dev/null || echo "0")

    if [[ $FILES_TO_REMOVE -gt 0 ]]; then
        echo
        echo "Step 3: Removing duplicate files..."

        if [[ "$DRY_RUN" == "true" ]]; then
            echo "DRY RUN - Would remove these files:"
            cat /tmp/files_to_remove | sed 's/^/  /'
        else
            echo "Removing $FILES_TO_REMOVE duplicate files..."

            # Remove files in parallel
            cat /tmp/files_to_remove | xargs -P $(nproc) -I {} rm -f "{}"

            echo "✓ Removed $FILES_TO_REMOVE duplicate files"

            # Clean up empty directories
            echo "Cleaning up empty directories..."
            find "$EXPORT_PATH" -type d -empty -delete 2>/dev/null || true
        fi
    else
        echo "No duplicate files found to remove."
    fi
else
    echo "No duplicate files found."
fi

# Cleanup
rm -f "$TEMP_FILE" /tmp/files_to_remove /tmp/ticket_analysis /tmp/latest_tickets

echo
echo "=== Deduplication Complete ==="
