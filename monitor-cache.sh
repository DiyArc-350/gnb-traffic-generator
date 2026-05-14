#!/bin/bash
export LC_NUMERIC=en_US.UTF-8

# =======================
# PREPARATION
# =======================
mkdir -p data_ats
declare -A baseline
log_buffer=""

# =======================
# UTILITIES
# =======================
get_next_file_index() {
    last=$(ls data_ats 2>/dev/null | grep -E '^data_ats_[0-9]+\.csv$' \
        | sed -E 's/.*_([0-9]+)\.csv/\1/' | sort -n | tail -1)
    [[ -z "$last" ]] && echo 1 || echo $((last + 1))
}

cleanup() {
    if [ "$(echo -e "$log_buffer" | wc -l)" -gt 1 ]; then
        file="data_ats/data_ats_$(get_next_file_index).csv"
        echo -e "$log_buffer" > "$file"
        echo -e "\n✅ Data saved to: $file"
    else
        echo -e "\n⚠️ No data saved."
    fi
    exit 0
}
trap cleanup SIGINT

get_int() {
    val=$(traffic_ctl metric get "$1" 2>/dev/null | awk 'NR==1{print int($2)}')
    [[ -z "$val" || ( "$val" == "0" && "$1" != *"hits"* ) ]] && echo 0 || echo "$val"
}

get_float() {
    val=$(traffic_ctl metric get "$1" 2>/dev/null | awk 'NR==1{print $2}')
    [[ -z "$val" ]] && echo 0 || echo "$val"
}

mb() {
    awk "BEGIN{printf \"%.2f\", $1/1048576}"
}

# =======================
# BASELINE
# =======================
set_baseline() {
    baseline[hit]=$(get_int proxy.process.cache_total_hits)
    baseline[req]=$(get_int proxy.process.cache_total_requests)
    baseline[miss]=$(get_int proxy.process.cache_total_misses)
    baseline[read]=$(get_int proxy.process.cache.read.success)
    baseline[read_fail]=$(get_int proxy.process.cache.read.failure)
    baseline[write]=$(get_int proxy.process.cache.write.success)
    baseline[dirent]=$(get_int proxy.process.cache.direntries.used)

    echo "✅ Baseline set at $(date '+%Y-%m-%d %H:%M:%S')"
    echo "   Hits         : ${baseline[hit]}"
    echo "   Requests     : ${baseline[req]}"
    echo "   Misses       : ${baseline[miss]}"
    echo "   Objects      : ${baseline[dirent]}"
    echo "   Writes       : ${baseline[write]}"
    echo "   Read Failures: ${baseline[read_fail]}"
    sleep 2
}
set_baseline

# =======================
# CSV HEADER
# =======================
log_buffer+="timestamp,\
hits_CUM,hits_REL,\
miss_CUM,miss_REL,\
req_CUM,req_REL,\
hit_ratio_REL,\
evict_CUM,evict_REL,\
evict_per_req_REL,\
objects_in_cache_SNAP,\
write_CUM,write_REL,\
read_CUM,read_REL,\
read_fail_CUM,read_fail_REL,\
percent_full_SNAP,\
disk_used_MB_SNAP,\
disk_total_MB_SNAP,\
ram_used_MB_SNAP,\
ram_total_MB_SNAP\n"

# =======================
# MAIN LOOP
# =======================
while true; do
    ts=$(date "+%Y-%m-%d %H:%M:%S")

    # --- CUMULATIVE ---
    hit=$(get_int proxy.process.cache_total_hits)
    miss=$(get_int proxy.process.cache_total_misses)
    req=$(get_int proxy.process.cache_total_requests)

    # ATS 9.2.0: use these two metrics to infer eviction activity
    read_fail=$(get_int proxy.process.cache.read.failure)
    write=$(get_int proxy.process.cache.write.success)

    read=$(get_int proxy.process.cache.read.success)
    dirent=$(get_int proxy.process.cache.direntries.used)

    # --- SNAPSHOT ---
    used=$(get_float proxy.process.cache.bytes_used)
    total=$(get_float proxy.process.cache.bytes_total)
    ram_used=$(get_float proxy.process.cache.ram_cache.bytes_used)
    ram_total=$(get_float proxy.process.cache.ram_cache.bytes_total)

    # --- CALCULATE percent_full manually ---
    pct=0
    [[ $(awk "BEGIN{print ($total > 0)}") -eq 1 ]] && \
        pct=$(awk "BEGIN{printf \"%.2f\", ($used/$total)*100}")

    # --- DELTA RELATIVE ---
    d_hit=$((hit - baseline[hit]))
    d_miss=$((miss - baseline[miss]))
    d_req=$((req - baseline[req]))
    d_read=$((read - baseline[read]))
    d_write=$((write - baseline[write]))
    d_read_fail=$((read_fail - baseline[read_fail]))

    # Derived eviction signal:
    # if write.success increases OR read.failure increases, treat it as eviction activity
    d_evict=$((d_read_fail))
    (( d_evict < 0 )) && d_evict=0

    # --- RATIOS ---
    hit_ratio=0
    evict_per_req=0
    [[ $d_req -gt 0 ]] && \
        hit_ratio=$(awk "BEGIN{printf \"%.2f\", ($d_hit/$d_req)*100}")
    [[ $d_req -gt 0 ]] && \
        evict_per_req=$(awk "BEGIN{printf \"%.2f\", ($d_evict/$d_req)*100}")

    # --- DISPLAY ---
    clear
    echo "╔══════════════════════════════════════════════╗"
    echo "║         ATS CACHE MONITOR — $ts         ║"
    echo "╚══════════════════════════════════════════════╝"
    echo
    echo "━━━━━ STORAGE SNAPSHOT ━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Cache Full     : ${pct}%"
    echo "  Disk Used      : $(mb $used) MB / $(mb $total) MB"
    echo "  RAM Used       : $(mb $ram_used) MB / $(mb $ram_total) MB"
    echo "  Objects Cached : $dirent"
    echo
    echo "━━━━━ EFFICIENCY (relative since baseline) ━━━━"
    echo "  Requests       : $d_req   (total: $req)"
    echo "  Hits           : $d_hit   (total: $hit)"
    echo "  Misses         : $d_miss  (total: $miss)"
    echo "  Hit Ratio      : ${hit_ratio}%"
    echo
    echo "━━━━━ EVICTION (derived from write.success + read.failure) ━━━━━"
    echo "  Write Success   : $d_write (total: $write)"
    echo "  Read Failure    : $d_read_fail (total: $read_fail)"
    echo "  Eviction Signal : $d_evict"
    echo "  Evict/Request   : ${evict_per_req}%"
    echo
    echo "━━━━━ CACHE I/O ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Writes         : $d_write (total: $write)"
    echo "  Reads          : $d_read  (total: $read)"
    echo
    echo "  [Press Ctrl+C to stop and save CSV]"

    # --- CSV LOG ---
    log_buffer+="$ts,\
$hit,$d_hit,\
$miss,$d_miss,\
$req,$d_req,\
$hit_ratio,\
$d_evict,$d_evict,\
$evict_per_req,\
$dirent,\
$write,$d_write,\
$read,$d_read,\
$read_fail,$d_read_fail,\
$pct,\
$(mb $used),$(mb $total),\
$(mb $ram_used),$(mb $ram_total)\n"

    sleep 1
done