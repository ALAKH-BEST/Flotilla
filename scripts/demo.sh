#!/usr/bin/env bash
# Runs a small Flotilla cluster on this machine and walks through what it does: replicate a
# write, survive losing the leader, restart a node from disk, add a node, remove a node.
# Needs Go. Uses ports 18000-18003 (HTTP) and 17000-17003 (Raft); set BASE to move them.
set -euo pipefail

BASE=${BASE:-17000}
work=$(mktemp -d)
bin=$work/flotilla
pids=()

cleanup() {
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

raft() { echo "127.0.0.1:$((BASE + $1))"; }
http() { echo "127.0.0.1:$((BASE + 1000 + $1))"; }
spec() { echo "$1=$(raft "$1")/$(http "$1")"; }
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
run()  { printf '$ flotilla %s\n' "$*"; "$bin" "$@"; }

start() { # start ID [extra flags]; the node's pid goes in $pid_of_$ID
  local id=$1; shift
  "$bin" server -id "$id" -data "$work/data$id" "$@" >"$work/node$id.log" 2>&1 &
  pids+=($!)
  eval "pid_$id=$!"
}

wait_for() { # retry a command for up to 15 seconds
  for _ in $(seq 150); do "$@" >/dev/null 2>&1 && return 0; sleep 0.1; done
  echo "timed out waiting for: $*" >&2; return 1
}

step "Build"
(cd "$(dirname "$0")/.." && go build -o "$bin" ./cmd/flotilla)

CLUSTER="$(spec 0),$(spec 1),$(spec 2)"
ALL="$(http 0),$(http 1),$(http 2)"

step "Start a three-node cluster"
for id in 0 1 2; do start "$id" -cluster "$CLUSTER"; done
wait_for "$bin" put -server "$ALL" ready yes

step "Write through one node and read through another; followers redirect to the leader"
run put -server "$(http 0)" city Paris
run get -server "$(http 2)" city

step "Find the leader and stop it"
leader=$("$bin" status -server "$ALL" | grep -o '"leader": *[0-9]*' | grep -o '[0-9]*$')
echo "leader is node $leader"
eval "kill \$pid_$leader"
survivors=""
for id in 0 1 2; do [ "$id" != "$leader" ] && survivors="${survivors:+$survivors,}$(http "$id")"; done

step "The others elect a new leader; nothing written earlier is lost"
wait_for "$bin" put -server "$survivors" after-failover ok
run get -server "$survivors" city
run get -server "$survivors" after-failover

step "Restart the stopped node from its data directory; it catches up"
start "$leader" -cluster "$CLUSTER"
rejoined() { "$bin" status -server "$(http "$leader")" | grep -q '"commitIndex":[1-9]'; }
wait_for rejoined
run status -server "$(http "$leader")"

step "Add a fourth node: it joins as a non-voting learner, then is promoted once it has caught up"
start 3 -cluster "$(spec 3)" -join
wait_for "$bin" status -server "$(http 3)"
run add -server "$ALL" 3 "$(raft 3)/$(http 3)"
run members -server "$ALL"
run get -server "$(http 3)" after-failover

step "Remove a node"
run remove -server "$ALL" 1
eval "kill \$pid_1" 2>/dev/null || true
rest="$(http 0),$(http 2),$(http 3)"
wait_for "$bin" put -server "$rest" after-remove ok
run members -server "$rest"
run get -server "$rest" after-remove

printf '\nDone. The cluster still has its data after losing the leader, restarting a node, and changing its members.\n'
