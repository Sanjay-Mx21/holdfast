#!/bin/sh
# Starts a Valkey node or a Sentinel (task 5.2) from a template in this
# directory, keeping the working copy in /data (a volume): Sentinel rewrites
# both, a node's role after a failover and the Sentinels' view of who is
# primary, and that must survive a restart. A fresh volume gets a fresh copy.
#
#   entrypoint.sh node <address> [<primary>]   a node, a replica of <primary> if given
#   entrypoint.sh sentinel <address>           a Sentinel
#
# <address> is the fixed address the process announces (compose.yaml), so
# the Sentinels and the services know every member by an address that
# survives a restart, and nobody needs DNS to reach a member (P47).
set -eu
kind=$1
name=$2

case $kind in
node)
	conf=/data/valkey.conf
	if [ ! -f "$conf" ]; then
		cp /etc/holdfast/valkey.conf "$conf"
		echo "replica-announce-ip $name" >>"$conf"
		if [ -n "${3:-}" ]; then
			echo "replicaof $3 6379" >>"$conf"
		fi
	fi
	exec docker-entrypoint.sh valkey-server "$conf"
	;;
sentinel)
	conf=/data/sentinel.conf
	if [ ! -f "$conf" ]; then
		cp /etc/holdfast/sentinel.conf "$conf"
		echo "sentinel announce-ip $name" >>"$conf"
	fi
	exec docker-entrypoint.sh valkey-server "$conf" --sentinel
	;;
*)
	echo "usage: entrypoint.sh node|sentinel <name> [<primary>]" >&2
	exit 2
	;;
esac
