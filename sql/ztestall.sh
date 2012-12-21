#! /bin/sh

set -e

for pg in pg83 pg84 pg90 pg91 pg92 pg93; do
  for mod in pgq pgq_coop pgq_node pgq_ext londiste; do
    echo "  #### $pg/$mod ####"
    $pg make -s -C $mod clean test
  done
done

