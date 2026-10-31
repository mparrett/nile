#!/bin/sh
# usage: nile-to-maru-compiler.sh MODULE FILE.nl
# Compiles Nile to Maru on a Maru checkout's eval2 (MARU, by default the
# checkout this one is a submodule of); writes MODULE.k to the current directory.

DIR="$( cd "$( dirname "$0" )" && pwd )"
MARU="${MARU:-$DIR/../../..}"

if [ ! -f "$MARU/boot2.l" ]; then
  echo "set MARU to a maru checkout" >&2
  exit 2
fi
MARU="$( cd "$MARU" && pwd )"

exec "$MARU/eval2" -b "$MARU/boot2.l" -L "$MARU/" -L "$DIR/" "$DIR/nile-compiler.l" "$DIR" maru "$@"
