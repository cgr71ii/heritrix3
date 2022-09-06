#!/bin/bash

DIR="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
PROFILES_DIR="${DIR}/profiles"
PROFILE_TARGET="${DIR}/profile-crawler-beans.cxml"
NEW_PROFILE="./profiles/$1"

if [[ ! -f "$PROFILE_TARGET" ]]; then
  >&2 echo "WARNING: there is not current profile actived ($PROFILE_TARGET)"
fi

ls_profiles()
{
  >&2 echo "Available profiles ($(ls ""$PROFILES_DIR"" | wc -l)):"
  >&2 ls "$PROFILES_DIR"
}

if [[ "$#" -eq "0" ]]; then
  >&2 echo "Syntax: $(basename $0) <profile>"
  >&2 echo ""
  ls_profiles

  exit 1
fi

pushd "$DIR" > /dev/null

if [[ ! -f "$NEW_PROFILE" ]]; then
  >&2 echo "Provided profile does not exist ($NEW_PROFILE)"
  >&2 echo ""
  ls_profiles

  exit 1
fi

ln -sf "$NEW_PROFILE" "$PROFILE_TARGET" \
  && echo "New profile activated: $PROFILE_TARGET -> $NEW_PROFILE" \
  || >&2 echo "Couldn't activate new profile"

popd > /dev/null
