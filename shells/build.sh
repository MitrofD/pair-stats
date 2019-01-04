#!/bin/bash
buildDir=bundle
sourceDir=src
packageFile=package.json

clean() {
  rm -r -f $buildDir
}

clean
export NODE_ENV=production
export NODE_DEBUG=false
npx babel $sourceDir --out-dir $buildDir --copy-files || exit 1

sed -e '/"scripts"/,/}/d; /"devDependencies"/,/}/d' $packageFile > $buildDir/$packageFile

while [ -n "$1" ]
do
  case "$1" in
    -m)
      cp -R $buildDir/* $2
      clean
    ;;
  esac
  shift
done

exit 0
