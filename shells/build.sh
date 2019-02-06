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
rm -r -f $buildDir/data

if hash tar 2>/dev/null
then
  tar -czf $buildDir.tar.gz $buildDir/ && clean
fi

exit 0
