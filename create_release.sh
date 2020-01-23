#!/bin/sh -ex

./gradlew -q clean test distTar

git co master
git pull

VERSION=`./gradlew -q printVersion`
BRANCH="release/$VERSION"

git co -b "$BRANCH"
git push origin "$BRANCH"

./gradlew -q clean distTar

git push origin "$VERSION"

RELEASE_BODY=$(cat <<EOF
{
  "tag_name": "${VERSION}",
  "target_commitish": "${BRANCH}",
  "name": "${VERSION}",
  "body": "Release ${VERSION}",
  "draft": false,
  "prerelease": false
}
EOF
)

RELEASE_ID=$(okurl -d "$RELEASE_BODY" https://api.github.com/repos/Redislabs-Solution-Architects/riot/releases | jq .id)

echo Created "https://api.github.com/repos/Redislabs-Solution-Architects/riot/releases/${RELEASE_ID}"

okurl -H "Content-Type: application/x-gzip" --data="@build/distributions/riot-${VERSION}.tgz" "https://uploads.github.com/repos/Redislabs-Solution-Architects/riot/releases/${RELEASE_ID}/assets?name=riot-${VERSION}.tgz" | jq ".browser_download_url"

git co master
