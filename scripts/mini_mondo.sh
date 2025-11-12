#!/bin/bash
# shellcheck disable=SC2046
OBO=https://github.com/monarch-initiative/mondo/releases/download/v2025-11-04/mondo.obo
module load robot/1.8.3

TARGET_TERMS=(
'MONDO:0008258' #
'MONDO:0008917' #
'MONDO:0012145' #
'MONDO:0000359' #
'MONDO:0000252' #
)

wget $OBO

# Extract terms
for term in "${TARGET_TERMS[@]}"; do
  echo "Processing ${term}"
  java -jar ./scripts/robot/robot.jar extract --input mondo.obo --method BOT --term "${term}" \
  convert --check false --output "${term}".out.mondo.obo 2>&1 | grep -v "WARNING:"
done

# Merge
INPUTS=""
for obofile in *.out.mondo.obo; do
  INPUTS="--input ${obofile} ${INPUTS}"
done
echo input argument "$INPUTS"
java -jar ./scripts/robot/robot.jar merge ${INPUTS} --output tests/assets/ontologies/2025-11-04_mondo.json 2>&1 | grep -v "WARNING:"

rm *.obo


version_url="http://purl.obolibrary.org/obo/mondo/releases/2025-11-04/mondo.json"

jq --arg version "$version_url" '
  .graphs[0].meta.version = $version
' tests/assets/ontologies/2025-11-04_mondo.json > tmp.$$.json && mv tmp.$$.json tests/assets/ontologies/2025-11-04_mondo.json


