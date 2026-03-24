#!/bin/bash
# shellcheck disable=SC2046
OBO=https://github.com/ncit-obo-org/ncit-obo-edition/releases/download/v2026-03-19/ncit.obo
module load robot/1.8.3

TARGET_TERMS=(
'NCIT:C12727' # Heart
'NCIT:C38516' # Tonsillar Tissue
'NCIT:C76356' # Leptotrichia buccalis
'NCIT:C26431' # Coronavirus
)

wget $OBO

# Extract terms
for term in "${TARGET_TERMS[@]}"; do
  echo "Processing ${term}"
  java -jar ./scripts/robot/robot.jar extract --input ncit.obo --method BOT --term "${term}" \
  convert --check false --output "${term}".out.ncit.obo 2>&1 | grep -v "WARNING:"
done

# Merge
INPUTS=""
for obofile in *.out.ncit.obo; do
  INPUTS="--input ${obofile} ${INPUTS}"
done
echo input argument "$INPUTS"
java -jar ./scripts/robot/robot.jar merge ${INPUTS} --output phenoxtract/src/test_suite/test_cache/ontology_registry/ncit_2026-03-19.obo 2>&1 | grep -v "WARNING:"

rm *.obo