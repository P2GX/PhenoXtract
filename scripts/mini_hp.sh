#!/bin/bash
# shellcheck disable=SC2046
OBO=https://github.com/obophenotype/human-phenotype-ontology/releases/download/v2025-09-01/hp.obo
module load robot/1.8.3

# TOP will assume that the term is the top term and get all children
# BOT will assume that the term is at the bottom an a hierarchy and get all ancestors.
TARGET_TERMS=(
'HP:0012823|TOP' # Severity
'HP:0012773|BOT' #|Reduced upper to lower segment ratio
'HP:0041249|BOT' #|Fractured nose
'HP:0010533|BOT' #|Spasmus nutans
'HP:0003674|TOP' #|onset
)


wget $OBO

# Extract terms
for entry in "${TARGET_TERMS[@]}"; do
  term="${entry%%|*}"      # Everything before |
  directory="${entry##*|}"
  echo "Processing term: ${term}"
  echo "With direction: ${directory}"
  java -jar ./scripts/robot/robot.jar extract --input hp.obo --method "${directory}" --term "${term}" \
  convert --check false --output "${term}".out.hp.obo 2>&1 | grep -v "WARNING:"
done

# Merge
INPUTS=""
for obofile in *.out.hp.obo; do
  INPUTS="--input ${obofile} ${INPUTS}"
done
echo input argument "$INPUTS"
java -jar ./scripts/robot/robot.jar merge ${INPUTS} --output tests/assets/ontologies/2025-09-01_hp.json 2>&1 | grep -v "WARNING:"

rm *.obo

ontology=$(cat tests/assets/ontologies/2025-09-01_hp.json)

version_url="http://purl.obolibrary.org/obo/hp/releases/2025-09-01/hp.json"

ontology=$(jq --arg version "$version_url" '
  .graphs[0].meta.version = $version
' <<<"$ontology")


echo "$ontology" > tests/assets/ontologies/2025-09-01_hp.json