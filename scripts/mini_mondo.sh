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
  convert --check false --output ${term}.out.mondo.obo
done

# Merge
INPUTS=""
for obofile in *.out.mondo.obo; do
  INPUTS="--input ${obofile} ${INPUTS}"
done
java -jar ./scripts/robot/robot.jar merge "${INPUTS}" --output mondo.toy.json

rm *.obo
