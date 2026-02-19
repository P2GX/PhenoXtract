# PhenoXtract

## How PhenoXtract works

Collection, strategies, blah blah.

## Extracting Individual Data

## Extracting Phenotypes

## Extracting Diseases

## Extracting Interpretations

## Extracting Measurements

## Extracting Medical Actions

## Contexts

TODO!

SubjectId,
SubjectSex,
DateOfBirth,
VitalStatus,
TimeAtLastEncounter(TimeElementType),
TimeOfDeath(TimeElementType),
CauseOfDeath,
SurvivalTimeDays,

Hpo,
Disease,
MultiHpoId,
Onset(TimeElementType),

Hgvs,
Hgnc,

QuantitativeMeasurement {
assay_id: String,
unit_ontology_id: String,
},
QualitativeMeasurement {
assay_id: String,
},
TimeOfMeasurement(TimeElementType),
ReferenceRange(Boundary),

TreatmentTarget,
TreatmentIntent,
ResponseToTreatment,
TreatmentTerminationReason,
ProcedureLabelOrId,
ProcedureBodySite,
TimeOfProcedure(TimeElementType),

ObservationStatus,
None,

### How to specify a context in a config.yaml

## Strategies

AgeToIso8601
AliasMap
DateToAge
Mapping
MultiHpoColExpansion
OntologyNormaliser