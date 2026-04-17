# Benchmark Claim

## Mainline Claim

This repo's mainline project is a patient- and cohort-level benchmark for
psychosis heterogeneity representations.

Its job is to determine which representations:

1. reproduce across independent datasets
2. improve prediction of intervention-relevant outcomes

The benchmark should compare diagnosis, dimensions, trajectory-aware summaries,
and simple clusters before expanding to biomarker-heavy or mechanism-heavy
claims.

## What This Repo Is

- a cross-cohort harmonization and evaluation framework
- a benchmark for comparing representations of psychosis heterogeneity
- a place to quantify whether added modalities improve on low-tech clinical
  representations
- an engine for producing explicit go / no-go decisions about which directions
  deserve deeper follow-up

## What This Repo Is Not

- a biomarker-discovery-first repo
- a novel subtype-discovery repo
- a target-prioritization repo
- a drug-discovery repo
- a single-cohort schizophrenia-only audit engine used as the main scientific
  claim

## Core Scientific Commitments

- real outcomes before synthetic proxy targets
- multi-cohort validation before strong structure claims
- simple clinical baselines before modality-heavy models
- explicit failure reporting, not just positive leaderboard outputs

## Allowed First-Phase Representation Families

- diagnosis-only
- symptom dimensions
- simple baseline clinical summaries
- simple trajectory-aware summaries where repeated measures support them
- simple clusters only as a comparator

## Deferred Until Later

- imaging or genetics as core model drivers
- mechanism claims
- biology-context layers as a project centerpiece
- target ranking
- de novo subtype claims from one cohort

## Handoff Rules For Future Agents

- treat this file and `docs/strict_open_pr_roadmap.md` as the current project
  framing
- treat old `strict_open` claims as superseded unless explicitly reused as
  infrastructure
- do not continue proxy-target work without a new decision that explicitly
  re-approves it
