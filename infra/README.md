# Infrastructure — Reference IaC Snippets

The techniques in this playbook are platform-agnostic — they work on EMR, Glue, Databricks, Dataproc, and self-managed Kubernetes Spark. This directory contains **reference Terraform snippets** showing the tuning-relevant configuration for the most common platforms.

## What's included

- `emr.tf.example` — EMR cluster sized according to Step 7 of the [decision tree](../docs/DECISION_TREE.md) (4 cores × 8GB per executor, right-sized shuffle partitions)

## What's NOT included (and why)

- Full VPC / IAM / monitoring modules — see my [aws-glue-cdc-framework](https://github.com/sushmakl95/aws-glue-cdc-framework) repo for a 13-module Terraform deployment with all of that
- Databricks Workspace Terraform — workspace creation is a one-time manual setup on most accounts; job-level resource config is what matters for perf, and those settings go in `job.json` / dbx.yml

## Why snippets instead of modules

Because this repo's focus is **the techniques**, not the infrastructure. Recruiters looking at this repo want to see:
1. Do I understand Spark internals? (→ the benchmark scripts)
2. Can I explain *why* a technique works? (→ the docstrings + docs)
3. Do I know how to apply this on real infra? (→ these snippets)

The full module structure is already demonstrated in my other repos.

## Using the snippets

Copy the relevant `.tf.example` into your project's Terraform directory, rename to `.tf`, and fill in the commented placeholders.

The snippets deliberately include the **perf-relevant configuration** (executor sizing, instance types, AQE configs via `spark.*` properties) but omit environment-specific details (VPC IDs, subnet IDs, IAM roles) since those vary per deployment.

## ⚠️ Cost warning

EMR clusters bill by the second. A 4-node m5.xlarge cluster costs ~$1/hour. Always `terraform destroy` after testing.

For zero-cost demos of the techniques, stick to the local benchmark harness — it shows the same speedups.
