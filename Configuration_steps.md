# CareSync Health Network --- Azure Data Engineering Project

> Metadata-driven healthcare data platform using Azure Data Factory,
> ADLS Gen2, Azure SQL Database, Azure Key Vault, Azure Databricks,
> Delta Lake, Unity Catalog, Logic Apps and GitHub.

**Project type:** End-to-end batch data engineering platform\
**Domain:** Healthcare / multi-hospital network\
**Primary pattern:** Metadata-driven ingestion + Bronze/Silver/Gold
medallion architecture

------------------------------------------------------------------------

## 1. Project objective

CareSync receives operational data from hospitals and recurring partner
files. The source data is spread across systems, new data arrives
regularly, and manual reconciliation makes reporting slow and
error-prone.

This project builds a reusable platform that:

-   Ingests PostgreSQL tables and partner CSV files through a
    metadata-driven ADF framework.
-   Supports full, append and watermark-based incremental/merge loads.
-   Lands each batch in a timestamped ADLS Gen2 folder.
-   Transforms data with reusable Databricks PySpark notebooks and Delta
    tables.
-   Tracks each table, run and processing stage in Azure SQL audit
    tables.
-   Stores credentials in Key Vault and uses managed identities for
    Azure storage access.
-   Sends run summaries through Azure Logic Apps.
-   Uses GitHub branches and pull requests to manage ADF and Databricks
    changes.
-   Publishes curated Gold tables for dashboards and natural-language
    analytics.

## 2. High-level architecture

``` text
Neon PostgreSQL                         ADLS Gen2 / partner CSV
(hospitals, doctors, patients)          (lab_results, insurance_providers)
          |                                         |
          +--------------------+--------------------+
                               |
                       Azure Data Factory
                 metadata-driven ingestion pipelines
                               |
                     Azure SQL control database
                  table_config | watermark | audit_log
                               |
                       ADLS Gen2 / landing
                               |
                     Azure Databricks + Spark
                               |
                 Bronze  ->  Silver  ->  Gold
                  raw        clean      curated
                               |
              +----------------+----------------+
              |                |                |
        AI/BI Dashboard   Genie Agent        Genie ONE

Cross-cutting services:
- Azure Key Vault: secrets
- Managed identities + Unity Catalog: governed storage access
- Azure Logic Apps: email notifications
- GitHub: source control and CI/CD
```

### Main service responsibilities

  -----------------------------------------------------------------------
  Service                             Responsibility
  ----------------------------------- -----------------------------------
  Neon PostgreSQL                     Hospital operational source tables

  ADLS Gen2                           Partner CSV source files and
                                      timestamped landing data

  Azure Data Factory                  Copy, metadata lookup,
                                      orchestration and notebook
                                      execution

  Azure SQL Database                  Control metadata, watermarks and
                                      audit history

  Azure Databricks                    PySpark transformations and Delta
                                      Lake tables

  Unity Catalog                       Catalog, schemas, volumes and
                                      governed access

  Azure Key Vault                     Secrets used by linked services

  Azure Logic Apps                    HTTP-triggered email summary
                                      workflow

  GitHub                              Version control, pull requests and
                                      collaboration

  Databricks SQL / AI-BI              Gold-layer consumption
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## 3. Repository layout

Clone the project repository:

``` bash
git clone https://github.com/databeli/caresync_azure_project.git
cd caresync_azure_project
```

The repository contains the following major folders:

  -------------------------------------------------------------------------
  Folder                                Purpose
  ------------------------------------- -----------------------------------
  `01_Sources Data/`                    PostgreSQL source DDL/seed data and
                                        sample partner CSV files

  `02_Metadata Table Queries/`          SQL scripts for
                                        `ctrl.table_config`,
                                        `ctrl.watermark`, and
                                        `ctrl.audit_log`

  `03_Gold STTM/`                       Source-to-target mapping workbook
                                        for Gold reporting tables

  `04_Azure Data Factory Repository/`   ADF pipelines, datasets, linked
                                        services and factory configuration

  `05_Databricks_Repository/`           Bronze/Silver notebooks, calendar
                                        notebook and Gold notebooks

  `assets/`                             Project diagrams and supporting
                                        assets

  `README.md`                           Repository overview
  -------------------------------------------------------------------------

Use the SQL scripts and notebooks supplied in these folders as the
implementation source of truth. This README explains the environment
setup and how the pieces connect; it does not replace the supplied
project artifacts.

------------------------------------------------------------------------

## 4. Prerequisites

Before starting, arrange:

1.  An Azure subscription with permission to create resource groups and
    the required services.
2.  A GitHub account.
3.  A Neon account for the PostgreSQL source.
4.  An Azure Databricks workspace that supports Unity Catalog.
5.  Basic familiarity with ADF, SQL, PySpark and Git.

You will need permission to: - Create Azure resources and role
assignments. - Configure managed identities and access connectors. -
Create Azure SQL databases and Key Vault secrets. - Configure ADF linked
services and Git integration. - Create Unity Catalog storage
credentials, external locations, schemas and volumes.

**Cost note:** Azure services, Databricks compute, storage, SQL and
Logic Apps may incur charges. Free credits and service availability
change over time. Check current pricing, quotas and regional
availability before deployment. Stop or terminate interactive compute
when it is not needed.

------------------------------------------------------------------------

## 5. Choose a naming convention

The project documentation uses names such as the following. Azure
resource names must be globally unique where Azure requires it; adjust
names if they are already taken.

  -----------------------------------------------------------------------
  Resource                            Example name
  ----------------------------------- -----------------------------------
  Resource group                      `rg-caresync-dev`

  Azure Data Factory                  `adf-caresync-dev`

  ADLS Gen2 storage account           `caresync<unique-suffix>`

  ADLS source container               `source`

  ADLS landing container              `landing`

  Azure SQL server/database           Your chosen SQL server / CareSync
                                      metadata DB

  Key Vault                           `caresync-keyvault`

  Databricks workspace                `adb-caresync`

  Databricks cluster                  `caresync_cluster`

  Unity Catalog catalog               `adb_caresync`

  ADF Git repository                  `caresync_adf_repo`

  Databricks Git repository           `caresync-adb-repo`
  -----------------------------------------------------------------------

The tutorial PDF uses a few different display names in different
sections. Keep a single consistent set of names in your own environment
and record them in your deployment notes.

------------------------------------------------------------------------

## 6. Provision Azure resources

Create the resources in a suitable region close to your users and data
sources. The tutorial demonstrates Central India; choose a region where
the required services and Databricks features are available.

### 6.1 Resource group

Azure Portal → Resource groups → Create.

-   Subscription: your subscription
-   Resource group: `rg-caresync-dev`
-   Region: selected Azure region

Keep the project's resources in this resource group where practical.

### 6.2 Azure Data Factory

Azure Portal → Create a resource → Data Factory.

-   Name: `adf-caresync-dev`
-   Version: V2
-   Resource group: `rg-caresync-dev`
-   Region: selected region

After deployment, open the resource and select **Launch Studio**.

Enable the ADF system-assigned managed identity. It will be used to
access ADLS and Key Vault after the relevant RBAC assignments are made.

### 6.3 ADLS Gen2

Azure Portal → Storage accounts → Create.

-   Performance: Standard for the learning setup
-   Redundancy: choose based on cost and availability requirements
-   Hierarchical namespace: **Enabled**
-   Account kind: StorageV2 / ADLS Gen2-compatible configuration

Enabling hierarchical namespace is important: it provides the directory
and filesystem semantics used by ADLS Gen2.

Create these containers:

``` text
source/
landing/
```

-   `source`: partner CSV files are uploaded here.
-   `landing`: ADF writes each extracted source batch here.
-   Unity Catalog may also need a separate container/path for the
    catalog's managed/default storage. Configure this separately rather
    than mixing it with the operational landing folders.

### 6.4 Grant ADF access to ADLS

Storage account → Access Control (IAM) → Add role assignment.

-   Role: `Storage Blob Data Contributor`
-   Assign access to: Managed identity
-   Managed identity: the CareSync ADF instance
-   Scope: storage account (or a narrower scope if your design uses it)

This allows ADF to read and write blobs without storing a storage
account key in a linked service.

### 6.5 Neon PostgreSQL

Create a Neon project and database.

-   Project: `caresync`
-   Region: select a region suitable for your environment.
-   Create or use the project database and a dedicated database user.

From Neon, copy the connection details. You will need:

-   Host/server name
-   Database name
-   Username
-   Password
-   Port (normally PostgreSQL's standard port unless Neon specifies
    otherwise)
-   SSL requirement / connection options from the Neon connection string

Do not commit the connection string or password to GitHub.

The source tables are:

-   `hospitals`
-   `doctors`
-   `patients`

Use the DDL and seed scripts in `01_Sources Data/01_postgres/` to create
and populate them.

### 6.6 Azure Key Vault

Create a Key Vault in the same project resource group.

Example name: `caresync-keyvault`.

Enable the ADF system-assigned identity and grant it the permissions
required to retrieve secrets. Prefer the least-privilege Key Vault
data-plane role appropriate to your access model (for example, **Key
Vault Secrets User** for reading secrets). Avoid granting broad
administrator permissions merely to make a connection test pass.

Create the secrets used by the project:

  -----------------------------------------------------------------------
  Secret name                         Value
  ----------------------------------- -----------------------------------
  `postgres-password`                 Neon PostgreSQL password

  `metadata-db-password`              Azure SQL login password, if using
                                      SQL authentication

  `adb-token`                         Databricks personal access token
                                      used by the ADF Databricks linked
                                      service
  -----------------------------------------------------------------------

The PDF refers to these three secret names. If you use a different
authentication method, update the linked service configuration
accordingly.

### 6.7 Azure SQL Database for metadata and audit

Create an Azure SQL logical server and database for the project's
control framework.

-   Database purpose: ADF metadata, watermarks and audit records.
-   Create the `ctrl` schema.
-   Run the SQL scripts from `02_Metadata Table Queries/` in the order
    provided by the repository.

The three control tables are:

1.  `ctrl.table_config`
2.  `ctrl.watermark`
3.  `ctrl.audit_log`

Use a dedicated SQL user or Microsoft Entra authentication according to
your environment. If using SQL authentication, store the password in Key
Vault.

------------------------------------------------------------------------

## 7. Configure ADF linked services

Create the following linked services in ADF Studio → Manage → Linked
services.

  -----------------------------------------------------------------------------
  Linked service          Connector         Connects to       Authentication /
                                                              credential
  ----------------------- ----------------- ----------------- -----------------
  `LS_PostgreSql`         PostgreSQL        Neon source       User/password
                                                              retrieved
                                                              securely from Key
                                                              Vault

  `LS_ADLS`               Azure Data Lake   ADLS source and   ADF
                          Storage Gen2      landing           system-assigned
                                                              managed identity

  `LS_AzureDatabricks`    Azure Databricks  Databricks        Token retrieved
                                            workspace and     from Key Vault in
                                            compute           the documented
                                                              setup

  `LS_AzureSqlDatabase`   Azure SQL         Metadata database Configured
                          Database                            SQL/Entra
                                                              authentication;
                                                              secret from Key
                                                              Vault if SQL auth

  `LS_AzureKeyVault`      Azure Key Vault   Key Vault         ADF
                                                              system-assigned
                                                              managed identity
  -----------------------------------------------------------------------------

For each linked service:

1.  Select the correct connector.
2.  Enter the endpoint/workspace/database details.
3.  Select the intended authentication method.
4.  Reference Key Vault secrets rather than typing passwords directly
    into pipeline JSON.
5.  Test the connection.
6.  Save and publish through the configured Git workflow.

**Important:** The exact linked service fields depend on the connector
version and authentication method. Use the actual host, database,
workspace URL and secret names from your environment; never copy another
person's credentials.

------------------------------------------------------------------------

## 8. ADF GitHub integration

Connect ADF Studio to GitHub before building pipelines.

ADF Studio → Manage → Git configuration → Configure.

Example configuration:

  Setting                Value
  ---------------------- ---------------------
  Repository type        GitHub
  Repository             `caresync_adf_repo`
  Collaboration branch   `main`
  Publish branch         `adf_publish`
  Root folder            `/`

Create and initialize the GitHub repository with a `README.md` so that
the `main` branch exists before connecting it.

-   `main`: collaboration branch.
-   Feature branches: development work.
-   `adf_publish`: ADF-generated publish artifacts. Do not manually edit
    this branch.

Use feature branches and pull requests for changes. Publish from `main`
after the changes have been reviewed and merged.

------------------------------------------------------------------------

## 9. Prepare source data

### 9.1 PostgreSQL tables

The internal hospital source contains:

  -----------------------------------------------------------------------
  Table                   Purpose                 Load behavior
  ----------------------- ----------------------- -----------------------
  `hospitals`             Hospital details,       FULL
                          location, type and bed  
                          count                   

  `doctors`               Doctor details linked   FULL
                          to hospitals            

  `patients`              Patient records linked  MERGE using
                          to hospitals and        `updated_at` watermark
                          insurance providers     
  -----------------------------------------------------------------------

The `patients` table uses `updated_at` for incremental extraction. The
project treats hospitals and doctors as full-reload tables.

Run the source DDL and seed scripts from the repository rather than
manually creating a different schema.

### 9.2 Partner CSV files

The external feeds are:

  -----------------------------------------------------------------------
  File/table              Purpose                 Load behavior
  ----------------------- ----------------------- -----------------------
  `lab_results`           Patient lab test        APPEND
                          results, status, value  
                          and reported date       

  `insurance_providers`   Insurance provider      FULL
                          details and coverage    
                          region                  
  -----------------------------------------------------------------------

Upload the sample CSV files from `01_Sources Data/02_adls_csv/` to the
ADLS `source` container, preserving the folder/file organization
expected by the ADF datasets and metadata.

------------------------------------------------------------------------

## 10. Create the metadata control framework

The `ctrl` schema in Azure SQL drives the generic ingestion process.
Instead of creating one pipeline for every source table, ADF reads
configuration rows and executes the appropriate path.

### 10.1 `ctrl.table_config`

One configuration row represents a source object or a Gold target.

Important configuration concepts include:

-   Source system
-   Source schema and source table
-   Source file format and source path
-   Bronze, Silver and Gold table names
-   Load type
-   Watermark column
-   Merge keys
-   Processing stage
-   Active/inactive flag

The project has eight configured entries:

  ------------------------------------------------------------------------------------------------------------
  Source / target            Source system     Load type   Watermark       Merge key      Stage
  -------------------------- ----------------- ----------- --------------- -------------- --------------------
  `hospitals`                `neon_postgres`   FULL        ---             ---            `SOURCE_TO_SILVER`

  `doctors`                  `neon_postgres`   FULL        ---             ---            `SOURCE_TO_SILVER`

  `patients`                 `neon_postgres`   MERGE       `updated_at`    `patient_id`   `SOURCE_TO_SILVER`

  `lab_results`              `adls_csv`        APPEND      `reported_at`   ---            `SOURCE_TO_SILVER`

  `insurance_providers`      `adls_csv`        FULL        ---             ---            `SOURCE_TO_SILVER`

  `patient_360`              `silver`          FULL        ---             ---            `SILVER_TO_GOLD`

  `hospital_daily_summary`   `silver`          FULL        ---             ---            `SILVER_TO_GOLD`

  `lab_test_trends`          `silver`          FULL        ---             ---            `SILVER_TO_GOLD`
  ------------------------------------------------------------------------------------------------------------

`is_active` controls whether a configuration row is picked up by the
pipeline. Deactivating a source should not require changing pipeline
code.

### 10.2 `ctrl.watermark`

Stores the last processed watermark value for incremental tables.

The project seeds the incremental entries for `patients` and
`lab_results` at `1900-01-01`, so the first run can capture the
available history. Subsequent successful runs advance the watermark.

Conceptually:

``` sql
-- Illustrative watermark extraction logic
SELECT *
FROM source_schema.source_table
WHERE watermark_column > last_successful_watermark;
```

Use the actual SQL scripts and dynamic ADF expressions supplied in the
repository for executable implementation.

### 10.3 `ctrl.audit_log`

Tracks processing at table/run/stage level.

Statuses used by the project:

-   `IN_PROGRESS`
-   `SUCCESS`
-   `FAILED`

The audit record includes the run/table/stage context, timestamps,
status and `records_written` returned by the Databricks notebook. A
shared ADF run ID is passed through the pipeline hierarchy to correlate
a complete run.

### 10.4 Control-table relationships

``` text
table_config (1) -------- (0..1) watermark
     |
     |
     +------------------- (many) audit_log
```

A table configuration can have multiple audit records over time.
Incremental tables have a corresponding watermark record.

------------------------------------------------------------------------

## 11. ADLS landing-zone design

Every ingestion batch gets a timestamped folder:

``` text
landing/
└── <source_system>/
    └── <table_name>/
        └── <landing_timestamp>/
            └── <file>
```

Examples:

``` text
landing/neon_postgres/hospitals/2026-11-10_09-00-00/data.parquet
landing/adls_csv/lab_results/2026-11-10_09-00-00/lab_results.csv
```

The examples above illustrate the folder pattern; use the timestamp
format generated by the actual ADF pipeline.

-   PostgreSQL tables are copied to Parquet.
-   Partner CSV files are copied as CSV/binary without converting the
    source file during ingestion.
-   Each run writes to a new timestamped folder rather than overwriting
    a shared landing path.
-   The `landing_timestamp` is passed to Databricks so each layer can
    identify the current batch.

------------------------------------------------------------------------

## 12. Configure Azure Databricks and Unity Catalog

### 12.1 Workspace and compute

Create the Databricks workspace in the selected Azure region and
enable/configure Unity Catalog.

The project documentation uses:

-   Workspace: `adb-caresync`
-   Interactive cluster: `caresync_cluster`
-   Catalog: `adb_caresync`

The interactive cluster is suitable for following the learning workflow.
For production, evaluate job compute/serverless options, policies,
autoscaling, access controls and cost.

### 12.2 Storage access through Unity Catalog

The intended access chain is:

``` text
Azure Databricks Access Connector
          |
          | Azure RBAC:
          | Storage Blob Data Contributor
          v
      ADLS Gen2
          |
          v
Unity Catalog Storage Credential
          |
          v
Unity Catalog External Location
          |
          v
Unity Catalog Volume
          |
          v
Databricks notebooks
```

Steps:

1.  Create or identify the Databricks access connector associated with
    the workspace.
2.  Grant its managed identity `Storage Blob Data Contributor` on the
    required storage scope.
3.  In Unity Catalog, create a storage credential using the access
    connector.
4.  Create an external location pointing to the intended ADLS
    container/path.
5.  Create the required external volume(s) on the external location.
6.  Repeat for the landing data and catalog-managed/default storage
    paths as required by the workspace setup.
7.  Grant the relevant catalog/schema/volume permissions to the notebook
    users or service principals.

Do not use storage account keys in notebook code.

### 12.3 Catalog and schemas

The project uses one catalog with layer schemas:

``` text
adb_caresync
├── landing
├── bronze
├── silver
└── gold
```

The `landing` schema exposes the ADLS landing path through a Unity
Catalog volume. Bronze, Silver and Gold contain Delta tables.

Create the catalog and schemas using the SQL/Unity Catalog workflow
supported by your workspace. Verify permissions by reading and writing a
small test file through the volume before running the full pipeline.

### 12.4 Databricks Git folder

Connect the workspace to the repository `caresync-adb-repo`.

The shared Git folder contains the notebook paths called by ADF. For
development, use an individual clone/checkout and feature branch; after
a pull request is merged, pull the updated `main` into the shared Git
folder that the pipelines execute.

------------------------------------------------------------------------

## 13. Databricks transformation notebooks

The repository includes reusable notebooks for each layer.

### 13.1 Landing to Bronze

Notebook: `Landing_to_Bronze`

Parameters/widgets:

-   `source_system`
-   `table_name`
-   `landing_timestamp`
-   `source_file_format`

Processing:

1.  Build the landing path from the supplied parameters.
2.  Check whether the expected batch path exists.
3.  If no new data exists, exit cleanly rather than failing the entire
    run.
4.  Read CSV or Parquet according to `source_file_format`.
5.  Add `landing_timestamp` and `insert_timestamp`.
6.  Append the rows to `adb_caresync.bronze.<table_name>`.
7.  Return the number of rows written using `dbutils.notebook.exit()`.

Bronze is append-only and preserves the raw history of every landed
batch.

### 13.2 Bronze to Silver

Notebook: `Bronze_to_Silver`

Parameters/widgets:

-   `landing_timestamp`
-   `table_name`
-   `merge_keys`
-   `load_type`

Read only the Bronze records for the current `landing_timestamp`. Apply
the write strategy from metadata:

  -----------------------------------------------------------------------
  Load type               Silver behavior         Example
  ----------------------- ----------------------- -----------------------
  FULL                    Overwrite current       hospitals, doctors,
                          Silver table            insurance_providers

  APPEND                  Append the new batch    lab_results

  MERGE                   Upsert using configured patients
                          key(s)                  
  -----------------------------------------------------------------------

For MERGE: - If the target table does not exist, initialize it from the
batch. - If it exists, match on `merge_keys`. - Update matched business
columns while preserving the original `insert_timestamp`. - Insert
unmatched rows. - Return the processed row count.

### 13.3 Calendar dimension

Notebook: `Prepare calendar table`

Creates `adb_caresync.silver.calendar` for the date range documented in
the project (1940-01-01 through 2030-12-31).

The dimension includes derived attributes such as year, quarter, month,
week, day name, weekend flag, period start/end dates and leap-year flag.
Gold aggregations use this table for date enrichment.

### 13.4 Gold notebooks

The three Gold notebooks are:

  -----------------------------------------------------------------------------
  Notebook / table              Grain                   Business purpose
  ----------------------------- ----------------------- -----------------------
  `NB_patient_360` /            One row per patient     Patient profile
  `patient_360`                                         enriched with hospital
                                                        and insurance details
                                                        plus lab-result counts
                                                        and latest test date

  `NB_hospital_daily_summary` / One row per hospital    Daily test volume and
  `hospital_daily_summary`      per day                 day-over-day change

  `NB_lab_test_trends` /        One row per test per    Monthly test trends and
  `lab_test_trends`             month                   month-over-month
                                                        abnormal-result-rate
                                                        change
  -----------------------------------------------------------------------------

Use `03_Gold STTM/` as the source-to-target mapping reference. The Gold
tables are full rebuilds in this project.

------------------------------------------------------------------------

## 14. ADF pipeline hierarchy

The orchestration layer has four main pipelines.

### 14.1 `PL_SOURCE_TO_SILVER_MAIN`

Parameters:

-   `source_system`
-   `adf_run_id`

Responsibilities:

1.  Read active `SOURCE_TO_SILVER` configuration rows for the supplied
    source system.
2.  Loop through the rows using ForEach.
3.  Insert an `IN_PROGRESS` audit record.
4.  Execute `PL_SOURCE_TO_SILVER_INNER` for each table.
5.  On failure, update the audit record to `FAILED` and fail the parent
    execution.

The master pipeline invokes this pipeline once for PostgreSQL and once
for CSV.

### 14.2 `PL_SOURCE_TO_SILVER_INNER`

Parameters:

-   `table_id`
-   `adf_run_id`

Responsibilities:

1.  Read the selected row from `ctrl.table_config`.
2.  Set the batch/landing timestamp.
3.  For incremental loads, read the current value from `ctrl.watermark`.
4.  Use a source-system Switch to choose PostgreSQL or ADLS CSV
    ingestion.
5.  Copy the data to the timestamped landing path.
6.  Execute `Landing_to_Bronze`.
7.  Execute `Bronze_to_Silver`.
8.  Update the audit record with status and row count.
9.  Advance the watermark only after successful processing.

### 14.3 `PL_SILVER_TO_GOLD`

Parameter:

-   `adf_run_id`

Responsibilities:

1.  Read active rows where `stage = 'SILVER_TO_GOLD'`.
2.  Loop through the configured Gold targets.
3.  Insert an in-progress audit record.
4.  Resolve the Gold notebook path dynamically from the metadata row.
5.  Execute the notebook.
6.  Update success/failure status and row count.

### 14.4 `PL_MASTER`

Entry point for the complete run.

``` text
PL_MASTER
   |
   +---- PL_SOURCE_TO_SILVER_MAIN (neon_postgres) ----+
   |                                                  |
   +---- PL_SOURCE_TO_SILVER_MAIN (adls_csv) ---------+
                         |
                  PL_SILVER_TO_GOLD
                         |
                   PL_SEND_EMAIL
```

The two source ingestion paths run in parallel. The same
`@pipeline().RunId` is passed as `adf_run_id` through child pipeline
calls so audit rows can be correlated.

The documented design proceeds to Gold after the source branches
complete, then sends the summary email. Review the actual dependency
conditions in the supplied ADF JSON before adopting this behavior for
production: decide explicitly whether Gold should run after any upstream
failure or only after successful source processing.

------------------------------------------------------------------------

## 15. Logic Apps email notifications

Create an Azure Logic App with an HTTP request trigger and an email
action.

The documented workflow has four steps and receives a payload from ADF.
It creates a run summary, including status information, and sends a
formatted email.

Configuration sequence:

1.  Create the Logic App.
2.  Add the HTTP request trigger.
3.  Save the workflow and copy its HTTP POST URL.
4.  Configure the email connector/action and authorize the mailbox.
5.  Build the request schema from the payload sent by ADF.
6.  Format the audit results as an HTML table.
7.  Save the workflow and test it with a sample request.
8.  Store the HTTP trigger URL in an ADF global parameter; do not
    hardcode it in multiple pipelines.

ADF global parameters documented for this project:

  -----------------------------------------------------------------------
  Parameter               Type                    Purpose
  ----------------------- ----------------------- -----------------------
  `email_receipent`       String                  Email recipient (the
                                                  source document spells
                                                  it this way; preserve
                                                  the exact name if
                                                  existing expressions
                                                  use it)

  `env`                   String                  DEV/PROD environment
                                                  label in subject

  `logic_apps_url`        String                  Logic App HTTP trigger
                                                  URL
  -----------------------------------------------------------------------

The Logic App URL contains a SAS signature. Treat it as a secret. Do not
commit it to GitHub or paste it into public logs.

The send-email pipeline queries the audit table for the current run and
calls the Logic App. Test both success and failure email paths.

------------------------------------------------------------------------

## 16. GitHub CI/CD workflow

### 16.1 Azure Data Factory

1.  Create a feature branch from `main`.
2.  Open ADF Studio and switch to that branch.
3.  Make pipeline, dataset or linked-service changes.
4.  Commit and push.
5.  Open a pull request into `main`.
6.  Review and merge.
7.  Switch ADF Studio to `main`.
8.  Validate and publish.
9.  Allow ADF to update its generated `adf_publish` branch.

Do not directly edit the generated publish branch.

### 16.2 Databricks

1.  Clone `caresync-adb-repo` into your own user Git folder.
2.  Create a feature branch.
3.  Edit notebooks and test them.
4.  Commit and push.
5.  Open a pull request into `main`.
6.  Merge after review.
7.  Open the shared Git folder used by ADF.
8.  Pull the latest `main` so pipeline notebook paths execute the merged
    code.

The individual development checkout and shared execution checkout serve
different purposes.

------------------------------------------------------------------------

## 17. Scheduling and execution

After validating a complete manual run:

1.  Open `PL_MASTER` in ADF.
2.  Validate all activities and linked services.
3.  Run a Debug/triggered test with a known dataset.
4.  Check landing files in ADLS.
5.  Check Bronze and Silver Delta tables.
6.  Check Gold output tables.
7.  Check `ctrl.audit_log` for the current ADF run ID.
8.  Verify the Logic App email.
9.  Configure the required daily schedule trigger.
10. Monitor ADF pipeline runs and Databricks job/notebook runs.

The tutorial demonstrates scheduling the master pipeline daily. Choose
the actual execution time and timezone based on your data arrival SLA.

------------------------------------------------------------------------

## 18. Validation checklist

### Infrastructure

-   [ ] Resource group and ADF deployed.
-   [ ] ADLS Gen2 hierarchical namespace enabled.
-   [ ] `source` and `landing` containers created.
-   [ ] ADF managed identity has required storage access.
-   [ ] Neon PostgreSQL is reachable from the selected integration
    runtime/network.
-   [ ] Azure SQL metadata database is reachable.
-   [ ] Key Vault secrets created and ADF can retrieve them.
-   [ ] Databricks workspace and compute are available.
-   [ ] Unity Catalog storage credential, external location and volumes
    work.

### Data and metadata

-   [ ] PostgreSQL source tables created and seeded.
-   [ ] Partner CSV files uploaded.
-   [ ] `ctrl.table_config` has the expected active rows.
-   [ ] Watermark rows exist for incremental tables.
-   [ ] Audit table accepts run records.
-   [ ] ADF linked-service test connections succeed.

### Pipeline and transformations

-   [ ] Full load for hospitals and doctors works.
-   [ ] Patient incremental extraction uses `updated_at`.
-   [ ] Patient Silver MERGE uses `patient_id`.
-   [ ] Lab results append without overwriting prior records.
-   [ ] Insurance providers full load works.
-   [ ] Empty incremental batch exits cleanly.
-   [ ] Bronze preserves multiple timestamped batches.
-   [ ] Silver contains the expected current/accumulated state per load
    type.
-   [ ] Gold notebooks create all three reporting tables.
-   [ ] Audit row counts and statuses are correct.
-   [ ] Failure email and success email both work.

### Delivery and governance

-   [ ] ADF Git integration works.
-   [ ] Databricks Git folder works.
-   [ ] Feature branch and PR workflow tested.
-   [ ] No secrets committed to Git.
-   [ ] Unity Catalog permissions tested with intended users.
-   [ ] Daily master pipeline schedule configured.
-   [ ] Compute is stopped/terminated when not required.

------------------------------------------------------------------------

## 19. Common troubleshooting

  -----------------------------------------------------------------------
  Symptom                             What to verify
  ----------------------------------- -----------------------------------
  ADF cannot access ADLS              Storage Blob Data Contributor
                                      assignment, correct managed
                                      identity, scope and RBAC
                                      propagation

  ADF Key Vault connection fails      Key Vault identity permissions,
                                      secret name, vault URI and
                                      network/firewall settings

  PostgreSQL connection fails         Host, database, username, password,
                                      SSL settings, firewall/network
                                      route and integration runtime

  ADF cannot connect to Azure SQL     Server firewall/private endpoint,
                                      authentication, database name and
                                      SQL permissions

  Databricks notebook cannot read     Access connector RBAC, Unity
  landing files                       Catalog storage credential,
                                      external location, volume path and
                                      grants

  Notebook parameters are empty       ADF base parameter names must
                                      exactly match Databricks widget
                                      names

  Bronze has duplicate batches        Check landing timestamp generation
                                      and whether the same batch was
                                      intentionally rerun

  Silver MERGE fails                  Verify merge key exists, key data
                                      types match and source batch does
                                      not contain ambiguous duplicate
                                      keys

  Watermark skips records             Check watermark column type,
                                      comparison operator,
                                      timezone/precision and when
                                      watermark is advanced

  Audit says success after child      Ensure failure dependency path
  failure                             updates audit and propagates
                                      failure to the parent pipeline

  Email does not arrive               Check Logic App run history, HTTP
                                      payload/schema, trigger URL, email
                                      connector authorization and
                                      recipient

  Git changes are missing in pipeline Confirm the PR is merged and the
  runs                                shared Databricks Git folder has
                                      pulled `main`
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## 20. Load strategy: when to use each

  -----------------------------------------------------------------------
  Strategy                Meaning                 CareSync example
  ----------------------- ----------------------- -----------------------
  FULL                    Reload the complete     hospitals, doctors,
                          source and replace the  insurance_providers
                          Silver target           

  APPEND                  Add only new records;   lab_results
                          keep previous records   

  MERGE                   Insert new keys and     patients
                          update existing keys    
  -----------------------------------------------------------------------

**Watermark principle:** persist the last successfully processed source
value. Do not advance the watermark before the corresponding data load
has completed successfully. For production systems, also plan for
late-arriving records, timestamp precision, deletes, retries and
idempotency.

------------------------------------------------------------------------

## 21. AWS-to-Azure concept mapping

For engineers familiar with AWS:

  -----------------------------------------------------------------------
  AWS concept                         Azure equivalent / closest project
                                      component
  ----------------------------------- -----------------------------------
  S3                                  ADLS Gen2 / Blob Storage

  AWS Glue workflows / Step Functions ADF pipelines (not a one-to-one
                                      equivalent)

  Glue Data Catalog                   Unity Catalog (broader governance
                                      and access capabilities)

  IAM role for service                Managed identity + Azure RBAC

  Secrets Manager                     Azure Key Vault

  CloudWatch alarms / notifications   Azure Monitor + Logic Apps
                                      (different service boundaries)

  CodeCommit / CodePipeline           GitHub-based source control and
                                      CI/CD in this project

  RDS PostgreSQL                      Neon PostgreSQL in this learning
                                      project; Azure Database for
                                      PostgreSQL is an Azure-managed
                                      alternative
  -----------------------------------------------------------------------

The mapping is conceptual, not an assertion that the services have
identical features.

------------------------------------------------------------------------

## 22. Interview explanation

**Short project explanation**

"I built a metadata-driven healthcare data platform on Azure for a
multi-hospital network. Data comes from Neon PostgreSQL and external
partner CSV feeds stored in ADLS Gen2. Azure Data Factory reads a SQL
control table and uses reusable pipelines to perform full, append and
watermark-based incremental loads. Data is landed in timestamped folders
and transformed in Databricks using PySpark and Delta Lake through
Bronze, Silver and Gold layers. Azure SQL maintains configuration,
watermark and audit records. Unity Catalog and managed identities govern
storage access, Key Vault protects credentials, and Logic Apps sends
run-summary emails. Both ADF and Databricks code are managed through
GitHub feature branches and pull requests. Gold tables are consumed
through Databricks dashboards and Genie."

### Important interview topics

-   Why metadata-driven pipelines instead of one pipeline per table?
-   Difference between FULL, APPEND and MERGE.
-   How watermark-based incremental extraction works.
-   Why Bronze is append-only while Silver maintains the required
    current state.
-   How Delta Lake `MERGE INTO` works.
-   How ADF passes parameters to Databricks widgets.
-   How the ADF run ID correlates audit records.
-   How managed identity and Unity Catalog avoid storage keys.
-   How Key Vault secrets are referenced by linked services.
-   How failures propagate through nested ADF pipelines.
-   How GitHub PRs and ADF publish work.
-   How late-arriving data and retries affect watermark design.

------------------------------------------------------------------------

## 23. Project references

-   **Source repository:**
    https://github.com/databeli/caresync_azure_project
-   **Project documentation:** CareSync Azure Data Engineering PDF
    supplied alongside this README.
-   **Video walkthrough:** Search for the CareSync Health Network
    metadata-driven Azure Data Engineering project by DataBeli.

The repository README describes the source-data, metadata SQL, Gold
STTM, ADF and Databricks folders. Follow the scripts and JSON/notebook
artifacts in those folders for exact implementation details.

------------------------------------------------------------------------

## 24. Final notes

-   Start with a development resource group and synthetic/sample
    healthcare data.
-   Never upload real patient information to a learning environment.
-   Do not commit passwords, access tokens, connection strings or Logic
    App SAS URLs.
-   Use least-privilege RBAC and Unity Catalog grants.
-   Review Azure and Databricks pricing before leaving compute or
    scheduled resources running.
-   Validate the supplied pipeline dependency conditions before using
    the design for production.
