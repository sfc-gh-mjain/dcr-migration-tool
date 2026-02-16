DCR Migration Tool

Version: 1.0.0
Target Architecture: Snowflake Collaboration Hub (API v2.0)
Source Architecture: Snowflake Native App Clean Rooms (Legacy Provider/Consumer)

Overview

The DCR Migration Tool is an automated engine designed to upgrade legacy Provider & Consumer (P&C) cleanrooms to the new Snowflake Collaboration Hub architecture. It abstracts the complexity of manually writing YAML specifications and API calls into a streamlined Plan → Execute → Finalize → Validate workflow.

The solution consists of:

Backend: A suite of Snowflake Stored Procedures (Python) that handle logic, spec generation, and API orchestration.

Frontend: A Streamlit App (running in Snowsight) that provides a user-friendly interface for the migration.

Features

Automated Discovery: Automatically detects if you are the Provider or Consumer of a cleanroom.

Spec Generation: Converts legacy SQL templates and table policies into v2.0 compliant YAML specifications.

Safety Guardrails: Pre-flight checks prevent migration of unsupported configurations (e.g., Python code, Multi-Provider).

Deterministic Versioning: Ensures Provider and Consumer generate matching IDs for shared artifacts without manual coordination.

Parity Validation: Verifies that the new Collaboration contains the same artifacts as the legacy cleanroom.

Prerequisites

Snowflake Account: Access to a Snowflake account with the Samooha Native App installed.

Role: You must execute the backend script and run the Streamlit app using the SAMOOHA_APP_ROLE.

Privileges: The role must have permissions to create databases/schemas (to install the tool) and call Native App procedures.

Installation

1. Deploy the Backend

Log in to Snowsight.

Open a new SQL Worksheet.

Copy the contents of migration_backend.sql.

Run the entire script ("Run All").

This will create the DCR_SNOWVA.MIGRATION schema and all necessary stored procedures.

2. Deploy the Streamlit App

In Snowsight, go to Streamlit.

Click + Streamlit App.

Name it "DCR Migration Tool".

Select a Warehouse and the Database/Schema where you deployed the backend (DCR_SNOWVA.MIGRATION).

Paste the contents of streamlit_app.py into the editor.

Click Run.

Usage Workflow

The tool follows a specific sequence to ensuring data integrity:

Phase 1: Configuration & Plan

Enter the Legacy Cleanroom Name (e.g., mj_act_uc) in the sidebar.

Click Generate Plan.

The tool scans the legacy environment and generates a migration plan (SQL script).

Review the summary (Role, Template count, Dataset count).

Phase 2: Execute Setup

Go to the "1. Execute Setup" tab.

Click Execute Setup Now.

Provider: This registers all templates/datasets and initializes the Collaboration.

Consumer: This registers any local datasets required for the cleanroom.

Phase 3: Finalize (Status & Join)

Snowflake requires manual confirmation for joining collaborations due to legal terms acceptance.

Go to the "2. Finalize" tab.

Click Check Status periodically until the status returns CREATED.

Click Join Collaboration.

Note: If the automated join fails (due to "Side Effects/Legal Terms"), the UI will provide a SQL snippet for you to run manually in a worksheet.

Phase 4: Validate

Go to the "Validate" tab.

Click Run Validation Check.

The tool compares the artifacts in the new Collaboration against the legacy Cleanroom to ensure 100% parity.

Troubleshooting

"Side Effects [SYSTEM$ACCEPT_LEGAL_TERMS]": This error occurs because Stored Procedures cannot accept legal terms on your behalf.

Fix: Copy the manual code provided in the "Finalize" tab and run it in a standard SQL Worksheet.

"Cleanroom not found": Ensure you are using the exact name of the legacy cleanroom and that you have SAMOOHA_APP_ROLE active.

"No data offerings found":

Provider: You must have data to migrate.

Consumer: This is normal; the tool will skip data registration and proceed to joining.

License

Proprietary - Internal Use Only.
