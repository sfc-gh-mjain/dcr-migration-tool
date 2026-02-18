import streamlit as st
import snowflake.snowpark as snowpark
import json
import pandas as pd
import time
from snowflake.snowpark.context import get_active_session

# --- PAGE CONFIG & CSS ---
st.set_page_config(layout="wide", page_title="DCR Migration Tool", page_icon="❄️")


st.markdown("""
<style>
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: transparent;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] {
        background-color: rgba(41, 181, 232, 0.1);
        border-bottom: 2px solid #29B5E8;
    }
    .metric-container {
        border: 1px solid #e0e0e0;
        padding: 10px;
        border-radius: 5px;
        text-align: center;
        background-color: #0e1117;

    
    [data-testid="stMetricValue"] {
        font-size: 24px !important;
    }

    }
</style>
""", unsafe_allow_html=True)

# --- BACKEND FUNCTIONS ---

def get_session():
    try:
        return get_active_session()
    except:
        return None

session = get_session()

def list_cleanrooms():
    """Fetch available cleanrooms for the picker."""
    rooms = []
    try:
        p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
        if p_res:
            for r in p_res:
                d = {k.upper(): v for k, v in r.as_dict().items()}
                name = d.get('CLEANROOM_NAME') or d.get('NAME')
                cid = d.get('CLEANROOM_ID') or d.get('ID')
                state = d.get('STATE') or d.get('STATUS') or ''
                is_api = str(name).upper().replace(' ', '_') == str(cid).upper().replace(' ', '_') if name and cid else False
                rooms.append({"name": name, "role": "PROVIDER", "state": state, "api_room": is_api})
    except:
        pass
    try:
        c_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.VIEW_CLEANROOMS()").collect()
        if c_res:
            existing_names = {r['name'].upper() for r in rooms if r.get('name')}
            for r in c_res:
                d = {k.upper(): v for k, v in r.as_dict().items()}
                name = d.get('CLEANROOM_NAME') or d.get('NAME')
                state = d.get('STATE') or d.get('STATUS') or ''
                if name and name.upper() not in existing_names:
                    rooms.append({"name": name, "role": "CONSUMER", "state": state, "api_room": True})
    except:
        pass
    return rooms

def get_migration_plan(cleanroom_name):
    try:
        res_str = session.call("DCR_SNOWVA.MIGRATION.AGENT_MIGRATE_ORCHESTRATOR", cleanroom_name, 'PLAN')
        if not res_str: return {"status": "ERROR", "message": "Empty response from backend."}
        plan = json.loads(res_str)
        
        if plan.get("status") == "ERROR":
            msg = plan.get('message', '')
            if "not found" in msg.lower() or "not installed" in msg.lower() or "CleanroomNotInstalled" in msg:
                st.error(f"Cleanroom '{cleanroom_name}' was not found. Please verify the cleanroom name is correct (use the P&C API name, not a UUID). Use the 'List Cleanrooms' button to see available rooms.")
            elif "ui-created" in msg.lower() or "ui cleanroom" in msg.lower():
                st.error(f"Cleanroom '{cleanroom_name}' is a UI-created cleanroom. Migration of UI cleanrooms is not supported in this release.")
            elif "laf" in msg.lower():
                st.error(f"Cleanroom '{cleanroom_name}' uses LAF (Cross-Cloud Auto-Fulfillment). LAF cleanroom migration is not supported.")
            elif "prerequisites" in msg.lower():
                st.error(f"Prerequisites failed: {msg}")
            else:
                st.error(f"Migration Error: {msg}")
            warnings = plan.get('warnings', [])
            for w in warnings:
                st.warning(w)
            return None
        
        plan['cleanroom_name'] = cleanroom_name
        return plan
    except Exception as e:
        st.error(f"Orchestration Error: {e}")
        return None

def execute_migration(cleanroom_name):
    try:
        res_str = session.call("DCR_SNOWVA.MIGRATION.AGENT_MIGRATE_ORCHESTRATOR", cleanroom_name, 'EXECUTE')
        return json.loads(res_str)
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

def initialize_collaboration(collab_spec):
    """Call INITIALIZE directly from Streamlit (not inside a stored procedure)."""
    try:
        spec = collab_spec.strip()
        res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.INITIALIZE($$\n{spec}\n$$)").collect()
        collab_name = ""
        msg = ""
        if res:
            rd = {k.upper(): v for k, v in res[0].as_dict().items()}
            collab_name = rd.get('COLLABORATION_NAME', '')
            msg = rd.get('MESSAGE', str(rd))
        return {"status": "SUCCESS", "message": msg, "collaboration_name": collab_name}
    except Exception as e:
        err = str(e)
        if "already exists" in err.lower():
            return {"status": "SUCCESS", "message": "Collaboration already exists.", "already_exists": True}
        return {"status": "ERROR", "message": err}

def review_collaboration(collab_name, owner_account):
    """Call REVIEW directly from Streamlit."""
    try:
        if owner_account:
            session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.REVIEW('{collab_name}', '{owner_account}')").collect()
        else:
            session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.REVIEW('{collab_name}')").collect()
        return {"status": "SUCCESS", "message": "Review complete."}
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

def join_collaboration_direct(collab_name):
    """Call JOIN directly from Streamlit (not inside a stored procedure)."""
    try:
        session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.JOIN('{collab_name}')").collect()
        return {"status": "SUCCESS", "message": "Join submitted. Check status to confirm."}
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

def check_status(cleanroom_name):
    try:
        res_str = session.call("DCR_SNOWVA.MIGRATION.AGENT_MIGRATE_ORCHESTRATOR", cleanroom_name, 'CHECK_STATUS')
        return json.loads(res_str)
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

def run_validation(cleanroom_name):
    try:
        res_str = session.call("DCR_SNOWVA.MIGRATION.AGENT_MIGRATE_ORCHESTRATOR", cleanroom_name, 'VALIDATE')
        
        # Handle potential double-encoded JSON
        try: result = json.loads(res_str)
        except: 
            import ast
            result = ast.literal_eval(res_str)
            
        if isinstance(result, str):
            result = json.loads(result)
            
        return result
    except Exception as e:
        return {"overall_status": "ERROR", "error": str(e)}

def execute_teardown(cleanroom_name):
    try:
        res_str = session.call("DCR_SNOWVA.MIGRATION.AGENT_MIGRATE_ORCHESTRATOR", cleanroom_name, 'TEARDOWN')
        return json.loads(res_str)
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

def get_manual_sql_scripts(plan):
    details = plan.get("details", {})
    collab_name = details.get("target_collaboration", f"migrated_{plan.get('cleanroom_name', '').replace(' ', '_')}")
    role = plan.get('role', 'UNKNOWN')
    
    # Finalize Script
    finalize_lines = [
        f"-- MANUAL FINALIZATION FOR {role} ({collab_name})",
        "USE ROLE SAMOOHA_APP_ROLE;",
        "",
        "-- 1. Check Status (Must be 'CREATED')",
        f"CALL samooha_by_snowflake_local_db.collaboration.get_status('{collab_name}');"
    ]
    if role == 'CONSUMER':
        finalize_lines.append(f"\n-- 2. Review\nCALL samooha_by_snowflake_local_db.collaboration.review('{collab_name}');")
    finalize_lines.append(f"\n-- 3. Join\nCALL samooha_by_snowflake_local_db.collaboration.join('{collab_name}');")

    # Cleanup Script
    cleanup_lines = [
        f"-- CLEANUP SCRIPT FOR {collab_name}",
        "USE ROLE SAMOOHA_APP_ROLE;",
        "",
        "-- 1. Teardown Collaboration",
        f"CALL samooha_by_snowflake_local_db.collaboration.teardown('{collab_name}');",
        f"CALL samooha_by_snowflake_local_db.collaboration.teardown('{collab_name}'); -- Call again to finalize",
        "",
        "-- 2. Drop Artifacts (If needed)"
    ]
    for t in details.get("templates", []):
        try: cleanup_lines.append(f"-- DROP TEMPLATE IF EXISTS ...; -- Check Name from Plan")
        except: pass
        
    return "\n".join(finalize_lines), "\n".join(cleanup_lines)


# --- MAIN APP UI ---

if not session:
    st.error("🚫 No active Snowpark session. Please run in Snowflake.")
    st.stop()

# Sidebar
with st.sidebar:
    st.title(" DCR Migration")
    st.caption("v2.1.0 Migration Toolkit")
    st.divider()

    if st.button("List Cleanrooms", use_container_width=True):
        with st.spinner("Fetching cleanrooms..."):
            rooms = list_cleanrooms()
            if rooms:
                st.session_state['available_rooms'] = rooms
            else:
                st.warning("No cleanrooms found.")

    if 'available_rooms' in st.session_state and st.session_state['available_rooms']:
        rooms = st.session_state['available_rooms']
        api_rooms = [r for r in rooms if r.get('api_room')]
        non_api_rooms = [r for r in rooms if not r.get('api_room')]

        room_options = ["-- Select --"] + [f"{r['name']}  ({r['role']}, {r['state']})" for r in api_rooms]
        selected = st.selectbox("Eligible Cleanrooms (P&C API)", room_options)

        if non_api_rooms:
            with st.expander(f"Ineligible: {len(non_api_rooms)} UI cleanrooms"):
                for r in non_api_rooms:
                    st.caption(f"{r['name']} ({r['role']}) - UI created, not migratable")

        if selected and selected != "-- Select --":
            cr_name_from_picker = selected.split("  (")[0].strip()
            cleanroom_input = st.text_input("Cleanroom Name", value=cr_name_from_picker, placeholder="e.g. mj_act_uc")
        else:
            cleanroom_input = st.text_input("Cleanroom Name", placeholder="e.g. mj_act_uc")
    else:
        cleanroom_input = st.text_input("Cleanroom Name", placeholder="e.g. mj_act_uc")

    if st.button("Generate Plan", type="primary", use_container_width=True):
        if not cleanroom_input:
            st.warning("Please enter a cleanroom name or select one from the list above.")
        else:
            with st.spinner("Analyzing environment..."):
                plan = get_migration_plan(cleanroom_input.strip())
                if plan:
                    st.session_state['plan'] = plan
                    st.session_state['active_tab'] = "Execute Setup"
                    st.session_state['collab_status'] = 'Not Started'
                    st.success("Plan Ready!")

# Main Content
if 'plan' not in st.session_state:
    st.info(" Enter P&C API Cleanroom Name in the sidebar to begin.")
    st.markdown("""
    ### Workflow
    1.  **Generate Plan:** Discover existing templates, datasets, and policies.
    2.  **Execute Setup:** Register artifacts and initialize the collaboration.
    3.  **Finalize:** Check status and join the new collaboration.
    4.  **Validate:** Verify object parity.
    """)
else:
    plan = st.session_state['plan']
    cr_name = plan['cleanroom_name']
    role = plan['role']
    details = plan.get('details', {})
    
    # Dashboard Metrics
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Role", role)
    m2.metric("Templates", len(details.get('templates', [])))
    m3.metric("Data Offerings", len(details.get('provider_data', [])))
    m4.metric("Status", st.session_state.get('collab_status', 'Not Started'))

    if role == 'CONSUMER' and not details.get('provider_data'):
        st.warning("No consumer data offerings were detected. Ensure you have linked datasets and set join/column policies on the legacy cleanroom before migrating.")

    st.divider()

    # Tabs
    tabs = st.tabs([" Review Plan", " Execute Setup", " Finalize (Join)", " Validate", " Cleanup"])
    
    # 1. REVIEW PLAN
    with tabs[0]:
        st.subheader("Generated Migration Script")
        st.caption("Review the SQL that will be executed.")
        st.code(plan.get('generated_script', '-- No script generated'), language='sql')

    # 2. EXECUTE SETUP
    with tabs[1]:
        st.subheader("Phase 1: Setup")
        st.info("This step registers templates/datasets and initializes the collaboration.")
        
        col1, col2 = st.columns([1, 2])
        if col1.button("Run Setup", type="primary"):
            with st.spinner("Registering templates and data offerings..."):
                res = execute_migration(cr_name)
            
            if res.get("status") == "SUCCESS":
                with st.expander("Registration Logs", expanded=True):
                    if res.get("message"):
                        st.info(res["message"])
                    actions = res.get("actions", [])
                    if actions:
                        for act in actions:
                            st.write(f"- {act}")
                
                collab_name = res.get("collab_name", "")
                collab_spec = res.get("collab_spec", "")
                role = res.get("role", "")
                
                if role == "PROVIDER" and collab_spec:
                    with st.spinner("Initializing collaboration..."):
                        init_res = initialize_collaboration(collab_spec)
                    if init_res.get("status") == "SUCCESS":
                        st.success(f"Collaboration initialized: {collab_name}")
                        if init_res.get("already_exists"):
                            st.info("Collaboration already existed.")
                        st.session_state['setup_complete'] = True
                        st.session_state['collab_name'] = collab_name
                    else:
                        st.error(f"Initialize failed: {init_res.get('message')}")
                elif role == "CONSUMER":
                    st.success("Consumer artifacts registered.")
                    st.session_state['setup_complete'] = True
                    st.session_state['collab_name'] = collab_name
                    st.session_state['owner_account'] = res.get("owner_account", "")
                    st.info("Proceed to **Finalize** tab to Review and Join the collaboration.")
            else:
                st.error(f"Setup Failed: {res.get('message')}")
                if res.get("actions"):
                    with st.expander("Partial Execution Logs", expanded=True):
                        for act in res["actions"]:
                            st.write(f"- {act}")

    # 3. FINALIZE
    with tabs[2]:
        st.subheader("Phase 2: Finalize")
        
        final_sql, _ = get_manual_sql_scripts(plan)
        
        col1, col2 = st.columns(2)
        
        # Check Status
        if col1.button("Check Status"):
            with st.spinner("Checking Collaboration Status..."):
                res = check_status(cr_name)
                if res.get("status") == "SUCCESS":
                    status = res.get("collaboration_status", "UNKNOWN")
                    st.session_state['collab_status'] = status
                    if status == 'CREATED':
                        st.success(f"Status: **{status}** - Ready to Join!")
                    elif status == 'JOINED':
                        st.success(f"Status: **{status}** - Already joined. Proceed to Validate.")
                    elif 'FAIL' in status.upper():
                        st.error(f"Status: **{status}**")
                        if res.get("hint"):
                            st.warning(res["hint"])
                    elif status == 'CREATING':
                        st.info(f"Status: **{status}** - Still creating. Check again in a few seconds.")
                    else:
                        st.warning(f"Status: **{status}** (Wait for CREATED)")
                    
                    if res.get("error_details"):
                        with st.expander("Error Details", expanded=True):
                            for detail in res["error_details"]:
                                st.error(detail)
                    
                    collaborators = res.get("collaborators", [])
                    if collaborators:
                        with st.expander("Collaborator Details"):
                            for c in collaborators:
                                status_icon = "+" if c['status'] in ('JOINED', 'CREATED') else "!" if 'FAIL' in c['status'].upper() else "-"
                                line = f"- **{c['name']}** ({c['account']}): {c['status']}"
                                if c.get('roles'):
                                    line += f"  _Roles: {c['roles']}_"
                                st.write(line)
                                if c.get('details'):
                                    st.caption(f"  Details: {c['details'][:500]}")
                else:
                    st.error(f"Check Failed: {res.get('message')}")
                    if res.get("hint"):
                        st.info(res["hint"])

        # Join - must be done manually due to SYSTEM$ACCEPT_LEGAL_TERMS restriction
        collab_name = st.session_state.get('collab_name', plan.get('details', {}).get('target_collaboration', ''))
        owner_account = st.session_state.get('owner_account', '')
        role = plan.get('role', 'PROVIDER')

        st.divider()
        st.subheader("Join Collaboration")
        st.info(
            "The **JOIN** command requires `SYSTEM$ACCEPT_LEGAL_TERMS` which cannot execute from Streamlit. "
            "Please copy the SQL below and run it in a **Snowflake SQL Worksheet**."
        )

        # Build worksheet link with multiple fallback strategies
        ws_url = None
        try:
            acct_url = session.sql("SELECT CURRENT_ACCOUNT_URL()").collect()[0][0]
            if acct_url:
                ws_url = f"{acct_url.rstrip('/')}/#/worksheets"
        except:
            pass

        if not ws_url:
            try:
                org = session.sql("SELECT CURRENT_ORGANIZATION_NAME()").collect()[0][0]
                acct = session.sql("SELECT CURRENT_ACCOUNT_NAME()").collect()[0][0]
                if org and acct:
                    ws_url = f"https://app.snowflake.com/{org.lower()}/{acct.lower()}/#/worksheets"
            except:
                pass

        if not ws_url:
            try:
                locator = session.sql("SELECT CURRENT_ACCOUNT()").collect()[0][0]
                region = session.sql("SELECT CURRENT_REGION()").collect()[0][0]
                if locator and region:
                    region_lower = region.lower().replace('_', '-')
                    ws_url = f"https://{locator.lower()}.{region_lower}.snowflakecomputing.com/console#/internal/worksheet"
            except:
                pass

        if ws_url:
            st.markdown(f"**[Open Snowflake Worksheets]({ws_url})**")
        else:
            st.markdown("**[Open Snowflake](https://app.snowflake.com)** and navigate to **Worksheets**.")

        st.markdown("""
**Instructions:**
1. Open a new SQL Worksheet in Snowflake using the link above
2. Copy and paste the SQL below
3. Make sure the role is set to **SAMOOHA_APP_ROLE** (the script sets it automatically)
4. Run each statement sequentially
""")

        join_sql_lines = ["-- Run this in a Snowflake SQL Worksheet"]
        join_sql_lines.append("USE ROLE SAMOOHA_APP_ROLE;")
        join_sql_lines.append("USE SECONDARY ROLES NONE;")
        if role == 'CONSUMER' and owner_account:
            join_sql_lines.append(f"\n-- Step 1: Review the collaboration")
            join_sql_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.review('{collab_name}', '{owner_account}');")
            join_sql_lines.append(f"\n-- Step 2: Join the collaboration")
        else:
            join_sql_lines.append(f"\n-- Join the collaboration (status must be CREATED)")
        join_sql_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.join('{collab_name}');")
        join_sql_lines.append(f"\n-- Step 3: Verify join status (wait for JOINED)")
        join_sql_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.get_status('{collab_name}');")

        st.code("\n".join(join_sql_lines), language='sql')
        # Join Button
        collab_status = st.session_state.get('collab_status', '')
        is_ready = collab_status in ('CREATED', 'INVITED')
        btn_label = "Join Collaboration" if is_ready else "Join (Wait for CREATED)"
        
        if col2.button(btn_label, disabled=not is_ready, type="primary"):
             collab_name = st.session_state.get('collab_name', plan.get('details', {}).get('target_collaboration', ''))
             owner_account = st.session_state.get('owner_account', '')
             role = plan.get('role', 'PROVIDER')
             
             if role == 'CONSUMER' and owner_account:
                 with st.spinner("Reviewing collaboration..."):
                     rev_res = review_collaboration(collab_name, owner_account)
                     if rev_res.get("status") == "SUCCESS":
                         st.info("Review complete.")
                     else:
                         st.warning(f"Review: {rev_res.get('message', 'Skipped or already reviewed.')}")
             
             with st.spinner("Joining collaboration..."):
                 res = join_collaboration_direct(collab_name)
                 if res.get("status") == "SUCCESS":
                     st.success(res.get("message"))
                     st.balloons()
                 else:
                     st.error(f"Join Failed: {res.get('message')}")

        st.divider()
        with st.expander("View Full Manual SQL Script"):
            st.code(final_sql, language='sql')

    # 4. VALIDATE
    with tabs[3]:
        st.subheader("Migration Validation")
        if st.button("Run Parity Check"):
            with st.spinner("Validating objects..."):
                report = run_validation(cr_name)
                
                status = report.get('overall_status', 'UNKNOWN')
                if status == "PASS":
                    st.success("Validation Passed: All objects match between legacy and new collaboration.")
                else:
                    st.error(f"Validation Status: {status}")
                    if report.get('error'):
                        st.error(f"Error: {report.get('error')}")
                
                steps = report.get('steps', [])
                if steps:
                    st.dataframe(
                        pd.DataFrame(steps), 
                        column_config={
                            "status": st.column_config.TextColumn("Status", width="small"),
                            "name": st.column_config.TextColumn("Check Name", width="medium"),
                            "details": st.column_config.TextColumn("Details", width="large"),
                            "fix_hint": st.column_config.TextColumn("How to Fix", width="large"),
                        },
                        use_container_width=True
                    )
                
                if report.get('missing_objects'):
                    st.error(f"Missing Objects: {report.get('missing_objects')}")

                remediation = report.get('remediation', [])
                if remediation:
                    with st.expander("Remediation Steps", expanded=True):
                        for i, hint in enumerate(remediation, 1):
                            st.markdown(f"**{i}.** {hint}")

    # 5. CLEANUP
    with tabs[4]:
        st.subheader("Teardown")
        st.warning("Destructive Action: This will remove the migrated collaboration and resources.")
        _, cleanup_sql = get_manual_sql_scripts(plan)
        st.code(cleanup_sql, language='sql')
        
        if st.button("Confirm Teardown", type="secondary"):
            with st.spinner("Tearing down..."):
                res = execute_teardown(cr_name)
                if res.get("status") == "SUCCESS":
                    st.success("Teardown Complete")
                else:
                    st.error(f"Teardown Failed: {res.get('message')}")
