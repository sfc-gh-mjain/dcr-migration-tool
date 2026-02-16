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

def get_migration_plan(cleanroom_name):
    try:
        res_str = session.call("DCR_SNOWVA.MIGRATION.AGENT_MIGRATE_ORCHESTRATOR", cleanroom_name, 'PLAN')
        if not res_str: return {"status": "ERROR", "message": "Empty response from backend."}
        plan = json.loads(res_str)
        
        if plan.get("status") == "ERROR":
            msg = plan.get('message', '')
            if "CleanroomNotInstalled" in msg or "is not installed" in msg:
                st.error(f"⚠️ Cleanroom '{cleanroom_name}' is not installed/found. Please verify the name.")
            else:
                st.error(f"Backend Error: {msg}")
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

def check_status(cleanroom_name):
    try:
        res_str = session.call("DCR_SNOWVA.MIGRATION.AGENT_MIGRATE_ORCHESTRATOR", cleanroom_name, 'CHECK_STATUS')
        return json.loads(res_str)
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}

def join_collaboration(cleanroom_name):
    try:
        res_str = session.call("DCR_SNOWVA.MIGRATION.AGENT_MIGRATE_ORCHESTRATOR", cleanroom_name, 'JOIN')
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
    st.caption("v2.0.0 Migration Toolkit")
    st.divider()
    
    cleanroom_input = st.text_input("Cleanroom Name", placeholder="e.g. mj_act_uc")
    
    if st.button(" Generate Plan", type="primary", use_container_width=True):
        if not cleanroom_input:
            st.warning("Please enter a name.")
        else:
            with st.spinner("Analyzing environment..."):
                plan = get_migration_plan(cleanroom_input.strip())
                if plan:
                    st.session_state['plan'] = plan
                    st.session_state['active_tab'] = "Execute Setup" # Reset workflow
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
            with st.spinner("Executing migration setup..."):
                res = execute_migration(cr_name)
                if res.get("status") == "SUCCESS":
                    st.success("Setup Complete!")
                    st.session_state['setup_complete'] = True
                    with st.expander("Execution Logs"):
                        for act in res.get("actions", []):
                            st.write(f"- {act}")
                else:
                    st.error(f"Setup Failed: {res.get('message')}")

    # 3. FINALIZE
    with tabs[2]:
        st.subheader("Phase 2: Finalize")
        
        final_sql, _ = get_manual_sql_scripts(plan)
        
        col1, col2 = st.columns(2)
        
        # Check Status
        if col1.button(" Check Status"):
            with st.spinner("Checking Collaboration Status..."):
                res = check_status(cr_name)
                if res.get("status") == "SUCCESS":
                    status = res.get("collaboration_status")
                    st.session_state['collab_status'] = status
                    if status == 'CREATED':
                        st.success(f"Status: **{status}**")
                    else:
                        st.warning(f"Status: **{status}** (Wait for CREATED)")
                else:
                    st.error(f"Check Failed: {res.get('message')}")

        # Join Button
        is_ready = st.session_state.get('collab_status') == 'CREATED'
        btn_label = "Join Collaboration" if is_ready else "Join (Wait for CREATED)"
        
        if col2.button(btn_label, disabled=not is_ready, type="primary"):
             with st.spinner("Joining..."):
                 res = join_collaboration(cr_name)
                 if res.get("status") == "SUCCESS":
                     st.success(res.get("message"))
                     st.balloons()
                 elif res.get("status") == "WARNING":
                     st.warning(res.get("message"))
                     st.code(final_sql, language='sql')
                 else:
                     st.error(f"Join Failed: {res.get('message')}")

        st.divider()
        with st.expander("View Manual SQL Commands"):
            st.code(final_sql, language='sql')

    # 4. VALIDATE
    with tabs[3]:
        st.subheader("Migration Validation")
        if st.button("Run Parity Check"):
            with st.spinner("Validating objects..."):
                report = run_validation(cr_name)
                
                status = report.get('overall_status', 'UNKNOWN')
                if status == "PASS":
                    st.success("Validation Passed: Objects match.")
                else:
                    st.error(f"Validation Failed: {status}")
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
                        },
                        use_container_width=True
                    )
                
                if report.get('missing_objects'):
                    st.error(f"Missing Objects: {report.get('missing_objects')}")

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
