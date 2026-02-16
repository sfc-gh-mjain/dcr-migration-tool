USE ROLE SAMOOHA_APP_ROLE;
CREATE DATABASE IF NOT EXISTS DCR_SNOWVA;
CREATE SCHEMA IF NOT EXISTS DCR_SNOWVA.MIGRATION;

CREATE OR REPLACE TABLE DCR_SNOWVA.MIGRATION.MIGRATION_JOBS (
  JOB_ID STRING,
  CLEANROOM_NAME STRING,
  STARTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  FINISHED_AT TIMESTAMP_NTZ,
  DRY_RUN BOOLEAN,
  STATUS STRING,
  DETAILS VARIANT
);

CREATE OR REPLACE PROCEDURE DCR_SNOWVA.MIGRATION.CHECK_PREREQUISITES(CLEANROOM_NAME STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'check_prereqs'
EXECUTE AS CALLER
AS
$$
def check_prereqs(session, cleanroom_name):
    errors = []
    
    # 1. Check Consumer-level LAF
    try:
        is_cons = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_ENABLED", cleanroom_name)
        if is_cons:
            is_laf_cr = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_LAF_ENABLED_FOR_CLEANROOM", cleanroom_name)
            if is_laf_cr:
                errors.append(f"LAF is enabled for cleanroom '{cleanroom_name}'. Migration not supported.")
    except: pass

    # 2. Provider Checks (Multi-Provider, UI Cleanroom, Python)
    try:
        # Fetch cleanrooms to check for UI status
        p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
        is_ui_room = False
        target_uuid = None
        
        for r in p_res:
            d = {k.upper(): v for k, v in r.as_dict().items()}
            c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
            c_id = d.get('CLEANROOM_ID') or d.get('ID')
            
            # FIX: Normalize name for comparison (Spaces <-> Underscores)
            if c_name and c_name.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_'):
                # UI Cleanroom Check: Name != ID implies UI creation
                if str(c_name).upper().replace(' ', '_') != str(c_id).upper().replace(' ', '_'):
                    is_ui_room = True
                
                # Capture UUID for other checks
                target_uuid = c_id
                break
        
        if is_ui_room:
            errors.append(f"Cleanroom '{cleanroom_name}' is a UI-created cleanroom. Migration is not supported.")

        if target_uuid:
            try:
                mp_check = session.sql(f"SHOW TABLES LIKE 'APPROVED_MULTIPROVIDER_CLEANROOMS' IN SCHEMA SAMOOHA_CLEANROOM_{target_uuid}.ADMIN").collect()
                if len(mp_check) > 0:
                    rows = session.sql(f"SELECT COUNT(*) as CNT FROM SAMOOHA_CLEANROOM_{target_uuid}.ADMIN.APPROVED_MULTIPROVIDER_CLEANROOMS").collect()
                    if rows and rows[0]['CNT'] > 0:
                        errors.append("Multi-provider cleanroom migration is not supported.")
            except: pass

            # Check Python Code
            try:
                py_files = session.sql(f"ls @SAMOOHA_CLEANROOM_{target_uuid}.APP.CODE/V1_0P1").collect()
                # FIX: Only flag if actual python files exist
                for f in py_files:
                    fname = f['name'].lower()
                    if fname.endswith('.py') or fname.endswith('.zip'):
                        errors.append("Cleanroom uses Python code, which is not supported in this release.")
                        break
            except: pass

    except: pass

    if errors: return {"status": "FAIL", "errors": errors}
    return {"status": "PASS"}
$$;

CREATE OR REPLACE PROCEDURE DCR_SNOWVA.MIGRATION.PREVIEW(CLEANROOM_NAME STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'preview'
EXECUTE AS CALLER
AS
$$
import pandas as pd

def preview(session, cleanroom_name):
    result = {"cleanroom_name": cleanroom_name, "role": "UNKNOWN", "datasets": [], "templates": [], "policies": {"join": [], "column": [], "activation": []}, "consumers": [], "errors": []}
    
    def fetch_df(query):
        try:
            res = session.sql(query).collect()
            if not res: return pd.DataFrame()
            return pd.DataFrame([{k.upper(): v for k, v in r.as_dict().items()} for r in res])
        except Exception as e:
            return pd.DataFrame()

    # --- ROLE DETECTION ---
    is_provider = False
    try:
        p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
        for r in p_res:
            d = {k.upper(): v for k, v in r.as_dict().items()}
            c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
            c_id = d.get('CLEANROOM_ID') or d.get('ID')
            c_state = d.get('STATE') or d.get('STATUS')
            
            # FIX: Normalize name
            if c_name and c_name.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_'):
                if str(c_name).upper().replace(' ', '_') == str(c_id).upper().replace(' ', '_') and c_state == 'CREATED':
                    is_provider = True
                break
    except: pass

    is_consumer = False
    if not is_provider:
        try:
            is_consumer = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_ENABLED", cleanroom_name)
        except: pass

    if not is_provider and not is_consumer:
        result["errors"].append("Cleanroom not found or not installed.")
        return result
    
    result["role"] = "PROVIDER" if is_provider else "CONSUMER"

    try:
        if is_provider:
            prov_ds = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.view_provider_datasets('{cleanroom_name}')")
            if not prov_ds.empty:
                result["datasets"] = prov_ds['TABLE_NAME'].tolist()
            
            cons_df = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CONSUMERS('{cleanroom_name}')")
            if not cons_df.empty:
                c_col = 'CONSUMER_ACCOUNT_NAME' if 'CONSUMER_ACCOUNT_NAME' in cons_df.columns else 'CONSUMER_NAME'
                if c_col in cons_df.columns: result["consumers"] = cons_df[c_col].tolist()
            
            tmps = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_ADDED_TEMPLATES('{cleanroom_name}')")
            if not tmps.empty: result["templates"] = tmps['TEMPLATE_NAME'].tolist()

            cr_record = fetch_df(f"SELECT * FROM SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PUBLIC.CLEANROOM_RECORD WHERE UPPER(CLEANROOM_NAME) = '{cleanroom_name.upper()}'")
            if not cr_record.empty:
                uuid_col = 'CLEANROOM_ID' if 'CLEANROOM_ID' in cr_record.columns else 'ID'
                uuid = cr_record.iloc[0][uuid_col]
                
                jp = fetch_df(f"SELECT * FROM SAMOOHA_CLEANROOM_{uuid}.SHARED_SCHEMA.JOIN_COLUMNS")
                if not jp.empty: result["policies"]["join"] = jp.to_dict(orient='records')

                cp = fetch_df(f"SELECT * FROM SAMOOHA_CLEANROOM_{uuid}.SHARED_SCHEMA.POLICY_COLUMNS")
                if not cp.empty: result["policies"]["column"] = cp.to_dict(orient='records')
                
                try:
                    ap = fetch_df(f"SELECT * FROM SAMOOHA_CLEANROOM_{uuid}.SHARED_SCHEMA.ACTIVATION_COLUMNS")
                    if not ap.empty: result["policies"]["activation"] = ap.to_dict(orient='records')
                except: pass

        elif is_consumer:
            cons_ds = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_consumer_datasets('{cleanroom_name}')")
            if not cons_ds.empty:
                t_col = 'TABLE_NAME' if 'TABLE_NAME' in cons_ds.columns else 'VIEW_NAME'
                if t_col in cons_ds.columns: result["datasets"] = cons_ds[t_col].tolist()

            reqs = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.LIST_TEMPLATE_REQUESTS('{cleanroom_name}')")
            if not reqs.empty: result["templates"] = reqs['TEMPLATE_NAME'].tolist() if 'TEMPLATE_NAME' in reqs.columns else []

            jp = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_join_policy('{cleanroom_name}')")
            if not jp.empty: result["policies"]["join"] = jp.to_dict(orient='records')

            cp = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_column_policy('{cleanroom_name}')")
            if not cp.empty: result["policies"]["column"] = cp.to_dict(orient='records')

            try:
                ap = fetch_df(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_activation_policy('{cleanroom_name}')")
                if not ap.empty: result["policies"]["activation"] = ap.to_dict(orient='records')
            except: pass

    except Exception as e:
        result["errors"].append(str(e))

    return result
$$;

CREATE OR REPLACE PROCEDURE DCR_SNOWVA.MIGRATION.GENERATE_TEMPLATE_SPECS(CLEANROOM_NAME STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'pyyaml')
HANDLER = 'gen_templates'
EXECUTE AS CALLER
AS
$$
import yaml
import pandas as pd
import re
from datetime import datetime
import json

def gen_templates(session, cleanroom_name):
    # --- ROLE DETECTION ---
    is_provider = False
    try:
        p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
        for r in p_res:
            d = {k.upper(): v for k, v in r.as_dict().items()}
            c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
            c_id = d.get('CLEANROOM_ID') or d.get('ID')
            c_state = d.get('STATE') or d.get('STATUS')
            # FIX: Normalize name
            if c_name and c_name.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_'):
                if str(c_name).upper().replace(' ', '_') == str(c_id).upper().replace(' ', '_') and c_state == 'CREATED': is_provider = True
                break
    except: pass
    
    is_consumer = False
    if not is_provider:
        try: is_consumer = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_ENABLED", cleanroom_name)
        except: pass

    if not is_provider and not is_consumer: return []

    df = pd.DataFrame()
    if is_provider:
        df_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_ADDED_TEMPLATES('{cleanroom_name}')").collect()
        if df_res: df = pd.DataFrame([r.as_dict() for r in df_res])
        try:
            req_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_TEMPLATE_REQUESTS('{cleanroom_name}')").collect()
            if req_res: 
                df2 = pd.DataFrame([r.as_dict() for r in req_res])
                df = pd.concat([df, df2], ignore_index=True)
        except: pass
    elif is_consumer:
        try:
            df_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.LIST_TEMPLATE_REQUESTS('{cleanroom_name}')").collect()
            if df_res: df = pd.DataFrame([r.as_dict() for r in df_res])
        except: pass

    if df.empty: return []
    
    norm_data = []
    for _, row in df.iterrows():
        r = {k.upper(): v for k, v in row.to_dict().items()}
        if 'TEMPLATE_NAME' not in r and 'NAME' in r:
            r['TEMPLATE_NAME'] = r['NAME']
        norm_data.append(r)

    if not norm_data: return []

    df_norm = pd.DataFrame(norm_data)
    if 'TEMPLATE_NAME' in df_norm.columns:
        df_norm = df_norm.drop_duplicates(subset=['TEMPLATE_NAME'])
    
    ver_str = "MIGRATION_V1" 

    specs = []
    for _, r in df_norm.iterrows():
        t_name = r.get('TEMPLATE_NAME')
        t_sql = str(r.get('TEMPLATE') or r.get('SQL_TEXT') or '')
        
        if not t_name or not t_sql: continue

        is_activation = "cleanroom.activation_" in t_sql.lower()
        cleaned_sql = t_sql
        if not is_activation:
            match = re.search(r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+.*?\s+AS\s*(.*?);", t_sql, re.IGNORECASE | re.DOTALL)
            if match: cleaned_sql = match.group(1).strip()

        params = []
        raw_params = r.get('PARAMETERS')
        if raw_params:
            try:
                if isinstance(raw_params, str):
                    try: params = json.loads(raw_params)
                    except: params = yaml.safe_load(raw_params)
                else: params = raw_params
            except: pass

        if not params and cleaned_sql:
            jinja_vars = re.findall(r"\{\{\s*([a-zA-Z0-9_]+)", cleaned_sql)
            system_vars = ['source_table', 'my_table', 'consumer_table', 'provider_table', 'dimensions', 'measures']
            seen = set()
            for v in jinja_vars:
                if v.lower() not in system_vars and v not in seen:
                    seen.add(v)
                    params.append({
                        "name": v,
                        "type": "string",
                        "description": f"Auto-detected parameter: {v}",
                        "default": ""
                    })

        spec_dict = {
            'api_version': '2.0.0', 
            'spec_type': 'template', 
            'name': f"migrated_{t_name}",
            'version': ver_str, 
            'type': 'sql_activation' if is_activation else 'sql_analysis',
            'description': f"Migrated from legacy: {t_name}", 
            'parameters': params, 
            'template': cleaned_sql
        }
        specs.append(yaml.dump(spec_dict))
    return specs
$$;

CREATE OR REPLACE PROCEDURE DCR_SNOWVA.MIGRATION.GENERATE_DATA_OFFERING_SPECS(CLEANROOM_NAME STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'pyyaml')
HANDLER = 'gen_data_offerings'
EXECUTE AS CALLER
AS
$$
import yaml
import pandas as pd
import re
import hashlib
from datetime import datetime

def gen_data_offerings(session, cleanroom_name):
    def guess_type(cname):
        c = cname.lower()
        if 'email' in c:
            if 'b64' in c: return 'hashed_email_b64_encoded'
            if 'hash' in c or 'sha256' in c: return 'hashed_email_sha256'
            return 'email'
        if 'phone' in c:
            if 'b64' in c: return 'hashed_phone_b64_encoded'
            if 'hash' in c or 'sha256' in c: return 'hashed_phone_sha256'
            return 'phone' 
        if 'ip' in c and 'zip' not in c:
            if 'b64' in c: return 'hashed_ip_address_b64_encoded'
            if 'hash' in c or 'sha256' in c: return 'hashed_ip_address_sha256'
            return 'ip_address'
        if 'device' in c or 'idfa' in c or 'maid' in c:
            if 'b64' in c: return 'hashed_device_b64_encoded'
            if 'hash' in c or 'sha256' in c: return 'hashed_device_id_sha256'
            return 'device_id'
        return None
    
    def refine_type_by_data(table, col, proposed_type):
        if not proposed_type or 'sha256' not in proposed_type: return proposed_type
        try:
            res = session.sql(f"SELECT {col} FROM {table} WHERE {col} IS NOT NULL LIMIT 1").collect()
            if not res: return proposed_type
            val = str(res[0][0])
            if len(val) == 44 or val.endswith('='):
                 if 'email' in proposed_type: return 'hashed_email_b64_encoded'
                 if 'phone' in proposed_type: return 'hashed_phone_b64_encoded'
                 if 'ip' in proposed_type: return 'hashed_ip_address_b64_encoded'
                 if 'device' in proposed_type: return 'hashed_device_b64_encoded'
            return proposed_type
        except: return proposed_type

    def get_df_upper(query):
        try:
            res = session.sql(query).collect()
            if not res: return pd.DataFrame()
            return pd.DataFrame([{k.upper(): v for k, v in r.as_dict().items()} for r in res])
        except: return pd.DataFrame()

    def sanitize_name(name):
        clean = name.replace('.', '_')
        if len(clean) > 75:
            h = hashlib.md5(name.encode()).hexdigest()[:8]
            clean = f"{clean[:60]}_{h}"
        if not clean[0].isalpha() and clean[0] != '_': clean = "T_" + clean
        return clean

    is_provider = False
    try:
        p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
        for r in p_res:
            d = {k.upper(): v for k, v in r.as_dict().items()}
            c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
            c_id = d.get('CLEANROOM_ID') or d.get('ID')
            c_state = d.get('STATE') or d.get('STATUS')
            # FIX: Normalize name
            if c_name and c_name.upper().replace(' ', '_') == cleanroom_name.upper().replace(' ', '_'):
                if str(c_name).upper().replace(' ', '_') == str(c_id).upper().replace(' ', '_') and c_state == 'CREATED': is_provider = True
                break
    except: pass

    is_consumer = False
    if not is_provider:
        try:
            is_consumer = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_ENABLED", cleanroom_name)
        except: pass

    if not is_provider and not is_consumer:
        return []

    tables_data = [] 
    join_df = pd.DataFrame()
    col_df = pd.DataFrame()
    act_df = pd.DataFrame()

    if is_provider:
        prov_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.view_provider_datasets('{cleanroom_name}')").collect()
        if prov_res:
            for row in prov_res:
                d = {k.upper(): v for k, v in row.as_dict().items()}
                t_name = d.get('TABLE_NAME')
                if t_name:
                    tables_data.append({'TABLE_NAME': t_name, 'SQL_ENABLED': d.get('SQL_ENABLED', False)})
        
        cr = session.sql(f"SELECT * FROM SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PUBLIC.CLEANROOM_RECORD WHERE UPPER(CLEANROOM_NAME) = '{cleanroom_name.upper()}'").collect()
        if cr:
            cr_dict = {k.upper(): v for k, v in cr[0].as_dict().items()}
            uuid = cr_dict.get('CLEANROOM_ID') or cr_dict.get('ID') or cr_dict.get('CLEANROOM_UUID')
            if not uuid:
                for k, v in cr_dict.items():
                    if 'ID' in k and 'SIDE' not in k: uuid = v; break
            
            if uuid:
                join_df = get_df_upper(f"SELECT * FROM SAMOOHA_CLEANROOM_{uuid}.SHARED_SCHEMA.JOIN_COLUMNS")
                col_df = get_df_upper(f"SELECT * FROM SAMOOHA_CLEANROOM_{uuid}.SHARED_SCHEMA.POLICY_COLUMNS")
                try: act_df = get_df_upper(f"SELECT * FROM SAMOOHA_CLEANROOM_{uuid}.SHARED_SCHEMA.ACTIVATION_COLUMNS")
                except: pass
    elif is_consumer:
        cons_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_consumer_datasets('{cleanroom_name}')").collect()
        if cons_res:
            for row in cons_res:
                d = {k.upper(): v for k, v in row.as_dict().items()}
                t_name = d.get('LINKED_TABLE') or d.get('TABLE_NAME') or d.get('VIEW_NAME')
                tables_data.append({'TABLE_NAME': t_name, 'SQL_ENABLED': False})
        
        join_df = get_df_upper(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_join_policy('{cleanroom_name}')")
        col_df = get_df_upper(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_column_policy('{cleanroom_name}')")
        try: act_df = get_df_upper(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.view_activation_policy('{cleanroom_name}')")
        except: pass

    if not tables_data: return []

    specs = []
    ver_str = "MIGRATION_V1"

    for t_data in tables_data:
        t_name = t_data['TABLE_NAME']
        if not t_name or "TEMP_PUBLIC_KEY" in t_name: continue
        
        schema_policies = {}
        if not join_df.empty:
             t_col = 'TABLE_NAME' if 'TABLE_NAME' in join_df.columns else 'DATASET_NAME'
             if t_col in join_df.columns:
                 for _, row in join_df.iterrows():
                     j_table = str(row.get(t_col, ''))
                     if j_table and j_table.split('.')[-1].upper() == t_name.split('.')[-1].upper():
                         cname = row['COLUMN_NAME']
                         gtype = guess_type(cname)
                         gtype = refine_type_by_data(t_name, cname, gtype)
                         schema_policies[cname] = {'category': 'join_standard', 'column_type': gtype if gtype else 'MANUAL_REVIEW'}

        if not col_df.empty:
             t_col = 'TABLE_NAME' if 'TABLE_NAME' in col_df.columns else 'DATASET_NAME'
             if t_col in col_df.columns:
                 for _, row in col_df.iterrows():
                     c_table = str(row.get(t_col, ''))
                     if c_table and c_table.split('.')[-1].upper() == t_name.split('.')[-1].upper():
                         cname = row['COLUMN_NAME']
                         if cname not in schema_policies: schema_policies[cname] = {'category': 'passthrough'}

        if not act_df.empty:
             t_col = 'TABLE_NAME' if 'TABLE_NAME' in act_df.columns else 'DATASET_NAME'
             if t_col in act_df.columns:
                 act_subset = act_df[act_df[t_col] == t_name]
                 for _, row in act_subset.iterrows():
                     cname = row['COLUMN_NAME']
                     if cname not in schema_policies: schema_policies[cname] = {'category': 'passthrough'}
                     schema_policies[cname]['activation_allowed'] = True
                     
        if not schema_policies:
            try:
                desc_res = session.sql(f"DESC TABLE {t_name}").collect()
                if desc_res:
                    first_col = desc_res[0]['name']
                    schema_policies[first_col] = {'category': 'passthrough'}
            except: 
                schema_policies['DUMMY_COL'] = {'category': 'passthrough'}

        safe_name = sanitize_name(f"migrated_{t_name}")
        
        dataset_obj = {
            'alias': safe_name,
            'data_object_fqn': t_name, 
            'allowed_analyses': 'template_and_freeform_sql' if t_data.get('SQL_ENABLED') else 'template_only',
            'object_class': 'custom', 
            'schema_and_template_policies': schema_policies
        }
        
        if t_data.get('SQL_ENABLED'):
            dataset_obj['freeform_sql_policies'] = {}
            dataset_obj['require_freeform_sql_policy'] = False

        spec = {
            'api_version': '2.0.0', 'spec_type': 'data_offering', 'name': safe_name,
            'version': ver_str, 'description': f"Migrated {t_name}",
            'datasets': [dataset_obj]
        }
        specs.append(yaml.dump(spec))
    return specs
$$;

CREATE OR REPLACE PROCEDURE DCR_SNOWVA.MIGRATION.GENERATE_COLLABORATION_SPEC(
    CLEANROOM_NAME STRING, PROVIDER_DO_IDS ARRAY, CONSUMER_DO_IDS ARRAY, TEMPLATE_IDS ARRAY, ENABLE_ACTIVATION BOOLEAN
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
HANDLER = 'gen_collab'
EXECUTE AS CALLER
AS
$$
import yaml
from datetime import datetime

def gen_collab(session, cleanroom_name, prov_ids, cons_ids, temp_ids, enable_activation):
    cr_res = session.sql(f"SELECT * FROM SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PUBLIC.CLEANROOM_RECORD WHERE UPPER(CLEANROOM_NAME) = '{cleanroom_name.upper()}'").collect()
    
    prov_acct = "PROVIDER_ACCOUNT"
    if cr_res:
        cr = {k.upper(): v for k, v in cr_res[0].as_dict().items()}
        for k,v in cr.items():
            if "PROVIDER" in k and "LOCATOR" in k: prov_acct = v

    if prov_acct == "PROVIDER_ACCOUNT" or '.' not in prov_acct:
        try:
            curr_org = session.sql("SELECT CURRENT_ORGANIZATION_NAME()").collect()[0][0]
            if prov_acct == "PROVIDER_ACCOUNT": prov_acct = f"{curr_org}.PROVIDER_ACCOUNT"
            else: prov_acct = f"{curr_org}.{prov_acct}"
        except: pass
    
    cons_res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CONSUMERS('{cleanroom_name}')").collect()
    cons_acct = "CONSUMER_ACCOUNT"
    if cons_res:
        c_dict = {k.upper(): v for k, v in cons_res[0].as_dict().items()}
        for k,v in c_dict.items():
            if 'NAME' in k or 'ACCOUNT' in k: 
                cons_acct = v; break
    
    if cons_acct == "CONSUMER_ACCOUNT" or '.' not in cons_acct:
         try:
             curr_org = session.sql("SELECT CURRENT_ORGANIZATION_NAME()").collect()[0][0]
             cons_acct = f"{curr_org}.REPLACE_CONSUMER_ACCOUNT"
         except:
             cons_acct = "ORG.REPLACE_CONSUMER_ACCOUNT" 

    runners = {}

    cons_runner_config = {
        'templates': [{'id': x} for x in temp_ids]
    }
    
    cons_runner_config['data_providers'] = {'Provider_Account': {'data_offerings': [{'id': x} for x in prov_ids]}} if prov_ids else {}

    if enable_activation:
        cons_runner_config['activation_destinations'] = {'snowflake_collaborators': ['Consumer_Account']}

    runners['Consumer_Account'] = cons_runner_config

    if cons_ids:
        prov_runner_config = {
            'templates': [{'id': x} for x in temp_ids],
            'data_providers': {'Consumer_Account': {'data_offerings': [{'id': x} for x in cons_ids]}}
        }
        if enable_activation:
             prov_runner_config['activation_destinations'] = {'snowflake_collaborators': ['Consumer_Account']}
        runners['Provider_Account'] = prov_runner_config
    
    else:
        can_prov_run = False
        try:
            res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.LIBRARY.IS_PROVIDER_RUN_ENABLED('{cleanroom_name}')").collect()
            if res:
                 val = str(res[0][0]).lower()
                 if "provider side run analysis is enabled" in val: can_prov_run = True
        except: pass
        
        if can_prov_run:
            prov_runner_config = {
                'templates': [{'id': x} for x in temp_ids],
                'data_providers': {'Consumer_Account': {'data_offerings': []}} 
            }
            if enable_activation:
                 prov_runner_config['activation_destinations'] = {'snowflake_collaborators': ['Consumer_Account']}
            runners['Provider_Account'] = prov_runner_config

    ver_str = "MIGRATION_V1"

    safe_collab_name = f"migrated_{cleanroom_name.replace(' ', '_')}"

    yaml_str = f"api_version: 2.0.0\n"
    yaml_str += f"spec_type: collaboration\n"
    yaml_str += f"name: {safe_collab_name}\n"
    yaml_str += f"description: 'Migrated from P&C: {cleanroom_name}'\n"
    yaml_str += f"version: {ver_str}\n"
    yaml_str += f"owner: Provider_Account\n"
    
    aliases = {
        'collaborator_identifier_aliases': {
            'Provider_Account': prov_acct,
            'Consumer_Account': cons_acct
        }
    }
    yaml_str += yaml.dump(aliases, sort_keys=False)
    runners_dict = {'analysis_runners': runners}
    yaml_str += yaml.dump(runners_dict, sort_keys=False)

    return yaml_str
$$;

CREATE OR REPLACE PROCEDURE DCR_SNOWVA.MIGRATION.GENERATE_ANALYSIS_SCRIPT(CLEANROOM_NAME STRING, TEMPLATE_NAME STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'pyyaml')
HANDLER = 'gen_run_script'
EXECUTE AS CALLER
AS
$$
import yaml
import pandas as pd

def gen_run_script(session, cleanroom_name, template_name):
    collab_name = f"migrated_{cleanroom_name}"
    target_template = None
    try:
        df = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_ADDED_TEMPLATES('{cleanroom_name}')").collect()
        if df:
            for row in df:
                r = {k.upper(): v for k, v in row.as_dict().items()}
                t_name = r.get('TEMPLATE_NAME')
                if t_name and template_name and t_name.lower() == template_name.lower():
                    target_template = r; break
    except: pass
            
    if not target_template: return f"-- Template '{template_name}' not found."

    migrated_name = f"migrated_{target_template['TEMPLATE_NAME']}"
    raw_params = target_template.get('PARAMETERS')
    is_activation = "cleanroom.activation_" in str(target_template.get('TEMPLATE', '')).lower()
    
    args = {}
    if raw_params: args['param_1'] = "<VAL>"

    if is_activation:
        args['collaborator_name'] = "<DESTINATION_ACCOUNT_ALIAS>"
        args['segment_name'] = "output_table"
        args['activation_column'] = ["<COL>"]

    view_mappings = {"source_tables": ["<REPLACE_WITH_TABLE>"]}
    actual_template_id = f"{migrated_name}_MIGRATION_V1"

    analysis_spec = {
        "api_version": "2.0.0",
        "spec_type": "analysis",
        "name": f"run_{migrated_name}",
        "description": f"Execution of {migrated_name}",
        "template": actual_template_id, 
        "template_configuration": {
            "view_mappings": view_mappings
        },
        "arguments": args
    }
    
    yaml_str = yaml.dump(analysis_spec)
    dd = "$" + "$"
    return f"CALL samooha_by_snowflake_local_db.collaboration.run('{collab_name}', {dd}{yaml_str}{dd});"
$$;

CREATE OR REPLACE PROCEDURE DCR_SNOWVA.MIGRATION.VALIDATE(CLEANROOM_NAME STRING, COLLABORATION_NAME STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'validate'
EXECUTE AS CALLER
AS
$$
import pandas as pd

def validate(session, cleanroom_name, collab_name):
    report = {"overall_status": "PASS", "steps": [], "missing_objects": []}
    def log_step(name, status, details=""):
        report['steps'].append({"name": name, "status": status, "details": details})
        if status == "FAIL": report['overall_status'] = "FAIL"

    try:
        is_provider = False
        try:
            p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
            for r in p_res:
                d = {k.upper(): v for k, v in r.as_dict().items()}
                c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
                c_id = d.get('CLEANROOM_ID') or d.get('ID')
                c_state = d.get('STATE') or d.get('STATUS')
                if c_name and c_name.upper() == cleanroom_name.upper():
                    if str(c_name).upper().replace(' ', '_') == str(c_id).upper().replace(' ', '_') and c_state == 'CREATED': is_provider = True
                    break
        except: pass

        try:
            res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.GET_STATUS('{collab_name}')").collect()
            if res:
                status_val = str(list(res[0].as_dict().values())[0])
                log_step("Collaboration Status", "PASS", f"Current Status: {status_val}")
            else:
                log_step("Collaboration Status", "FAIL", "Collaboration not found")
        except Exception as e:
            log_step("Collaboration Status", "FAIL", str(e))

        if is_provider:
            # FIX: Robust check for Template Parity
            try:
                legacy_df = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_ADDED_TEMPLATES('{cleanroom_name}')").collect()
                legacy_names = [r['TEMPLATE_NAME'] for r in legacy_df] if legacy_df else []
                # UPDATED: Use Collaboration view to check parity
                new_templates_df = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.VIEW_TEMPLATES('{collab_name}')").collect()
                new_template_names = set()
                if new_templates_df:
                     for r in new_templates_df:
                        row_dict = {k.upper(): v for k, v in r.as_dict().items()}
                        name = row_dict.get('NAME') or row_dict.get('TEMPLATE_NAME')
                        if name: new_template_names.add(name)
                
                missing = [f"migrated_{old}" for old in legacy_names if f"migrated_{old}" not in new_template_names]
                if not missing: log_step("Template Parity", "PASS", f"All {len(legacy_names)} found.")
                else: 
                     log_step("Template Parity", "FAIL", f"Missing: {missing}")
                     report['missing_objects'].extend(missing)
            except Exception as e:
                log_step("Template Parity", "FAIL", str(e))

            # FIX: Robust check for Data Offering Parity
            try:
                prov_ds = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.view_provider_datasets('{cleanroom_name}')").collect()
                legacy_tables = [r['TABLE_NAME'] for r in prov_ds if "TEMP_PUBLIC_KEY" not in r['TABLE_NAME']] if prov_ds else []
                # UPDATED: Use Collaboration view to check parity
                new_dos_df = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.VIEW_DATA_OFFERINGS('{collab_name}')").collect()
                new_do_names = set()
                if new_dos_df:
                     for r in new_dos_df:
                        row_dict = {k.upper(): v for k, v in r.as_dict().items()}
                        name = row_dict.get('NAME') or row_dict.get('DATA_OFFERING_NAME')
                        if name: new_do_names.add(name)
                
                missing_dos = []
                for t in legacy_tables:
                    # Basic sanitation check matching generator
                    sanitized = f"migrated_{t.replace('.', '_')}"
                    found = False
                    for n in new_do_names:
                        if n.startswith(sanitized[:50]): found = True; break
                    if not found: missing_dos.append(sanitized)

                if not missing_dos: log_step("Data Offering Parity", "PASS", f"All {len(legacy_tables)} found.")
                else: 
                    log_step("Data Offering Parity", "FAIL", f"Potential missing: {missing_dos}")
                    report['missing_objects'].extend(missing_dos)
            except Exception as e:
                log_step("Data Offering Parity", "FAIL", str(e))
        else:
             log_step("Consumer Check", "INFO", "Verified Collaboration Access.")

    except Exception as e:
        report['overall_status'] = "ERROR"
        report['error'] = str(e)
        
    return report
$$;

CREATE OR REPLACE PROCEDURE DCR_SNOWVA.MIGRATION.TEARDOWN(COLLABORATION_NAME STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'teardown_collab'
EXECUTE AS CALLER
AS
$$
import time

def teardown_collab(session, collab_name):
    collab_name = collab_name.replace(' ', '_')
    try:
        session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.TEARDOWN('{collab_name}')").collect()
        
        for _ in range(10):
            res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.GET_STATUS('{collab_name}')").collect()
            if res:
                row = {k.upper(): v for k, v in res[0].as_dict().items()}
                status = row.get('STATUS')
                if status == 'LOCAL_DROP_PENDING':
                    break
            time.sleep(2)
            
        session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.TEARDOWN('{collab_name}')").collect()
        
        return f"Teardown completed for {collab_name}"
    except Exception as e:
        return f"Teardown error: {str(e)}"
$$;

CREATE OR REPLACE PROCEDURE DCR_SNOWVA.MIGRATION.AGENT_MIGRATE_ORCHESTRATOR(
    CLEANROOM_NAME STRING, 
    ACTION_MODE STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'pyyaml', 'snowflake-snowpark-python')
HANDLER = 'agent_main'
EXECUTE AS CALLER
AS
$$
import json
import yaml
import pandas as pd
import hashlib
import time

def agent_main(session, cleanroom_name, action_mode):
    action = action_mode.upper()
    dd = "$" + "$"
    
    try:
        # Check LAF Status for informational purposes
        laf_info = ""
        is_account_laf = False
        try:
             res = session.sql("CALL samooha_by_snowflake_local_db.library.is_laf_enabled_on_account()").collect()
             if res and str(res[0][0]).upper() == 'TRUE':
                 is_account_laf = True
        except: pass

        is_cr_laf = False
        try:
             # Try checking specific cleanroom LAF status (Consumer side)
             is_cr_laf = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_LAF_ENABLED_FOR_CLEANROOM", cleanroom_name)
        except: pass
        
        if is_cr_laf:
            laf_info = " Note: This cleanroom utilizes Cross-Cloud Auto-Fulfillment (LAF)."
        elif is_account_laf:
            laf_info = " Note: LAF is enabled on this account. Please verify if this Cleanroom uses it."

        check = session.call("DCR_SNOWVA.MIGRATION.CHECK_PREREQUISITES", cleanroom_name)
        if isinstance(check, str): check = json.loads(check)
        if check.get('status') == 'FAIL':
             return json.dumps({"status": "ERROR", "message": f"Prerequisites failed: {check.get('errors')}"})
        
        is_provider = False
        try:
            p_res = session.sql("CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.VIEW_CLEANROOMS()").collect()
            for r in p_res:
                d = {k.upper(): v for k, v in r.as_dict().items()}
                c_name = d.get('CLEANROOM_NAME') or d.get('NAME')
                c_id = d.get('CLEANROOM_ID') or d.get('ID')
                c_state = d.get('STATE') or d.get('STATUS')
                if c_name and c_name.upper() == cleanroom_name.upper():
                    # FIX: Case-insensitive check
                    if str(c_name).upper().replace(' ', '_') == str(c_id).upper().replace(' ', '_') and c_state == 'CREATED': is_provider = True
                    break
        except: pass
        
        is_consumer = False
        if not is_provider:
             try:
                 is_consumer = session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.CONSUMER.IS_ENABLED", cleanroom_name)
             except: pass
             
        if not is_provider and not is_consumer:
             return json.dumps({"status": "ERROR", "message": f"Cleanroom '{cleanroom_name}' not found or access denied."})

        role_type = "PROVIDER" if is_provider else "CONSUMER"
        
        safe_collab_name = f"migrated_{cleanroom_name.replace(' ', '_')}"
        
        if action == 'TEARDOWN':
            res = session.call("DCR_SNOWVA.MIGRATION.TEARDOWN", safe_collab_name)
            return json.dumps({"status": "SUCCESS", "message": res})

        res_tmps = session.call("DCR_SNOWVA.MIGRATION.GENERATE_TEMPLATE_SPECS", cleanroom_name)
        tmps = json.loads(res_tmps) if isinstance(res_tmps, str) else res_tmps
        
        res_dos = session.call("DCR_SNOWVA.MIGRATION.GENERATE_DATA_OFFERING_SPECS", cleanroom_name)
        dos = json.loads(res_dos) if isinstance(res_dos, str) else res_dos

        if not dos and not tmps and action != 'TEARDOWN' and action != 'CHECK_STATUS' and action != 'JOIN':
             if role_type == 'PROVIDER':
                  return json.dumps({"status": "ERROR", "message": "No data offerings or templates found."})

        script_lines = []
        script_lines.append("USE ROLE SAMOOHA_APP_ROLE;")
        script_lines.append(f"-- MIGRATION SCRIPT FOR: {cleanroom_name} ({role_type})")
        script_lines.append("-- Generated via DCR_SNOWVA.MIGRATION Package\n")

        if tmps:
            script_lines.append(f"-- [1] REGISTER TEMPLATES ({len(tmps)} found)")
            for y_str in tmps:
                script_lines.append(f"CALL samooha_by_snowflake_local_db.registry.register_template({dd}\n{y_str}\n{dd});\n")

        if dos:
            script_lines.append(f"\n-- [2] REGISTER DATA OFFERINGS ({len(dos)} found)")
            for y_str in dos:
                spec = yaml.safe_load(y_str)
                script_lines.append(f"-- {role_type} Offering: {spec['name']}")
                script_lines.append(f"CALL samooha_by_snowflake_local_db.registry.register_data_offering({dd}\n{y_str}\n{dd});\n")

        cons_ids = []
        
        def get_safe_do_name(spec):
            orig_name = spec['name']
            if len(orig_name) > 75:
                pass
            return f"{spec['name']}_{spec['version']}"

        if is_provider:
             prov_ds_df = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.PROVIDER.view_provider_datasets('{cleanroom_name}')").collect()
             prov_table_names = set()
             if prov_ds_df:
                 for row in prov_ds_df:
                     d = {k.upper(): v for k, v in row.as_dict().items()}
                     t_name = d.get('TABLE_NAME')
                     if t_name: prov_table_names.add(t_name)
             
             prov_ids = []
             for y_str in dos:
                spec = yaml.safe_load(y_str)
                ds_fqn = spec['datasets'][0].get('data_object_fqn')
                # DO ID must use version suffix
                do_id = f"{spec['name']}_{spec['version']}"
                if ds_fqn in prov_table_names: prov_ids.append(do_id)
                else: cons_ids.append(do_id)
             
             tmp_ids = []
             has_activation = False
             for y_str in tmps:
                spec = yaml.safe_load(y_str)
                t_id = f"{spec['name']}_{spec['version']}"
                tmp_ids.append(t_id)
                if spec.get('type') == 'sql_activation': has_activation = True
             
             collab_yml = session.call("DCR_SNOWVA.MIGRATION.GENERATE_COLLABORATION_SPEC", cleanroom_name, prov_ids, cons_ids, tmp_ids, has_activation)
             
             script_lines.append(f"\n-- [3] CREATE COLLABORATION: {safe_collab_name}")
             script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.initialize({dd}\n{collab_yml}\n{dd});\n")
             script_lines.append(f"-- Wait for status 'CREATED' before joining")
             script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.get_status('{safe_collab_name}');\n")
             script_lines.append(f"-- [4] JOIN COLLABORATION (Self-Join for Provider)")
             script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.join('{safe_collab_name}');\n")
        else:
             script_lines.append(f"\n-- [3] REVIEW COLLABORATION")
             script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.review('{safe_collab_name}');\n")
             script_lines.append(f"-- [4] JOIN COLLABORATION")
             script_lines.append(f"CALL samooha_by_snowflake_local_db.collaboration.join('{safe_collab_name}');\n")

        full_script_text = "\n".join(script_lines)

        if action == 'PLAN':
            t_count = len(tmps) if tmps else 0
            d_count = len(dos) if dos else 0
            
            return json.dumps({
                "status": "READY_TO_MIGRATE",
                "role": role_type,
                "summary": f"Found {t_count} templates and {d_count} datasets.{laf_info}",
                "generated_script": full_script_text,
                "next_step": "Ask user to confirm execution.",
                "details": {
                    "templates": tmps,
                    "provider_data": dos,
                    "target_collaboration": safe_collab_name
                }
            })

        elif action == 'EXECUTE':
            actions_taken = []
            
            if tmps:
                for y_str in tmps:
                    try:
                        session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.REGISTRY.REGISTER_TEMPLATE", y_str)
                        actions_taken.append("Registered template")
                    except Exception as e:
                        if "already exists" in str(e).lower(): pass
                        else: raise e
            
            if dos:
                for y_str in dos:
                    try:
                        session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.REGISTRY.REGISTER_DATA_OFFERING", y_str)
                        actions_taken.append("Registered data offering")
                    except Exception as e:
                        if "already exists" in str(e).lower(): pass
                        else: raise e

            if is_provider:
                collab_yml = session.call("DCR_SNOWVA.MIGRATION.GENERATE_COLLABORATION_SPEC", cleanroom_name, prov_ids, cons_ids, tmp_ids, has_activation)
                try:
                    session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.INITIALIZE", collab_yml)
                    actions_taken.append(f"Created collaboration '{safe_collab_name}'")
                    actions_taken.append("Initialization complete. Proceed to Status Check.")
                except Exception as e:
                    if "already exists" in str(e).lower(): pass
                    else: raise e
            else:
                actions_taken.append("Artifacts registered. Proceed to Status Check and Join.")

            return json.dumps({
                "status": "SUCCESS",
                "message": f"Setup complete for '{cleanroom_name}'. Please verify status before joining.",
                "actions": actions_taken,
                "consumer_action_required": "None"
            })

        elif action == 'CHECK_STATUS':
             try:
                 res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.GET_STATUS('{safe_collab_name}')").collect()
                 status = "UNKNOWN"
                 if res:
                     row = {k.upper(): v for k, v in res[0].as_dict().items()}
                     status = row.get('STATUS') or str(list(row.values())[0])
                 return json.dumps({"status": "SUCCESS", "collaboration_status": status})
             except Exception as e:
                 return json.dumps({"status": "ERROR", "message": str(e)})

        elif action == 'JOIN':
             try:
                 # Safety Check: Verify status is CREATED before joining
                 if role_type == 'PROVIDER':
                     res = session.sql(f"CALL SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.GET_STATUS('{safe_collab_name}')").collect()
                     current_status = "UNKNOWN"
                     if res:
                         row = {k.upper(): v for k, v in res[0].as_dict().items()}
                         current_status = row.get('STATUS')
                     
                     if current_status != 'CREATED':
                         return json.dumps({"status": "ERROR", "message": f"Collaboration is not ready. Current status: {current_status}. Please wait for 'CREATED'."})

                 msg = []
                 if role_type == 'CONSUMER':
                     try:
                        session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.REVIEW", safe_collab_name)
                        msg.append("Reviewed")
                     except: pass
                 
                 session.call("SAMOOHA_BY_SNOWFLAKE_LOCAL_DB.COLLABORATION.JOIN", safe_collab_name)
                 msg.append("Join command submitted")
                 return json.dumps({"status": "SUCCESS", "message": ". ".join(msg)})
             except Exception as e:
                 if "side effects" in str(e).lower() or "accept_legal_terms" in str(e).lower():
                     return json.dumps({
                         "status": "WARNING", 
                         "message": f"Join requires manual acceptance of legal terms. Please run 'CALL samooha_by_snowflake_local_db.collaboration.join(\'{safe_collab_name}\')' in a worksheet."
                     })
                 return json.dumps({"status": "ERROR", "message": str(e)})
            
        elif action == 'VALIDATE':
            report = session.call("DCR_SNOWVA.MIGRATION.VALIDATE", cleanroom_name, safe_collab_name)
            return str(report)

        elif action == 'GENERATE_ANALYSIS':
            return json.dumps({"status": "SUCCESS", "message": "Analysis generation not fully supported in Consumer-empty mode yet."})

        elif action == 'TEARDOWN':
            res = session.call("DCR_SNOWVA.MIGRATION.TEARDOWN", safe_collab_name)
            return json.dumps({"status": "SUCCESS", "message": res})
        
        else: return json.dumps({"status": "ERROR", "message": "INVALID MODE"})

    except Exception as e:
        return json.dumps({"status": "ERROR", "message": str(e)})
$$;
