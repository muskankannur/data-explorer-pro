import streamlit as st
import pandas as pd
import altair as alt
import io
import re
from snowflake.snowpark import Session
from snowflake.snowpark.files import SnowflakeFile

# ---------------- CONNECTION HELPER ----------------
# This is the only change: ensuring the app can connect from the web
def get_session():
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except:
        return Session.builder.configs(st.secrets["snowflake"]).create()

session = get_session()

st.set_page_config(page_title="Data Explorer Pro", layout="wide")
st.title("📊 Data Explorer Pro")

# ---------------- SESSION STATE ----------------
if "df" not in st.session_state:
    st.session_state.df = None
if "ai_q" not in st.session_state:
    st.session_state.ai_q = ""
if "sql_q" not in st.session_state:
    st.session_state.sql_q = ""
if "table_name" not in st.session_state:
    st.session_state.table_name = None
if "tags" not in st.session_state:
    st.session_state.tags = {}

# ---------------- FUNCTIONS ----------------
def save_to_snowflake(df):
    table_name = "DATA_" + str(abs(hash(str(df.columns))))[:6]
    df_sync = df.astype(str)
    session.write_pandas(
        df_sync,
        table_name,
        auto_create_table=True,
        overwrite=True,
        table_type="transient"
    )
    st.session_state.table_name = table_name
    return table_name

def render_chart(data, x, y, chart_type="Bar"):
    if chart_type == "Bar":
        st.bar_chart(data, x=x, y=y)
    elif chart_type == "Line":
        st.line_chart(data, x=x, y=y)
    elif chart_type == "Area":
        st.area_chart(data, x=x, y=y)
    elif chart_type == "Pie":
        pie = alt.Chart(data.head(20)).mark_arc().encode(
            theta=alt.Theta(field=y, type="quantitative"),
            color=alt.Color(field=x, type="nominal"),
            tooltip=[x, y]
        ).properties(width=500, height=400)
        st.altair_chart(pie, use_container_width=True)

def classify_column(col):
    if re.search("email", col, re.IGNORECASE):
        return "Sensitive (Email)"
    elif re.search("name", col, re.IGNORECASE):
        return "PII"
    elif re.search("id", col, re.IGNORECASE):
        return "Identifier"
    return "General"

# ---------------- SIDEBAR ----------------
st.sidebar.header("📂 Data Source")
source = st.sidebar.radio(
    "Select Source",
    ["Upload File", "Snowflake Stage", "Snowflake Table"]
)

# ---------------- DATA LOADING ----------------
if source == "Upload File":
    file = st.sidebar.file_uploader("Upload CSV/XLSX", type=["csv", "xlsx"])
    if file and st.sidebar.button("Load File"):
        df = pd.read_csv(file) if file.name.endswith(".csv") else pd.read_excel(file)
        st.session_state.df = df
        table_name = save_to_snowflake(df)
        st.success(f"✅ File loaded into {table_name}")

elif source == "Snowflake Stage":
    try:
        stage = "MY_STREAMLIT_DB.PUBLIC.MY_STAGE"
        files_df = session.sql(f"LIST @{stage}").to_pandas()

        if files_df.empty:
            st.error("No files found in stage")
            st.stop()

        file_list = files_df.iloc[:, 0].tolist()
        selected_file = st.sidebar.selectbox("Select File", file_list)

        if st.sidebar.button("Load Stage File"):
            file_path = selected_file.split("/", 1)[1] if "/" in selected_file else selected_file

            if file_path.endswith(".csv"):
                with SnowflakeFile.open(f"@{stage}/{file_path}", "rb") as f:
                    df = pd.read_csv(io.BytesIO(f.readall()))
            else:
                with SnowflakeFile.open(f"@{stage}/{file_path}", "rb") as f:
                    df = pd.read_excel(io.BytesIO(f.readall()))

            st.session_state.df = df
            table_name = save_to_snowflake(df)
            st.success(f"✅ Loaded into table: {table_name}")

    except Exception as e:
        st.error(f"Stage Error: {e}")

elif source == "Snowflake Table":
    try:
        dbs = session.sql("SHOW DATABASES").to_pandas().iloc[:,1].tolist()
        db = st.sidebar.selectbox("Database", dbs)

        schemas = session.sql(f"SHOW SCHEMAS IN {db}").to_pandas().iloc[:,1].tolist()
        sch = st.sidebar.selectbox("Schema", schemas)

        tables = session.sql(f"SHOW TABLES IN {db}.{sch}").to_pandas().iloc[:,1].tolist()
        tbl = st.sidebar.selectbox("Table", tables)

        if st.sidebar.button("Load Table"):
            df = session.sql(f"SELECT * FROM {db}.{sch}.{tbl} LIMIT 10000").to_pandas()
            st.session_state.df = df
            table_name = save_to_snowflake(df)
            st.success(f"✅ Table loaded into {table_name}")

    except Exception as e:
        st.error(e)

if st.session_state.df is None:
    st.info("Load data first")
    st.stop()

df = st.session_state.df
table_name = st.session_state.table_name

num_cols = df.select_dtypes(include="number").columns.tolist()
cat_cols = df.select_dtypes(include="object").columns.tolist()
all_cols = df.columns.tolist()

# ---------------- TABS ----------------
tabs = st.tabs(["📋 Data", "📊 Charts", "🤖 AI Assistant", "🧠 SQL Assistant", "📚 Catalog"])

# ---------------- DATA TAB ----------------
with tabs[0]:
    st.dataframe(df.head(1000), use_container_width=True)

# ---------------- CHART TAB ----------------
with tabs[1]:
    st.subheader("📊 Charts")
    chart_type = st.selectbox("Chart Type", ["Bar", "Line", "Area", "Scatter", "Pie"])
    col_x = st.selectbox("X-Axis", all_cols)
    col_y = st.selectbox("Y-Axis", all_cols)

    try:
        if chart_type == "Scatter":
            if pd.api.types.is_numeric_dtype(df[col_x]) and pd.api.types.is_numeric_dtype(df[col_y]):
                plot_df = df[[col_x, col_y]].dropna()
                st.scatter_chart(plot_df)
            else:
                st.warning("Scatter requires numeric columns")
        elif chart_type == "Pie":
            agg_df = df[col_x].value_counts().reset_index().head(20)
            agg_df.columns = [col_x, "count"]
            render_chart(agg_df, col_x, "count", "Pie")
        else:
            if not pd.api.types.is_numeric_dtype(df[col_y]):
                agg_df = df.groupby(col_x)[col_y].count().reset_index()
                agg_df.columns = [col_x, "count"]
                render_chart(agg_df, col_x, "count", chart_type)
            else:
                agg_df = df.groupby(col_x)[col_y].mean().reset_index()
                render_chart(agg_df, col_x, col_y, chart_type)
    except Exception as e:
        st.error(f"Chart error: {e}")

# ---------------- AI TAB ----------------
with tabs[2]:
    st.subheader("🤖 AI Assistant")
    ai_chart_type = st.selectbox("Chart style", ["Bar", "Line", "Area", "Pie"])
    st.caption("Quick questions:")
    c1, c2, c3 = st.columns(3)
    if cat_cols and c1.button(f"Distribution of {cat_cols[0]}"):
        st.session_state.ai_q = f"distribution {cat_cols[0]}"
    if num_cols and c2.button(f"Average of {num_cols[0]}"):
        st.session_state.ai_q = f"average {num_cols[0]}"
    if c3.button("Summary stats"):
        st.session_state.ai_q = "summary"

    ai_q = st.text_input("Ask question", value=st.session_state.ai_q)

    try:
        if "distribution" in ai_q and cat_cols:
            vc = df[cat_cols[0]].value_counts().reset_index()
            vc.columns = [cat_cols[0], "count"]
            render_chart(vc, cat_cols[0], "count", ai_chart_type)
        elif "average" in ai_q and num_cols:
            st.metric("Average", df[num_cols[0]].mean())
        elif "summary" in ai_q:
            st.dataframe(df.describe())
    except Exception as e:
        st.error(e)

# ---------------- SQL TAB ----------------
with tabs[3]:
    st.subheader("🧠 SQL Assistant")
    st.caption("Quick queries:")
    if st.button("Top 10 rows"):
        st.session_state.sql_q = f"SELECT * FROM {table_name} LIMIT 10"
    query = st.text_area("Query", value=st.session_state.sql_q)
    if st.button("Run SQL"):
        try:
            result = session.sql(query).to_pandas()
            st.dataframe(result)
        except Exception as e:
            st.error(e)

# ---------------- CATALOG TAB ----------------
with tabs[4]:
    st.subheader("📚 Data Catalog")
    st.markdown("### 📊 Dataset Overview")
    oc1, oc2, oc3 = st.columns(3)
    oc1.metric("Rows", df.shape[0])
    oc2.metric("Columns", df.shape[1])
    oc3.metric("Memory (KB)", round(df.memory_usage().sum()/1024,2))

    st.markdown("### 🔍 Search Columns")
    search = st.text_input("Search column name")
    filtered_cols = [col for col in df.columns if search.lower() in col.lower()] if search else df.columns

    st.markdown("### 🧾 Column Metadata")
    meta = pd.DataFrame({
        "Column": filtered_cols,
        "Type": df[filtered_cols].dtypes.astype(str),
        "Nulls": df[filtered_cols].isnull().sum().values,
        "Null %": (df[filtered_cols].isnull().sum().values / len(df) * 100).round(2),
        "Unique": [df[col].nunique() for col in filtered_cols],
        "Sample": [str(df[col].dropna().iloc[0]) if not df[col].dropna().empty else "" for col in filtered_cols],
        "Classification": [classify_column(col) for col in filtered_cols]
    })
    st.dataframe(meta, use_container_width=True)

    st.markdown("### ⚠️ Data Quality Check")
    quality = []
    for col in df.columns:
        null_pct = df[col].isnull().mean() * 100
        unique_pct = df[col].nunique() / len(df) * 100
        if null_pct > 50: quality.append((col, "High Nulls"))
        elif unique_pct == 100: quality.append((col, "High Cardinality"))
        else: quality.append((col, "Good"))
    quality_df = pd.DataFrame(quality, columns=["Column", "Quality"])
    st.dataframe(quality_df, use_container_width=True)

    st.markdown("### 🏷️ Column Tagging")
    col_tag = st.selectbox("Select Column", df.columns)
    tag = st.text_input("Enter Tag")
    desc = st.text_input("Column Description")
    if st.button("Save Metadata"):
        if col_tag not in st.session_state.tags:
            st.session_state.tags[col_tag] = {}
        st.session_state.tags[col_tag]["tag"] = tag
        st.session_state.tags[col_tag]["description"] = desc
    st.json(st.session_state.tags)

    st.markdown("### 📊 Column Profiling")
    col_profile = st.selectbox("Select Column to Profile", df.columns)
    if pd.api.types.is_numeric_dtype(df[col_profile]):
        st.write(df[col_profile].describe())
        st.bar_chart(df[[col_profile]].dropna())
    else:
        st.dataframe(df[col_profile].value_counts().head(10))

    st.markdown("### 🧬 Data Lineage")
    st.info(f"Source: User Input ({source}) | Stored Table: {table_name} | Usage: Charts | AI Assistant | SQL Assistant")