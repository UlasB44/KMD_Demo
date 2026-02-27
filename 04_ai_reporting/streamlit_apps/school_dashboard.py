"""
KMD Denmark - School Analytics Dashboard
Streamlit application for municipality school data analysis
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="KMD Schools Analytics",
    page_icon="🏫",
    layout="wide"
)

@st.cache_resource
def get_session():
    return get_active_session()

@st.cache_data(ttl=600)
def load_municipality_data():
    session = get_session()
    df = session.sql("""
        SELECT 
            municipality_name,
            COUNT(DISTINCT school_id) as num_schools,
            SUM(total_students) as total_students,
            ROUND(AVG(avg_grade), 2) as avg_grade,
            ROUND(AVG(passing_rate_pct), 1) as passing_rate
        FROM KMD_ANALYTICS.GOLD.AGG_MUNICIPALITY_PERFORMANCE
        GROUP BY municipality_name
        ORDER BY avg_grade DESC
    """).to_pandas()
    return df

@st.cache_data(ttl=600)
def load_school_performance():
    session = get_session()
    df = session.sql("""
        SELECT 
            municipality_name,
            school_name,
            school_type,
            avg_grade,
            passing_rate_pct,
            total_students_graded
        FROM KMD_ANALYTICS.GOLD.AGG_SCHOOL_PERFORMANCE
        ORDER BY avg_grade DESC
    """).to_pandas()
    return df

@st.cache_data(ttl=600)
def load_subject_performance():
    session = get_session()
    df = session.sql("""
        SELECT 
            subject,
            subject_category,
            ROUND(AVG(grade_numeric), 2) as avg_grade,
            COUNT(*) as total_grades,
            ROUND(SUM(CASE WHEN is_passing THEN 1 ELSE 0 END) / COUNT(*) * 100, 1) as passing_rate
        FROM KMD_ANALYTICS.GOLD.FACT_GRADES
        WHERE grade_numeric IS NOT NULL
        GROUP BY subject, subject_category
        ORDER BY avg_grade DESC
    """).to_pandas()
    return df

@st.cache_data(ttl=600)
def load_grade_distribution():
    session = get_session()
    df = session.sql("""
        SELECT 
            grade_value,
            grade_name,
            COUNT(*) as count
        FROM KMD_ANALYTICS.GOLD.FACT_GRADES
        WHERE grade_value IS NOT NULL
        GROUP BY grade_value, grade_name
        ORDER BY 
            CASE grade_value 
                WHEN '12' THEN 1 WHEN '10' THEN 2 WHEN '7' THEN 3 
                WHEN '4' THEN 4 WHEN '02' THEN 5 WHEN '00' THEN 6 
                WHEN '-3' THEN 7 
            END
    """).to_pandas()
    return df

st.title("KMD Denmark - School Analytics Dashboard")
st.markdown("### Multi-tenant Municipality School Data Analysis")

st.sidebar.header("Filters")

try:
    municipality_df = load_municipality_data()
    municipalities = ["All"] + municipality_df["MUNICIPALITY_NAME"].tolist()
    selected_municipality = st.sidebar.selectbox(
        "Select Municipality",
        municipalities
    )
except Exception as e:
    st.sidebar.warning("Sample data shown - connect to Snowflake for live data")
    selected_municipality = "All"
    municipality_df = pd.DataFrame({
        "MUNICIPALITY_NAME": ["Copenhagen", "Aarhus", "Odense", "Aalborg", "Esbjerg"],
        "NUM_SCHOOLS": [12, 8, 6, 5, 4],
        "TOTAL_STUDENTS": [8500, 4200, 2800, 1900, 1200],
        "AVG_GRADE": [7.8, 7.5, 7.3, 7.2, 7.0],
        "PASSING_RATE": [92.5, 90.2, 88.5, 87.8, 86.5]
    })

st.header("Municipality Overview")

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_schools = municipality_df["NUM_SCHOOLS"].sum()
    st.metric("Total Schools", f"{total_schools:,}")

with col2:
    total_students = municipality_df["TOTAL_STUDENTS"].sum()
    st.metric("Total Students", f"{total_students:,}")

with col3:
    avg_grade = municipality_df["AVG_GRADE"].mean()
    st.metric("Average Grade", f"{avg_grade:.2f}")

with col4:
    avg_passing = municipality_df["PASSING_RATE"].mean()
    st.metric("Avg Passing Rate", f"{avg_passing:.1f}%")

st.header("Performance by Municipality")

col1, col2 = st.columns(2)

with col1:
    fig_bar = px.bar(
        municipality_df,
        x="MUNICIPALITY_NAME",
        y="AVG_GRADE",
        title="Average Grade by Municipality",
        color="AVG_GRADE",
        color_continuous_scale="RdYlGn"
    )
    fig_bar.update_layout(
        xaxis_title="Municipality",
        yaxis_title="Average Grade",
        showlegend=False
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with col2:
    fig_scatter = px.scatter(
        municipality_df,
        x="TOTAL_STUDENTS",
        y="AVG_GRADE",
        size="NUM_SCHOOLS",
        color="MUNICIPALITY_NAME",
        title="Students vs Performance",
        hover_data=["PASSING_RATE"]
    )
    fig_scatter.update_layout(
        xaxis_title="Total Students",
        yaxis_title="Average Grade"
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

try:
    school_df = load_school_performance()
    if selected_municipality != "All":
        school_df = school_df[school_df["MUNICIPALITY_NAME"] == selected_municipality]
    
    st.header("School Performance Ranking")
    
    st.dataframe(
        school_df.head(15),
        column_config={
            "MUNICIPALITY_NAME": "Municipality",
            "SCHOOL_NAME": "School",
            "SCHOOL_TYPE": "Type",
            "AVG_GRADE": st.column_config.NumberColumn("Avg Grade", format="%.2f"),
            "PASSING_RATE_PCT": st.column_config.ProgressColumn(
                "Passing Rate",
                format="%.1f%%",
                min_value=0,
                max_value=100
            ),
            "TOTAL_STUDENTS_GRADED": "Students"
        },
        hide_index=True,
        use_container_width=True
    )
except Exception as e:
    st.info("Connect to Snowflake to view school performance data")

try:
    subject_df = load_subject_performance()
    
    st.header("Subject Performance Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig_subject = px.bar(
            subject_df,
            x="SUBJECT",
            y="AVG_GRADE",
            color="SUBJECT_CATEGORY",
            title="Average Grade by Subject",
            barmode="group"
        )
        fig_subject.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_subject, use_container_width=True)
    
    with col2:
        fig_category = px.pie(
            subject_df,
            names="SUBJECT_CATEGORY",
            values="TOTAL_GRADES",
            title="Grade Distribution by Subject Category"
        )
        st.plotly_chart(fig_category, use_container_width=True)

except Exception as e:
    st.info("Connect to Snowflake to view subject analysis")

try:
    grade_dist_df = load_grade_distribution()
    
    st.header("Grade Distribution")
    
    fig_dist = px.bar(
        grade_dist_df,
        x="GRADE_VALUE",
        y="COUNT",
        color="GRADE_VALUE",
        title="Distribution of Grades (Danish 7-point Scale)",
        color_discrete_map={
            "12": "#2E7D32", "10": "#4CAF50", "7": "#8BC34A",
            "4": "#FFEB3B", "02": "#FF9800", "00": "#F44336", "-3": "#B71C1C"
        }
    )
    fig_dist.update_layout(
        xaxis_title="Grade",
        yaxis_title="Count",
        showlegend=False
    )
    st.plotly_chart(fig_dist, use_container_width=True)

except Exception as e:
    grade_dist_df = pd.DataFrame({
        "GRADE_VALUE": ["12", "10", "7", "4", "02", "00", "-3"],
        "COUNT": [1200, 2500, 4500, 2800, 1500, 600, 200]
    })
    fig_dist = px.bar(
        grade_dist_df,
        x="GRADE_VALUE",
        y="COUNT",
        title="Grade Distribution (Sample Data)"
    )
    st.plotly_chart(fig_dist, use_container_width=True)

st.sidebar.markdown("---")
st.sidebar.markdown("### About")
st.sidebar.info("""
**KMD Schools Analytics**

Multi-tenant solution for Danish municipalities.
Powered by Snowflake.

Features:
- Real-time analytics
- Dynamic data masking
- Role-based access
""")

st.sidebar.markdown("---")
st.sidebar.caption("KMD Denmark - Workshop Demo")
