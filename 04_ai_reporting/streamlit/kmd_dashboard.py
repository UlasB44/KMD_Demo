import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="KMD Schools Dashboard", page_icon="🏫", layout="wide")

session = get_active_session()

st.title("🏫 KMD Danish Schools Analytics")
st.markdown("Multi-tenant analytics dashboard for Danish municipality schools")

tab1, tab2, tab3 = st.tabs(["📊 Municipality Overview", "🏫 School Details", "📚 Class Enrollment"])

with tab1:
    st.header("Municipality Summary")
    
    df_muni = session.sql("""
        SELECT municipality_name, school_count, teacher_count, student_count, 
               total_capacity, capacity_utilization_pct
        FROM KMD_ANALYTICS.MARTS.DT_MUNICIPALITY_SUMMARY
        ORDER BY student_count DESC
    """).to_pandas()
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Schools", df_muni['SCHOOL_COUNT'].sum())
    col2.metric("Total Teachers", df_muni['TEACHER_COUNT'].sum())
    col3.metric("Total Students", df_muni['STUDENT_COUNT'].sum())
    col4.metric("Avg Utilization", f"{df_muni['CAPACITY_UTILIZATION_PCT'].mean():.1f}%")
    
    st.subheader("Students by Municipality")
    st.bar_chart(df_muni.set_index('MUNICIPALITY_NAME')['STUDENT_COUNT'])
    
    st.subheader("Capacity Utilization by Municipality")
    st.dataframe(df_muni, use_container_width=True)

with tab2:
    st.header("School Performance")
    
    municipalities = session.sql("SELECT DISTINCT municipality_name FROM KMD_ANALYTICS.MARTS.DT_MUNICIPALITY_SUMMARY").to_pandas()
    selected_muni = st.selectbox("Select Municipality", municipalities['MUNICIPALITY_NAME'].tolist())
    
    muni_codes = {'Copenhagen': 101, 'Aarhus': 751, 'Odense': 461, 'Aalborg': 851, 'Esbjerg': 561}
    muni_code = muni_codes.get(selected_muni, 101)
    
    df_schools = session.sql(f"""
        SELECT school_name, school_type, city, current_students, teacher_count, 
               student_capacity, student_teacher_ratio
        FROM KMD_ANALYTICS.MARTS.DT_SCHOOL_SUMMARY
        WHERE municipality_code = {muni_code}
        ORDER BY current_students DESC
    """).to_pandas()
    
    st.subheader(f"Schools in {selected_muni}")
    st.dataframe(df_schools, use_container_width=True)
    
    st.subheader("Student-Teacher Ratio Distribution")
    st.bar_chart(df_schools.set_index('SCHOOL_NAME')['STUDENT_TEACHER_RATIO'])

with tab3:
    st.header("Class Enrollment")
    
    df_classes = session.sql("""
        SELECT school_name, class_name, grade, enrolled_students, max_students, 
               available_seats, enrollment_pct
        FROM KMD_ANALYTICS.MARTS.DT_CLASS_ENROLLMENT
        WHERE available_seats > 0
        ORDER BY available_seats DESC
        LIMIT 50
    """).to_pandas()
    
    col1, col2 = st.columns(2)
    col1.metric("Classes with Open Seats", len(df_classes))
    col2.metric("Total Available Seats", df_classes['AVAILABLE_SEATS'].sum())
    
    st.subheader("Classes with Available Seats")
    st.dataframe(df_classes, use_container_width=True)
