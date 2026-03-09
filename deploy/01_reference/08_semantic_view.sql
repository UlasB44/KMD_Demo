-- ============================================================================
-- KMD WORKSHOP - STEP 8: SEMANTIC VIEW (Cortex Analyst)
-- ============================================================================
-- Creates semantic view for natural language queries
-- ============================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE KMD_WH;

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS KMD_ANALYTICS.SEMANTIC_MODELS;

-- ============================================================================
-- CREATE SEMANTIC VIEW
-- ============================================================================

CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  'KMD_ANALYTICS.SEMANTIC_MODELS',
  $$name: kmd_schools_analytics
description: Semantic model for Danish municipality school analytics covering schools, classes, and municipality-level metrics for KMD Denmark.

tables:
  - name: school_summary
    description: School-level summary with student and teacher counts, capacity utilization, and student-teacher ratios
    base_table:
      database: KMD_ANALYTICS
      schema: MARTS
      table: DT_SCHOOL_SUMMARY

    dimensions:
      - name: school_id
        description: Unique identifier for each school
        expr: SCHOOL_ID
        data_type: TEXT
        
      - name: school_name
        description: Name of the school
        expr: SCHOOL_NAME
        data_type: TEXT
        synonyms:
          - skole navn
          - school
          - skole
        
      - name: municipality_code
        description: Danish municipality code (101=Copenhagen, 751=Aarhus, 461=Odense, 851=Aalborg, 561=Esbjerg)
        expr: MUNICIPALITY_CODE
        data_type: NUMBER
        synonyms:
          - kommune kode
          - municipality
          - kommune
        
      - name: school_type
        description: Type of school (e.g., Folkeskole, Gymnasium)
        expr: SCHOOL_TYPE
        data_type: TEXT
        
      - name: city
        description: City where school is located
        expr: CITY
        data_type: TEXT

    facts:
      - name: student_capacity
        description: Maximum number of students the school can accommodate
        expr: STUDENT_CAPACITY
        data_type: NUMBER
          
      - name: current_students
        description: Current number of enrolled students
        expr: CURRENT_STUDENTS
        data_type: NUMBER
        synonyms:
          - students
          - elever
          
      - name: teacher_count
        description: Number of teachers at the school
        expr: TEACHER_COUNT
        data_type: NUMBER
        synonyms:
          - teachers
          
      - name: student_teacher_ratio
        description: Ratio of students to teachers
        expr: STUDENT_TEACHER_RATIO
        data_type: NUMBER

  - name: class_enrollment
    description: Class-level enrollment data with capacity and availability metrics
    base_table:
      database: KMD_ANALYTICS
      schema: MARTS
      table: DT_CLASS_ENROLLMENT

    dimensions:
      - name: class_id
        description: Unique identifier for each class
        expr: CLASS_ID
        data_type: TEXT
        
      - name: class_name
        description: Name of the class (e.g., 1.A, 2.B)
        expr: CLASS_NAME
        data_type: TEXT
        synonyms:
          - klasse
        
      - name: school_name
        description: Name of the school
        expr: SCHOOL_NAME
        data_type: TEXT
        
      - name: municipality_code
        description: Danish municipality code
        expr: MUNICIPALITY_CODE
        data_type: NUMBER
        
      - name: grade
        description: Grade level (0-9 for folkeskole)
        expr: GRADE
        data_type: NUMBER
        synonyms:
          - klassetrin
        
      - name: academic_year
        description: Academic year (e.g., 2024-2025)
        expr: ACADEMIC_YEAR
        data_type: TEXT

    facts:
      - name: max_students
        description: Maximum number of students allowed in class
        expr: MAX_STUDENTS
        data_type: NUMBER
        
      - name: enrolled_students
        description: Current number of enrolled students
        expr: ENROLLED_STUDENTS
        data_type: NUMBER
        synonyms:
          - students
          - elever
          
      - name: available_seats
        description: Number of available seats in class
        expr: AVAILABLE_SEATS
        data_type: NUMBER
          
      - name: enrollment_pct
        description: Percentage of class capacity filled
        expr: ENROLLMENT_PCT
        data_type: NUMBER

  - name: municipality_summary
    description: Municipality-level aggregated metrics for schools, teachers, and students
    base_table:
      database: KMD_ANALYTICS
      schema: MARTS
      table: DT_MUNICIPALITY_SUMMARY

    dimensions:
      - name: municipality_code
        description: Danish municipality code
        expr: MUNICIPALITY_CODE
        data_type: NUMBER
        
      - name: municipality_name
        description: Name of the municipality (Copenhagen, Aarhus, Odense, Aalborg, Esbjerg)
        expr: MUNICIPALITY_NAME
        data_type: TEXT
        synonyms:
          - kommune

    facts:
      - name: school_count
        description: Total number of schools in municipality
        expr: SCHOOL_COUNT
        data_type: NUMBER
          
      - name: teacher_count
        description: Total number of teachers in municipality
        expr: TEACHER_COUNT
        data_type: NUMBER
          
      - name: student_count
        description: Total number of students in municipality
        expr: STUDENT_COUNT
        data_type: NUMBER
        synonyms:
          - students
          - elever
          
      - name: total_capacity
        description: Combined student capacity of all schools
        expr: TOTAL_CAPACITY
        data_type: NUMBER
        
      - name: capacity_utilization_pct
        description: Percentage of total capacity being used
        expr: CAPACITY_UTILIZATION_PCT
        data_type: NUMBER

verified_queries:
  - name: vqr_0
    question: How many students are in each municipality?
    sql: |
      SELECT municipality_name, student_count
      FROM KMD_ANALYTICS.MARTS.DT_MUNICIPALITY_SUMMARY
      ORDER BY student_count DESC

  - name: vqr_1
    question: Which schools have the highest student-teacher ratio?
    sql: |
      SELECT school_name, municipality_code, current_students, teacher_count, student_teacher_ratio
      FROM KMD_ANALYTICS.MARTS.DT_SCHOOL_SUMMARY
      ORDER BY student_teacher_ratio DESC
      LIMIT 10

  - name: vqr_2
    question: What is the capacity utilization for each municipality?
    sql: |
      SELECT municipality_name, school_count, student_count, total_capacity, capacity_utilization_pct
      FROM KMD_ANALYTICS.MARTS.DT_MUNICIPALITY_SUMMARY
      ORDER BY capacity_utilization_pct DESC

  - name: vqr_3
    question: Which classes have available seats?
    sql: |
      SELECT class_name, school_name, grade, enrolled_students, max_students, available_seats
      FROM KMD_ANALYTICS.MARTS.DT_CLASS_ENROLLMENT
      WHERE available_seats > 0
      ORDER BY available_seats DESC$$,
  FALSE
);

-- ============================================================================
-- VERIFY
-- ============================================================================
SHOW SEMANTIC VIEWS IN SCHEMA KMD_ANALYTICS.SEMANTIC_MODELS;
