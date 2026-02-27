{{
    config(
        materialized='table'
    )
}}

with grades as (
    select * from {{ ref('stg_grades') }}
),

schools as (
    select * from {{ ref('dim_schools') }}
),

municipalities as (
    select * from {{ source('shared', 'dim_municipalities') }}
),

aggregated as (
    select
        g.municipality_code,
        m.municipality_name,
        m.region,
        g.school_id,
        s.school_name,
        s.school_type,
        g.academic_year,
        g.subject,
        g.term,
        count(distinct g.student_id) as total_students,
        count(g.grade_record_id) as total_grades,
        round(avg(g.grade_numeric), 2) as avg_grade,
        round(median(g.grade_numeric), 2) as median_grade,
        min(g.grade_numeric) as min_grade,
        max(g.grade_numeric) as max_grade,
        sum(case when g.is_passing then 1 else 0 end) as passing_count,
        round(
            sum(case when g.is_passing then 1 else 0 end) / 
            nullif(count(*), 0) * 100, 
            1
        ) as passing_rate_pct,
        current_timestamp() as dbt_loaded_at
    from grades g
    left join schools s on g.school_id = s.school_id
    left join municipalities m on g.municipality_code = m.municipality_code
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
)

select * from aggregated
