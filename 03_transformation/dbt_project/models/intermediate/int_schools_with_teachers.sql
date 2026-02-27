{{
    config(
        materialized='ephemeral'
    )
}}

with schools as (
    select * from {{ ref('stg_schools') }}
),

teachers as (
    select * from {{ ref('stg_teachers') }}
),

teacher_counts as (
    select
        school_id,
        count(*) as total_teachers,
        count(case when is_active then 1 end) as active_teachers
    from teachers
    group by school_id
)

select
    s.*,
    coalesce(tc.total_teachers, 0) as total_teachers,
    coalesce(tc.active_teachers, 0) as active_teachers
from schools s
left join teacher_counts tc on s.school_id = tc.school_id
