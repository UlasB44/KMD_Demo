{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('raw', 'grades_raw') }}
),

cleaned as (
    select
        grade_id,
        student_id,
        class_id,
        upper(trim(subject)) as subject,
        grade_value,
        grade_date,
        upper(trim(term)) as term,
        teacher_id,
        _loaded_at,
        _source_file
    from source
    where grade_id is not null
      and grade_value is not null
    qualify row_number() over (partition by grade_id order by _loaded_at desc) = 1
)

select * from cleaned
