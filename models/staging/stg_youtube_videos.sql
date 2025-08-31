{{ config(materialized='table') }}

-- Read raw CSV directly with DuckDB
with raw as (
  select *
  from read_csv_auto('data/raw/youtube_videos.csv', HEADER=true)
),

dur as (
  select
    *,
    coalesce(try_cast(regexp_extract(duration, 'PT(\\d+)H', 1) as int), 0) as _h,
    coalesce(try_cast(regexp_extract(duration, 'PT(?:\\d+H)?(\\d+)M', 1) as int), 0) as _m,
    coalesce(try_cast(regexp_extract(duration, 'PT(?:\\d+H)?(?:\\d+M)?(\\d+)S', 1) as int), 0) as _s
  from raw
),

typed as (
  select
    cast(video_id as varchar)                 as video_id,
    cast(channel_id as varchar)               as channel_id,
    cast(channel_title as varchar)            as channel_title,
    cast(title as varchar)                    as title,
    cast(description as varchar)              as description,
    cast(published_at as timestamp)           as published_at,
    cast(duration as varchar)                 as duration_iso,
    (_h * 3600 + _m * 60 + _s)                as duration_seconds,
    cast(tags as varchar)                     as tags_raw,
    cast(default_language as varchar)         as default_language,
    try_cast(view_count as int)               as view_count,
    try_cast(like_count as int)               as like_count,
    try_cast(comment_count as int)            as comment_count
  from dur
),

derived as (
  select
    *,
    case
      when view_count is null or view_count = 0 then null
      else round( (coalesce(like_count,0) + coalesce(comment_count,0))::double / view_count, 4)
    end as engagement_ratio,

    length(title)                               as title_length,
    case when tags_raw is null or tags_raw = '' then 0
         else length(tags_raw) - length(replace(tags_raw, '|', '')) + 1 end as tag_count,

    cast(date_trunc('day', published_at) as date)         as published_date,
    strftime(published_at, '%H:00')                       as published_hour_local, 
    case extract(dow from published_at)
      when 0 then 'Sun' when 1 then 'Mon' when 2 then 'Tue'
      when 3 then 'Wed' when 4 then 'Thu' when 5 then 'Fri'
      else 'Sat' end                                       as publish_dow,

    case
      when duration_seconds < 60 then 'Short (<60s)'
      when duration_seconds < 2*60 then '1–2 min'
      when duration_seconds < 10*60 then '2–10 min'
      when duration_seconds is null then 'Unknown'
      else '10+ min'
    end as length_bucket
  from typed
)

select * from derived
