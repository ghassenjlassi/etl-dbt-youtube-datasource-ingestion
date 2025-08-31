{{ config(materialized='table') }}

with vids as (
  select * from {{ ref('stg_youtube_videos') }}
)
select
  video_id,
  channel_id,
  channel_title,
  title,
  published_at,
  published_date,
  publish_dow,
  published_hour_local,
  duration_seconds,
  length_bucket,
  view_count,
  like_count,
  comment_count,
  engagement_ratio,
  title_length,
  tag_count,
  default_language
from vids
order by published_at desc
