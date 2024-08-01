with 
user_segmentation as (
    select 
        user,
        segment
    from query_{{user_segment_query}}
),
protocol_segmentation as (
    select 
        protocol,
        segment
    from query_{{protocol_segment_query}}
),
ethereum_summary_daily as (
    select
        date_trunc('day',date) as date,
        s.user as "from",
        coalesce(p.segment, {{protocol_fallback_value}}) as protocol_segment,
        coalesce(u.segment, {{user_fallback_value}}) as segment,
    from dune.pyor_xyz.result_arb_base_all_materialize_final s
        left join segmentation u 
            on s.user = u.user
        left join protocol_segmentation p 
            on s.protocol = p.protocol
    where 1 = 1
    -- and protocol = 'Camelot'
    group by 1, 2, 3, 4
),

date_range as (
    select
        min(date) as min_date,
        max(date) as max_date
    from ethereum_summary_daily
),
    
sequence_table AS (
    SELECT date_add('day', i, min_date) AS date_sequence
    FROM date_range
    CROSS JOIN UNNEST(SEQUENCE(0, date_diff('day', min_date, max_date))) AS t(i)
),

diff_calculated as (
        select /*+ broadcast(sequence_table) */
            sequence_table.date_sequence,
            ethereum_summary_daily."date",
            ethereum_summary_daily."from",
            ethereum_summary_daily.protocol_segment,
            ethereum_summary_daily.segment,
            date_diff('day', ethereum_summary_daily.date,sequence_table.date_sequence) as diff 
        from sequence_table 
        join ethereum_summary_daily 
            on sequence_table.date_sequence >= ethereum_summary_daily.date
            and sequence_table.date_sequence <= date_add('day',30,ethereum_summary_daily.date)
            and date_diff('day',sequence_table.date_sequence,sequence_table.date_sequence) = date_diff('day',ethereum_summary_daily.date,ethereum_summary_daily.date)
    ),

final as (
    select
        date_sequence as date,
        protocol_segment,
        segment,
        count(distinct case when diff = 0 then "from" end) as dau, --r0_active_users_avalanche,
        count(distinct case when diff >= 0 and diff <= 6 then "from" end) as wau, --r_minus_7_active_users_avalanche,
        count(distinct case when diff >= 0 and diff <= 29 then "from" end) as mau --r_minus_30__active_users_avalanche
    from diff_calculated
    group by 1,2,3
)

select *,
    cast(dau as double)/cast(mau as double) as dau_by_mau
from final