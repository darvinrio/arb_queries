with 
segmentation as (
    select 
        protocol,
        segment
    from query_{{segment_query}}
),
first_occurence_eoa as (
    select  
        s.user as "from",
        coalesce(u.segment, {{fallback_value}}) as segment,
        min(date) as fod
    from dune.pyor_xyz.result_arb_base_all_materialize_final s
        left join segmentation u 
            on s.protocol = u.protocol
    where 1=1
    -- and protocol = 'Camelot'
    group by 1,2
),
new_users_eoa as (
    select        
        fod as date,
        segment,
        count(distinct "from") as new_eoa_addresses
    from first_occurence_eoa
    group by 1,2
),
total_addresses_eoa as (
    select 
        date,
        segment,
        new_eoa_addresses,
        sum(new_eoa_addresses) over (partition by segment order by date) as total_eoa_cumulative_addresses
    from new_users_eoa
),
grant_amount as (
    select 
        segment,
        date'2023-11-01' as start_date,
        sum(grant_amount) as grant_amount
    from dune.pyor_xyz.dataset_arb_incentives_distributed s
        left join segmentation u 
                on s.protocol = u.protocol
    group by 1,2
    -- where protocol = 'Camelot' 
),
disbursed_amount as (
    select 
        segment,
        sum(arb_claimed) as arb_claimed_total
    from dune.pyor_xyz.result_arb_claim_final_level_summary s
        left join segmentation u 
                on s.protocol = u.protocol
    group by 1
),
arb_prices as (
    select * from prices.usd
    where minute in (
        date'2023-11-01',
        date'2024-01-01',
        date'2024-06-01'
    )
    and blockchain = 'arbitrum'
    and contract_address = 0x912CE59144191C1204E64559FE8253a0e49E6548
),
eth_prices as (
    select * from prices.usd
    where minute = date_trunc('day', minute)
    and minute > date'2023-03-01'
    and blockchain = 'ethereum'
    and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
),
summary_aggregated as (
    select 
        date,
        segment,
        sum(txs) as txs,
        sum(calls) as calls,
        sum(giga_gas) as giga_gas,
        sum(fee_paid) as fee_paid,
        sum(l1_fee_paid) as l1_fee_paid,
        sum(seq_fee_paid) as seq_fee_paid
    from dune.pyor_xyz.result_arb_protocol_summary_daily s
        left join segmentation u 
                on s.protocol = u.protocol
    group by 1,2
)

select s.*,
    e.price as eth_price,
    a.price as arb_price,
    sum(s.seq_fee_paid*e.price) over(partition by s.segment order by s.date) as cumu_seq_fee_paid,
    sum(
        case when s.date <= g.start_date 
            then s.seq_fee_paid*e.price
        else 0 end
    ) over(partition by s.segment order by s.date) as cumu_seq_fee_paid_pre_program,
    sum(
        case when s.date > g.start_date 
            then s.seq_fee_paid*e.price
        else 0 end
    ) over(partition by s.segment order by s.date) as cumu_seq_fee_paid_post_program,
    g.grant_amount * a.price as grant_usd,
    d.arb_claimed_total * a.price as disbursed_usd,
    case when s.date >= g.start_date
        -- CALC acq_cost as grant amount spent per day / new users 
            then ((
                    (g.grant_amount*a.price)/
                    date_diff('day', g.start_date, current_date)
                )/
                n.new_eoa_addresses 
            )
        else 0
    end as acq_cost,
    
    case when s.date >= g.start_date
        -- -- CALC acq_cost as grant amount spent per day * number days since start / total new addresses
        then (
                (g.grant_amount*a.price)*date_diff('day', g.start_date, s.date)/
                date_diff('day', g.start_date, current_date)
            )/
            sum(
                case 
                    when s.date > g.start_date 
                    then coalesce(n.new_eoa_addresses,0)
                else 0 end
            ) over (partition by s.segment order by s.date)
        else 0
    end as acq_cost_2,


    case when s.date >= g.start_date
        -- -- CALC acq_cost as grant amount spent per day * number days since start / total new addresses
        then (
                (d.arb_claimed_total*a.price)*date_diff('day', g.start_date, s.date)/
                date_diff('day', g.start_date, current_date)
            )/
            sum(
                case 
                    when s.date > g.start_date 
                    then coalesce(n.new_eoa_addresses,0)
                else 0 end
            ) over (partition by s.segment order by s.date)
        else 0
    end as acq_cost_3
from summary_aggregated s
    left join grant_amount g
        on s.segment = g.segment
    left join disbursed_amount d
        on s.segment = d.segment
    left join eth_prices e
        on s.date = e.minute
    left join arb_prices a
        on g.start_date = a.minute
    left join total_addresses_eoa n 
        on s.date = n.date
        and s.segment = n.segment
-- where s.protocol = 'Camelot'