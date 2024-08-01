with 
first_occurence_eoa as (
    select  
        s.user as "from",
        min(date) as fod
    from dune.pyor_xyz.result_arb_base_all_materialize_final s
    where 1=1
    group by 1
),
new_users_eoa as (
    select        
        fod as date,
        count(distinct "from") as new_eoa_addresses
    from first_occurence_eoa
    group by 1
),
total_addresses_eoa as (
    select 
        date,
        new_eoa_addresses,
        sum(new_eoa_addresses) over (order by date) as total_eoa_cumulative_addresses
    from new_users_eoa
),
grant_amount as (
    select 
        start_date,
        end_date,
        sum(grant_amount) over(order by start_date) as grant_amount
    from (
        select 
            case program
                when 'STIP' then date'2023-11-01'
                when 'LTIPP' then date'2024-06-01'
                else date'2024-01-01'
            end as start_date,
            case program
                when 'STIP' then date'2024-01-01'
                when 'LTIPP' then date'2024-07-01'
                else date'2024-06-01'
            end as end_date,
            sum(grant_amount) as grant_amount
        from dune.pyor_xyz.dataset_arb_incentives_distributed
        group by 1,2
    )
),
disbursed_amount as (
    select 
        sum(arb_claimed) as arb_claimed_total
    from dune.pyor_xyz.result_arb_claim_final_level_summary
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
        sum(txs) as txs,
        sum(calls) as calls,
        sum(giga_gas) as giga_gas,
        sum(fee_paid) as fee_paid,
        sum(l1_fee_paid) as l1_fee_paid,
        sum(seq_fee_paid) as seq_fee_paid
    from dune.pyor_xyz.result_arb_protocol_summary_daily
    group by 1
)

select s.*,
    e.price as eth_price,
    a.price as arb_price,
    sum(s.seq_fee_paid*e.price) over(order by s.date) as cumu_seq_fee_paid,
    sum(
        case when s.date <= g.start_date 
            then s.seq_fee_paid*e.price
        else 0 end
    ) over(order by s.date) as cumu_seq_fee_paid_pre_program,
    sum(
        case when s.date > g.start_date 
            then s.seq_fee_paid*e.price
        else 0 end
    ) over(order by s.date) as cumu_seq_fee_paid_post_program,
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
            ) over (order by s.date)
        else 0
    end as acq_cost_2,

    case when s.date >= g.start_date
        -- -- CALC acq_cost as incentives distributed per day * number days since start / total new addresses
        then (
                (d.arb_claimed_total*a.price)*date_diff('day', g.start_date, s.date)/
                date_diff('day', g.start_date, current_date)
            )/
            sum(
                case 
                    when s.date > g.start_date 
                    then coalesce(n.new_eoa_addresses,0)
                else 0 end
            ) over (order by s.date)
        else 0
    end as acq_cost_3
from summary_aggregated s
    left join grant_amount g
        on s.date > g.start_date 
        and s.date < g.end_date
    left join disbursed_amount d
        on 1=1
    left join eth_prices e
        on s.date = e.minute
    left join arb_prices a
        on g.start_date = a.minute
    left join total_addresses_eoa n 
        on s.date = n.date