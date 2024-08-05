select 
    protocol,
    case 
        when grants <= 200000 then 'less than 200K'
        when grants <= 400000 then 'less than 400K'
        when grants <= 1000000 then 'less than 1M'
        else 'more than 1M'
    end as segment
from dune.pyor_xyz.dataset_arb_tvl_defillama

set fallback_value = 'more than 1M'