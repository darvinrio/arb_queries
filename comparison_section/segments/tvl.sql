select 
    protocol,
    case 
        when tvl <= 2000000 then 'less than 2M'
        when tvl <= 20000000 then 'less than 20M'
        when tvl <= 100000000 then 'less than 100M'
        else 'more than 100M'
    end as segment
from dune.pyor_xyz.dataset_arb_tvl_defillama

set fallback_value = 'more than 100M'