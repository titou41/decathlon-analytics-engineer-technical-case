select
    year,
    week,
    store_code,
    is_tested_region,
    family_range,
    location,
    sales_area,
    -- Flag période test
    case
        when year = 2023 and week >= 35 and week <= 41
        then true
        else false
    end as is_test_period,

    -- Kit 10kg
    coalesce(sum(case when product_category = 'kit_10kg' then gmv end), 0) as gmv_kit_10kg,
    coalesce(sum(case when product_category = 'kit_10kg' then quantity end), 0) as quantity_kit_10kg,
    count(distinct case when product_category = 'kit_10kg' then transaction_id end) as nb_transaction_kit_10kg,

    -- Alternatives
    coalesce(sum(case when product_category = 'alternative' then gmv end), 0) as gmv_alternatives,
    coalesce(sum(case when product_category = 'alternative' then quantity end), 0) as quantity_alternatives,
    count(distinct case when product_category = 'alternative' then transaction_id end) as nb_transaction_alternatives,

    -- Totaux
    coalesce(sum(gmv), 0) as gmv_total,
    round(coalesce(sum(gmv), 0) / nullif(sales_area, 0), 4) as gmv_per_sqm,

    -- Disponibilité stock
    avg(top_available_stock ) as availability_rate, 

    coalesce(sum(case when product_category = 'alternative' then gmv end), 0)  - coalesce(sum(case when product_category = 'kit_10kg' then gmv end), 0) as net_uplift_estimated

from {{ref('int_experiment_sales_enriched')}} as sales 
group by 1, 2, 3, 4, 5, 6, 7