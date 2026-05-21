with sales as (

    select *, 
        extract(year from transaction_date) as year,
        extract(week from transaction_date) as week
    from {{ source('domyos', 'fact_sales') }}
    where item_operation_type = 'sale'
    and transaction_channel_type = 'offline'
            
), 

products as (
    select
        item_code,
        model_code,
        model_name,
        product_weight,
        product_nature,
        range_item,
        case
            when item_code in (2524419, 4361142) then 'kit_10kg'
            when product_nature in ('weight set', 'weight plate') then 'alternative'
            else 'other'
        end as product_category
    from {{ source('domyos', 'dim_model') }}
),

stores as (
    select
        store_code,
        sales_area,
        location,
        is_tested_region,
        family_range
    from {{ source('domyos', 'dim_store') }}
),
stock as (

    select
        store_code,
        item_code,
        stock_date,
        max(case when top_available_stock then 1  else 0 end ) as top_available_stock
    from {{ source('domyos', 'fact_stock') }}
    group by 1,2,3

)

select
    -- Identifiants transaction
    sales.transaction_id,
    sales.transaction_date,
    sales.year,
    sales.week,

    -- Dimensions magasin
    sales.store_code,
    stores.is_tested_region,
    stores.family_range,
    stores.location,
    stores.sales_area,

    -- Dimensions produit
    sales.item_code,
    products.model_code,
    products.model_name,
    products.product_weight,
    products.product_nature,
    products.range_item,
    products.product_category,

    -- Transaction
    sales.transaction_channel_type,
    sales.quantity,
    sales.gmv,

    -- Stock
    stock.top_available_stock

from sales

inner join products  
on sales.item_code = products.item_code

left join stores   
on sales.store_code = stores.store_code

left join stock      
on sales.store_code = stock.store_code
and sales.item_code  = stock.item_code
and sales.transaction_date = stock.stock_date