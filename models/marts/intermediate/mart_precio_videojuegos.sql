with je as (
    select * from {{ref("stg_productos")}}
)

select
    release_year,
    precio_medio
    from ( select 
    release_year,
    avg(precio_lanzamiento) over (partition by release_year order by release_year) as precio_medio
from je)
group by release_year, precio_medio
order by release_year 