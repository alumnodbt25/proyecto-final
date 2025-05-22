{{ config(
    materialized='incremental',
    unique_key = 'videojuego'
    ) 
    }}

with ventas as (
    select 
    cast(lower(nombre) as varchar(256)) as videojuego,
    cast(lower(plataforma)as varchar(256)) as consola,
    cast(year_published as integer) as lanzamiento,
    cast(lower(genero) as varchar(256)) as genero,
    cast(lower(publisher) as varchar(256)) as empresa,
    cast(replace(ventas_na,',','.') as decimal(38,2)) as ventas_na,
    cast(replace(ventas_eu,',','.') as decimal(38,2)) as ventas_eu,
    cast(replace(ventas_jp,',','.') as decimal(38,2)) as ventas_jp,
    cast(replace(otras_ventas,',','.') as decimal(38,2)) as otras_ventas,
    cast(reviews as varchar(256)) as critica,
    TO_DATE(fecha_carga, 'DD-MM-YYYY') as fecha_carga
    
     from {{source('trabajo_final','ventas')}}
)

select 
    {{ dbt_utils.generate_surrogate_key(['videojuego','consola']) }} as ventas_id,
    {{ dbt_utils.generate_surrogate_key(['videojuego']) }} as juego_id,
    videojuego,
    consola,
    {{ dbt_utils.generate_surrogate_key(['empresa']) }} as empresa_id,
    empresa,
    lanzamiento,
    genero,
    ventas_na,
    ventas_eu,
    ventas_jp,
    otras_ventas,
    critica,
    fecha_carga

from ventas

{% if is_incremental() %}

  where fecha_carga >= (select max(fecha_carga) from {{ this }})

{% endif %}