{{ config(
    materialized='incremental',
    unique_key = 'videojuego'
    ) 
    }}

with ventas as (
    select 
    lower(nombre) as videojuego,
    lower(plataforma) as consola,
    year_published as lanzamiento,
    lower(genero) as genero,
    lower(publisher) as empresa,
    ventas_na,
    ventas_eu,
    ventas_jp,
    otras_ventas,
    reviews as critica,
    fecha_carga
    
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