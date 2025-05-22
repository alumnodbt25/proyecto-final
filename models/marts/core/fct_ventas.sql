{{ config(
    materialized='incremental',
    unique_key = 'videojuego'
    ) 
    }}

with ventass as (
    select * from {{ref("int_ventas")}}
), inf as (
    select * from {{ref("dim_inflacion")}}
) 

select 
    v.ventas_id,
    v.juego_id,
    v.videojuego,
    v.consola_id,
    v.consola,
    v.empresa_id,
    v.empresa,
    v.lanzamiento,
    v.precio_dolares,
    v.precio_dolares * i.cambio_dolar_euro as precio_euros,
    v.ventas_na,
    v.ventas_eu,
    v.ventas_jp,
    v.otras_ventas,
    v.ventas_na + v.ventas_eu + v.ventas_jp + v.otras_ventas as ventas_globales,
    v.critica,
    ventas_globales * precio_dolares as ganancias_dolares,
    v.fecha_carga

from ventass v
join inf i 
on i.fecha = v.lanzamiento
order by ventas_globales DESC


