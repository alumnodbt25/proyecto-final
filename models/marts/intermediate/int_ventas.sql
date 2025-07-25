{{ config(
    materialized='incremental',
    unique_key = 'videojuego'
    ) 
    }}
    
with ventass as (
    select * from {{ref("stg_ventas")}}
), cons as (
    select * from {{ref("stg_consolas")}}
), prod as (
    select * from {{ref("stg_productos")}}
), inter as (

select 
    ventas_id,
    juego_id,
    v.videojuego,
    c.consola_id,
    c.nombre_2,
    v.consola,
    ROW_NUMBER() OVER(PARTITION BY v.videojuego,v.consola ORDER BY v.videojuego ASC) as dupi,
    empresa_id,
    v.empresa,
    v.lanzamiento,
    v.genero,
    p.precio_lanzamiento as precio_dolares,
    ventas_na,
    ventas_eu,
    ventas_jp,
    otras_ventas,
    v.critica,
    v.fecha_carga

from ventass v
join cons c
on v.consola = c.abreviatura
left join prod p 
on v.videojuego = p.videojuego and p.consola = c.nombre_2
)

select 
    ventas_id,
    juego_id,
    videojuego,
    consola_id,
    consola,
    empresa_id,
    empresa,
    lanzamiento,
    genero,
    precio_dolares,
    ventas_na,
    ventas_eu,
    ventas_jp,
    otras_ventas,
    critica,
    fecha_carga

from inter
where dupi = 1
order by videojuego
