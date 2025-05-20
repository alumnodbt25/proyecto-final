with ventass as (
    select * from {{ref("base_ventas")}}
), cons as (
    select * from {{ref("stg_consolas")}}
), prod as (
    select * from {{ref("stg_productos")}}
), inter as (

select 
    ventas_id,
    {{ dbt_utils.generate_surrogate_key(['v.videojuego']) }} as juego_id,
    v.videojuego,
    c.consola_id,
    v.consola,
    ROW_NUMBER() OVER(PARTITION BY v.videojuego,v.consola ORDER BY v.videojuego ASC) as dup,
    {{ dbt_utils.generate_surrogate_key(['v.empresa']) }} as empresa_id,
    v.empresa,
    v.lanzamiento,
    p.precio_lanzamiento as precio_dolares,
    ventas_na,
    ventas_eu,
    ventas_jp,
    otras_ventas,
    v.ventas_globales,
    v.critica

from ventass v
join cons c
on v.consola = c.abreviatura
left join prod p 
on v.videojuego = p.videojuego
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
    precio_dolares,
    ventas_na,
    ventas_eu,
    ventas_jp,
    otras_ventas,
    ventas_globales,
    critica

from inter
where dup = 1
order by videojuego