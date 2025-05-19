with ventass as (
    select * from {{ref("base_ventas")}}
), consolass as (
    select * from {{ref("stg_consolas")}}
), emp as (
    select * from {{ref("stg_desarrolladores")}}
), inf as (
    select * from {{ref("stg_inflacion")}}
)

select 
    ventas_id,
    videojuego,
    consola_id,
    v.consola,
    desarrollador_id,
    v.empresa,
    v.lanzamiento,
    ventas_na,
    ventas_eu,
    ventas_jp,
    otras_ventas,
    v.ventas_globales,
    critica

from ventass v
join consolass c
on v.consola = c.abreviatura
left join emp e 
on e.desarrollador = v.empresa
join inf i 
on i.anyo = v.lanzamiento
