with juegos as (
    select 
        distinct videojuego,
        genero,
        empresa
    
     from {{ref("stg_ventas")}}
), product as (
    select * from {{ref("stg_productos")}}
), interr as (

select 
    {{ dbt_utils.generate_surrogate_key(['j.videojuego']) }} as juego_id,
    j.videojuego,
    j.genero,
    j.empresa,
    multiplatform,
    online,
    average,
    ROW_NUMBER() OVER(PARTITION BY j.videojuego,j.genero,j.empresa,multiplatform,online,average ORDER BY j.videojuego ASC) as dupp,
from juegos j 
left join product p 
on j.videojuego = p.videojuego
order by videojuego)

select 
    juego_id,
    videojuego,
    genero,
    empresa,
    multiplatform,
    online,
    average
from interr
where dupp = '1'
order by videojuego
