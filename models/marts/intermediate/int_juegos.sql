with juegos as (
    select 
        distinct videojuego,
        genero,
        empresa
    
     from {{ref("base_ventas")}}
), product as (
    select * from {{ref("stg_productos")}}
)

select 
    {{ dbt_utils.generate_surrogate_key(['j.videojuego']) }} as juego_id,
    j.videojuego,
    j.genero,
    j.empresa,
    precio_lanzamiento
from juegos j 
join product p 
on j.videojuego = p.videojuego