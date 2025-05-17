with juegos as (
    select 
        distinct videojuego,
        genero,
        empresa
    
     from {{ref("base_ventas")}}
)

select 
    {{ dbt_utils.generate_surrogate_key(['videojuego']) }} as juego_id,
    videojuego,
    genero,
    empresa
from juegos