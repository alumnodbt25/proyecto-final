--Dimensión de los videojuegos con sus categorías y compañías
with juegoss as (
    select * from {{ref("int_juegos")}}
)

select 
   juego_id,
    videojuego,
    genero,
    empresa,
    multiplatform,
    online,
    average
from juegoss 
order by videojuego