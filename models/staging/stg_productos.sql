with prod as (
    select * from {{source('trabajo_final','producto')}}
)

select 
    lower(titulo) as videojuego,
    features_handheld,
    max_players as jugadores_max,
    multiplatform, 
    online,
    lower(genre) as genero,
    licensed,
    lower(publishers) as publishers,
    sequel,
    review_score as critica,
    sales, 
    used_price as precio_lanzamiento,
    lower(console) as consola,
    rating, 
    re_release,
    release_year,
    average,
    leisure,
    play_median,
    polled,
    rushed,
    comp_average,
    comp_leisure,
    comp_median,
    comp_polled,
    comp_rushed,
    extras_av,
    extras_leisure,
    extras_median,
    extras_polled,
    extras_rushed,
    story_av,
    story_leisure,
    story_median,
    story_polled,
    story_rushed
from prod 
order by videojuego