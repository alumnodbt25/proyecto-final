with prod as (
    select * from {{source('trabajo_final','producto')}}
)

select 
    cast(lower(titulo) as varchar(256)) as videojuego,
    cast(features_handheld as boolean) as features_handheld,
    cast(max_players as integer) as jugadores_max,
    cast(multiplatform as boolean) as multiplatform, 
    cast(online as boolean) as online,
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