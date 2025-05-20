with paises as (
    select 
    lower(pais) as pais,
    lower(country) as country,
    lower(pays) as pays,
    abreviatura_1,
    abreviatura_2,
    codigo_telf,
    lower(continente) as continente
    
    from {{source("trabajo_final",'paises')}}
)

select 
    pais,
    country,
    pays,
    abreviatura_1,
    abreviatura_2,
    codigo_telf,
    continente

 from paises