with paises as (
    select 
    cast(lower(pais) as varchar(256)) as pais,
    cast(lower(country) as varchar(256)) as country,
    cast(lower(pays) as varchar(256)) as pays,
    cast(abreviatura_1 as varchar(4)) as abreviatura_1,
    cast(abreviatura_2 as varchar(4)) as abreviatura_2,
    cast(replace(codigo_telf,' ','') as integer) as codigo_telf,
    cast(lower(continente) as varchar(256)) as continente
    
    from {{source("trabajo_final",'paises')}}
)

select 
    pais,
    case when country = 'united states of america' then 'united states'
    else country
    end as country,
    pays,
    abreviatura_1,
    abreviatura_2,
    codigo_telf,
    continente

 from paises