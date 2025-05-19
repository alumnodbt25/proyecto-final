with inf as (
    select * from {{source('trabajo_final','inflacion')}}
)

select 
    {{ dbt_utils.generate_surrogate_key(['anyo','infacion_global']) }} as inflacion_id,
    anyo,
    infacion_global as inflacion_global,
    case when cambio_euro_dolar IS NULL then '1' --Como no había euros, lo dejamos como relación 1:1,
    else cambio_euro_dolar                       -- al no tener los cambios de cada moneda
    end as cambio_euro_dolar,
    case when cambio_dolar_euro IS NULL then '1'
    else cambio_dolar_euro
    end as cambio_dolar_euro

 from inf