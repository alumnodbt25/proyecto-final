with inf as (
    select * from {{source('trabajo_final','inflacion')}}
)

select 
    {{ dbt_utils.generate_surrogate_key(['anyo','infacion_global']) }} as inflacion_id,
    anyo,
    infacion_global as inflacion_global,
    cambio_euro_dolar,
    cambio_dolar_euro

 from inf