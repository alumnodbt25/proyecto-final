with inf as (
    select 
    cast(anyo as integer) as anyo,
    cast(replace(infacion_global,',','.') as decimal(38,2)) as inflacion_global,
    cast(replace(cambio_euro_dolar,',','.') as decimal(38,2)) as cambio_euro_dolar,
    cast(replace(cambio_dolar_euro,',','.') as decimal(38,2)) as cambio_dolar_euro,
     from {{source('trabajo_final','inflacion')}}
)

select 
    {{ dbt_utils.generate_surrogate_key(['anyo','inflacion_global']) }} as inflacion_id,
    anyo,
    inflacion_global,
    cambio_euro_dolar,
    cambio_dolar_euro

 from inf

 