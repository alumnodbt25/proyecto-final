-- models/precio_ajustado_productos.sql

{{ calcular_precio_ajustado(
    'dim_consolas', 
    'dim_inflacion',                 
    'consola',               
    'precio_na',               
    'fecha', 
    'lanzamiento',                     
    'inflacion_global'                     
) }}

