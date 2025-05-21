-- models/precio_ajustado_productos.sql

{{ calcular_precio_ajustado(
    'fct_ventas', 
    'dim_inflacion',                 
    'videojuego',               
    'precio_euros',               
    'fecha', 
    'lanzamiento',                     
    'inflacion_global'                     
) }}
