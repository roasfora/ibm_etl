with base as (

    select
        empresa_id,
        to_date(tempo_id::varchar, 'YYYYMMDD') as data,
        preco_fechamento
   from {{ source('raw', 'fact_cotacoes') }}

),

agregado as (

    select
        empresa_id,
        date_trunc('month', data) as mes,
        round(avg(preco_fechamento)::numeric, 2) as preco_medio_fechamento
    from base
    group by empresa_id, mes

)

select * from agregado
