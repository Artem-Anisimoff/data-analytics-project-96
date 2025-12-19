with lpc_base as (
    select
        s.visitor_id,
        s.visit_date,
        s.source as utm_source,
        s.medium as utm_medium,
        s.campaign as utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        row_number() over (
            partition by s.visitor_id
            order by s.visit_date desc
        ) as visit_rank
    from sessions as s
    left join leads as l
        on
            s.visitor_id = l.visitor_id
            and s.visit_date <= l.created_at
    where s.medium in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

traffic_metrics as (
    select
        utm_source,
        utm_medium,
        utm_campaign,
        null::numeric as total_cost,
        date_trunc('day', visit_date) as visit_date,
        count(visitor_id) as visitors_count,
        count(lead_id) as leads_count,
        sum(
            case
                when
                    closing_reason = 'Успешная продажа'
                    or status_id = 142
                    then 1
                else 0
            end
        ) as purchases_count,
        sum(amount) as revenue
    from lpc_base
    where visit_rank = 1
    group by
        date_trunc('day', visit_date),
        utm_source,
        utm_medium,
        utm_campaign
),

ads_spend as (
    select
        campaign_date as visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        daily_spent as total_cost
    from vk_ads

    union all

    select
        campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        daily_spent
    from ya_ads
)

select
    utm_source,
    utm_medium,
    utm_campaign,
    to_char(visit_date, 'YYYY-MM-DD') as visit_date,
    sum(visitors_count) as visitors_count,
    sum(total_cost) as total_cost,
    sum(leads_count) as leads_count,
    sum(purchases_count) as purchases_count,
    sum(revenue) as revenue
from (
    select
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        visitors_count,
        leads_count,
        purchases_count,
        revenue,
        total_cost
    from traffic_metrics

    union all

    select
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        null as visitors_count,
        null as leads_count,
        null as purchases_count,
        null as revenue,
        total_cost
    from ads_spend
) as combined_data
group by
    visit_date,
    utm_source,
    utm_medium,
    utm_campaign
order by
    revenue desc nulls last,
    visit_date asc,
    visitors_count desc,
    utm_source asc,
    utm_medium asc,
    utm_campaign asc
limit 15;
