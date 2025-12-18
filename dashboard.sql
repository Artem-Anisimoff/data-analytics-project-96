-- общее число визитов
SELECT 
	COUNT(*) AS total_visits_count,
    COUNT(DISTINCT visitor_id) AS unique_visitors_count
FROM sessions;


-- уникальные визиты по источнику
SELECT 
    source AS utm_source,
    COUNT(DISTINCT visitor_id) AS unique_visitors
FROM sessions
GROUP BY source
ORDER BY unique_visitors DESC;


-- основные каналы 
SELECT 
    source AS utm_source,
    medium AS utm_medium,
    COUNT(DISTINCT visitor_id) AS unique_visitors,
    COUNT(*) AS total_visits,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS traffic_share_percent,
    ROUND(AVG(COUNT(*)) OVER (
        PARTITION BY source, medium
    ), 1) AS avg_daily_visits
FROM sessions
GROUP BY 
    source,
    medium
ORDER BY unique_visitors DESC;


-- каналы которые приводят на сайт по дням
SELECT 
    DATE(visit_date) AS visit_date,
    source AS utm_source,
    medium AS utm_medium,
    COUNT(DISTINCT visitor_id) AS unique_visitors,
    COUNT(*) AS total_visits
FROM sessions
WHERE visit_date >= '2023-06-01' 
  AND visit_date <= '2023-06-30'
GROUP BY 
    DATE(visit_date),
    source,
    medium
ORDER BY 
    visit_date DESC,
    total_visits DESC;


-- каналы которые приводят на сайт по неделям
SELECT 
    EXTRACT(WEEK FROM visit_date) AS week_number,
    DATE_TRUNC('week', visit_date)::DATE AS week_start,
    (DATE_TRUNC('week', visit_date) + INTERVAL '6 days')::DATE AS week_end,
    source AS utm_source,
    medium AS utm_medium,
    COUNT(DISTINCT visitor_id) AS unique_visitors,
    COUNT(*) AS total_visits
FROM sessions
WHERE visit_date >= '2023-06-01' 
  AND visit_date <= '2023-06-30'
GROUP BY 
    EXTRACT(WEEK FROM visit_date),
    DATE_TRUNC('week', visit_date),
    source,
    medium
ORDER BY 
    week_start DESC,
    total_visits DESC;


-- по месяцам
SELECT 
    EXTRACT(MONTH FROM visit_date) AS month_number,
    EXTRACT(YEAR FROM visit_date) AS year,
    TO_CHAR(visit_date, 'Month') AS month_name,
    source AS utm_source,
    medium AS utm_medium,
    COUNT(DISTINCT visitor_id) AS unique_visitors,
    COUNT(*) AS total_visits
FROM sessions
WHERE visit_date >= '2023-06-01' 
  AND visit_date <= '2023-06-30'
GROUP BY 
    EXTRACT(MONTH FROM visit_date),
    EXTRACT(YEAR FROM visit_date),
    TO_CHAR(visit_date, 'Month'),
    source,
    medium
ORDER BY 
    total_visits DESC;


--сколько лидов к нам приходит 
SELECT 
    COUNT(DISTINCT visitor_id) AS unique_lead_authors
FROM leads;


-- конверсия из клика в лид (общая)
WITH clicks AS (
    SELECT 
        COUNT(DISTINCT visitor_id) AS total_unique_visitors
    FROM sessions
),
leads_data AS (
    SELECT 
        COUNT(DISTINCT visitor_id) AS total_lead_authors
    FROM leads
)
SELECT 
    c.total_unique_visitors,
    l.total_lead_authors,
    ROUND(l.total_lead_authors * 100.0 / NULLIF(c.total_unique_visitors, 0), 2) AS click_to_lead_conversion_rate
FROM clicks c
CROSS JOIN leads_data l;


-- из лида в оплату (общая)
WITH all_leads AS (
    SELECT 
        COUNT(DISTINCT lead_id) AS total_leads
    FROM leads
),
purchases AS (
    SELECT 
        COUNT(DISTINCT lead_id) AS total_purchases
    FROM leads
    WHERE closing_reason = 'Успешно реализовано' OR status_id = 142
)
SELECT 
    a.total_leads,
    p.total_purchases,
    ROUND(p.total_purchases * 100.0 / NULLIF(a.total_leads, 0), 2) AS lead_to_purchase_conversion_rate
FROM all_leads a
CROSS JOIN purchases p;



-- затраты 
WITH all_campaigns AS (
    SELECT 
        DATE(campaign_date) AS spend_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS daily_spent,
        'vk' AS ad_platform
    FROM vk_ads
    GROUP BY 
        DATE(campaign_date),
        utm_source,
        utm_medium,
        utm_campaign
    
    UNION ALL
    
    SELECT 
        DATE(campaign_date) AS spend_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS daily_spent,
        'yandex' AS ad_platform
    FROM ya_ads
    GROUP BY 
        DATE(campaign_date),
        utm_source,
        utm_medium,
        utm_campaign
)
SELECT 
    spend_date,
    utm_source,
    utm_medium,
    utm_campaign,
    ad_platform,
    daily_spent
FROM all_campaigns
ORDER BY spend_date DESC, daily_spent DESC;


-- окупаемость + основные метрики и конверсии 
with last_touch as (
    select distinct on (sess.visitor_id)
        sess.visitor_id,
        sess.visit_date,
        sess.source as src,
        sess.medium as med,
        sess.campaign as cmp,
        ld.lead_id,
        ld.amount,
        ld.closing_reason
    from sessions as sess
    left join leads as ld
        on ld.visitor_id = sess.visitor_id
        and sess.visit_date <= ld.created_at
    where sess.medium <> 'organic'
    order by
        sess.visitor_id,
        sess.visit_date desc
),

ya_costs as (
    select
        utm_source as src,
        utm_medium as med,
        utm_campaign as cmp,
        to_char(campaign_date, 'YYYY-MM-DD') as day_key,
        sum(daily_spent) as cost
    from ya_ads
    group by
        utm_source,
        utm_medium,
        utm_campaign,
        to_char(campaign_date, 'YYYY-MM-DD')
),

vk_costs as (
    select
        utm_source as src,
        utm_medium as med,
        utm_campaign as cmp,
        to_char(campaign_date, 'YYYY-MM-DD') as day_key,
        sum(daily_spent) as cost
    from vk_ads
    group by
        utm_source,
        utm_medium,
        utm_campaign,
        to_char(campaign_date, 'YYYY-MM-DD')
),

utm_level as (
    select
        lt.src as utm_source,
        lt.med as utm_medium,
        lt.cmp as utm_campaign,
        count(distinct lt.visitor_id) as visitors_cnt,
        count(distinct lt.lead_id) as leads_cnt,
        sum(
            case
                when lt.closing_reason = 'Успешная продажа' then 1
                else 0
            end
        ) as purchases_cnt,
        sum(lt.amount) as revenue_amt,
        coalesce(yc.cost, vc.cost) as cost_amt
    from last_touch as lt
    left join ya_costs as yc
        on to_char(lt.visit_date, 'YYYY-MM-DD') = yc.day_key
        and lt.src = yc.src
        and lt.med = yc.med
        and lt.cmp = yc.cmp
    left join vk_costs as vc
        on to_char(lt.visit_date, 'YYYY-MM-DD') = vc.day_key
        and lt.src = vc.src
        and lt.med = vc.med
        and lt.cmp = vc.cmp
    group by
        lt.src,
        lt.med,
        lt.cmp,
        coalesce(yc.cost, vc.cost)
)

select
    utm_source,
    sum(visitors_cnt) as total_visitors,
    sum(cost_amt) as total_cost,
    sum(leads_cnt) as total_leads,
    round(
        sum(leads_cnt)::decimal * 100
        / sum(visitors_cnt)::decimal,
        2
    ) as users_to_leads_percent,
    round(
        sum(purchases_cnt)::decimal * 100
        / nullif(sum(leads_cnt), 0),
        2
    ) as leads_to_purchases_percent,
    sum(purchases_cnt) as total_purchases,
    sum(revenue_amt) as total_revenue,
    sum(revenue_amt) - sum(cost_amt) as total_profit,
    round(
        (sum(revenue_amt) - sum(cost_amt)) * 100
        / nullif(sum(cost_amt), 0),
        2
    ) as roi,
    round(
        sum(cost_amt)
        / nullif(sum(visitors_cnt), 0),
        2
    ) as cpu,
    round(
        sum(cost_amt)
        / nullif(sum(leads_cnt), 0),
        2
    ) as cpl,
    round(
        sum(cost_amt)
        / nullif(sum(purchases_cnt), 0),
        2
    ) as cppu
from utm_level
group by utm_source
having
    sum(purchases_cnt) > 0
    and sum(cost_amt) > 0
order by total_profit desc;



-- Прибыль по компаниям 
with t as (
    select distinct on (s.visitor_id)
        s.visitor_id,
        s.visit_date,
        s.source as utm_source,
        s.medium as utm_medium,
        s.campaign as utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id
    from sessions as s
    left join leads as l
        on s.visitor_id = l.visitor_id
        and s.visit_date <= l.created_at
    where s.medium != 'organic'
    order by
        s.visitor_id asc,
        s.visit_date desc
),

ya as (
    select
        utm_source,
        utm_medium,
        utm_campaign,
        to_char(campaign_date, 'YYYY-MM-DD') as campaign_date,
        sum(daily_spent) as summ
    from ya_ads
    group by
        to_char(campaign_date, 'YYYY-MM-DD'),
        utm_source,
        utm_medium,
        utm_campaign
),

vk as (
    select
        utm_source,
        utm_medium,
        utm_campaign,
        to_char(campaign_date, 'YYYY-MM-DD') as campaign_date,
        sum(daily_spent) as summ
    from vk_ads
    group by
        to_char(campaign_date, 'YYYY-MM-DD'),
        utm_source,
        utm_medium,
        utm_campaign
),

result as (
    select
        t.utm_source,
        t.utm_medium,
        t.utm_campaign,
        count(distinct t.visitor_id) as visitors_count,
        coalesce(y.summ, v.summ) as cost,
        count(distinct t.lead_id) as leads_count,
        round(
            cast(count(distinct t.lead_id) as decimal) * 100
            / cast(count(distinct t.visitor_id) as decimal),
            2
        ) as users_to_leads_percent,
        sum(
            case
                when t.closing_reason = 'Успешная продажа' then 1
                else 0
            end
        ) as purchases_count,
        case
            when sum(
                case
                    when t.closing_reason = 'Успешная продажа' then 1
                    else 0
                end
            ) = 0 then 0
            else round(
                cast(
                    sum(
                        case
                            when t.closing_reason = 'Успешная продажа' then 1
                            else 0
                        end
                    ) as decimal
                ) * 100
                / cast(count(distinct t.lead_id) as decimal),
                2
            )
        end as leads_to_purchases_percent,
        sum(t.amount) as revenue
    from t
    left join ya as y
        on to_char(t.visit_date, 'YYYY-MM-DD') = y.campaign_date
        and t.utm_source = y.utm_source
        and t.utm_medium = y.utm_medium
        and t.utm_campaign = y.utm_campaign
    left join vk as v
        on to_char(t.visit_date, 'YYYY-MM-DD') = v.campaign_date
        and t.utm_source = v.utm_source
        and t.utm_medium = v.utm_medium
        and t.utm_campaign = v.utm_campaign
    group by
        t.utm_source,
        t.utm_medium,
        t.utm_campaign,
        coalesce(y.summ, v.summ)
)

select
    utm_source,
    utm_medium,
    utm_campaign,
    sum(visitors_count) as total_visitors,
    sum(cost) as total_cost,
    sum(leads_count) as total_leads,
    round(
        cast(sum(leads_count) as decimal) * 100
        / cast(sum(visitors_count) as decimal),
        2
    ) as users_to_leads_percent,
    case
        when sum(purchases_count) = 0 then 0
        else round(
            cast(sum(purchases_count) as decimal) * 100
            / cast(sum(leads_count) as decimal),
            2
        )
    end as leads_to_purchases_percent,
    sum(purchases_count) as total_purchases,
    coalesce(sum(revenue), 0) as total_revenue,
    coalesce(sum(revenue), 0) - sum(cost) as total_profit
from result
group by
    utm_source,
    utm_medium,
    utm_campaign
having sum(cost) > 0
order by total_profit desc;


