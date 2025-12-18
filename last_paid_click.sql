with paid_utm_sources as (
    select distinct
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content
    from (
        select
            utm_source,
            utm_medium,
            utm_campaign,
            utm_content
        from vk_ads
        where
            utm_medium in (
                'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
            )
        union
        select
            utm_source,
            utm_medium,
            utm_campaign,
            utm_content
        from ya_ads
        where
            utm_medium in (
                'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
            )
    ) as all_utms
),

last_paid_sessions as (
    select
        s.visitor_id,
        s.visit_date,
        s.source as utm_source,
        s.medium as utm_medium,
        s.campaign as utm_campaign,
        s.content as utm_content,
        row_number() over (
            partition by s.visitor_id
            order by s.visit_date desc
        ) as session_rn
    from sessions as s
    inner join paid_utm_sources as pus
        on
            s.source = pus.utm_source
            and s.medium = pus.utm_medium
            and s.campaign = pus.utm_campaign
    where s.medium in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
)

select
    lps.visitor_id,
    lps.visit_date,
    lps.utm_source,
    lps.utm_medium,
    lps.utm_campaign,
    lps.utm_content,
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.learning_format,
    l.status_id
from last_paid_sessions as lps
left join leads as l
    on
        lps.visitor_id = l.visitor_id
        and lps.visit_date <= l.created_at
where lps.session_rn = 1
order by
    case when l.amount is null then 1 else 0 end,
    l.amount desc nulls last,
    lps.visit_date asc,
    lps.utm_source asc,
    lps.utm_medium asc,
    lps.utm_campaign asc
limit 10;
