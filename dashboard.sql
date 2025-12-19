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
    source AS utm_source,
    medium AS utm_medium,
    DATE(visit_date) AS visit_date,
    COUNT(DISTINCT visitor_id) AS unique_visitors,
    COUNT(*) AS total_visits
FROM sessions
GROUP BY
    DATE(visit_date),
    source,
    medium
ORDER BY
    visit_date DESC,
    total_visits DESC;


-- каналы которые приводят на сайт по неделям
SELECT
    DATE_TRUNC('week', visit_date)::DATE AS week_start,
    (DATE_TRUNC('week', visit_date) + INTERVAL '6 days')::DATE AS week_end,
    source AS utm_source,
    medium AS utm_medium,
    EXTRACT(WEEK FROM visit_date) AS week_number,
    COUNT(DISTINCT visitor_id) AS unique_visitors,
    COUNT(*) AS total_visits
FROM sessions
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
    source AS utm_source,
    medium AS utm_medium,
    EXTRACT(MONTH FROM visit_date) AS month_number,
    EXTRACT(YEAR FROM visit_date) AS y3ar,
    TO_CHAR(visit_date, 'Month') AS month_name,
    COUNT(DISTINCT visitor_id) AS unique_visitors,
    COUNT(*) AS total_visits
FROM sessions
GROUP BY
    EXTRACT(MONTH FROM visit_date),
    EXTRACT(YEAR FROM visit_date),
    TO_CHAR(visit_date, 'Month'),
    source,
    medium
ORDER BY
    total_visits DESC;


--сколько лидов к нам приходит 
SELECT COUNT(DISTINCT visitor_id) AS unique_lead_authors
FROM leads;


-- конверсия из клика в лид (общая)
WITH clicks AS (
    SELECT COUNT(DISTINCT visitor_id) AS total_unique_visitors
    FROM sessions
),

leads_data AS (
    SELECT COUNT(DISTINCT visitor_id) AS total_lead_authors
    FROM leads
)

SELECT
    c.total_unique_visitors,
    l.total_lead_authors,
    ROUND(
        l.total_lead_authors * 100.0 / NULLIF(c.total_unique_visitors, 0), 2
    ) AS click_to_lead_conversion_rate
FROM clicks AS c
CROSS JOIN leads_data AS l;


-- из лида в оплату (общая)
WITH all_leads AS (
    SELECT COUNT(DISTINCT lead_id) AS total_leads
    FROM leads
),

purchases AS (
    SELECT COUNT(DISTINCT lead_id) AS total_purchases
    FROM leads
    WHERE closing_reason = 'Успешно реализовано' OR status_id = 142
)

SELECT
    a.total_leads,
    p.total_purchases,
    ROUND(p.total_purchases * 100.0 / NULLIF(a.total_leads, 0), 2)
        AS lead_to_purchase_conversion_rate
FROM all_leads AS a
CROSS JOIN purchases AS p;


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
WITH last_touch AS (
    SELECT DISTINCT ON (sess.visitor_id)
        sess.visitor_id,
        sess.visit_date,
        sess.source AS src,
        sess.medium AS med,
        sess.campaign AS cmp,
        ld.lead_id,
        ld.amount,
        ld.closing_reason
    FROM sessions AS sess
    LEFT JOIN leads AS ld
        ON
            sess.visitor_id = ld.visitor_id
            AND sess.visit_date <= ld.created_at
    WHERE sess.medium <> 'organic'
    ORDER BY
        sess.visitor_id ASC,
        sess.visit_date DESC
),

ya_costs AS (
    SELECT
        utm_source AS src,
        utm_medium AS med,
        utm_campaign AS cmp,
        TO_CHAR(campaign_date, 'YYYY-MM-DD') AS day_key,
        SUM(daily_spent) AS co5t
    FROM ya_ads
    GROUP BY
        utm_source,
        utm_medium,
        utm_campaign,
        TO_CHAR(campaign_date, 'YYYY-MM-DD')
),

vk_costs AS (
    SELECT
        utm_source AS src,
        utm_medium AS med,
        utm_campaign AS cmp,
        TO_CHAR(campaign_date, 'YYYY-MM-DD') AS day_key,
        SUM(daily_spent) AS co5t
    FROM vk_ads
    GROUP BY
        utm_source,
        utm_medium,
        utm_campaign,
        TO_CHAR(campaign_date, 'YYYY-MM-DD')
),

utm_level AS (
    SELECT
        lt.src AS utm_source,
        lt.med AS utm_medium,
        lt.cmp AS utm_campaign,
        COUNT(DISTINCT lt.visitor_id) AS visitors_cnt,
        COUNT(DISTINCT lt.lead_id) AS leads_cnt,
        SUM(
            CASE
                WHEN lt.closing_reason = 'Успешная продажа' THEN 1
                ELSE 0
            END
        ) AS purchases_cnt,
        SUM(lt.amount) AS revenue_amt,
        COALESCE(yc.cost, vc.cost) AS cost_amt
    FROM last_touch AS lt
    LEFT JOIN ya_costs AS yc
        ON
            TO_CHAR(lt.visit_date, 'YYYY-MM-DD') = yc.day_key
            AND lt.src = yc.src
            AND lt.med = yc.med
            AND lt.cmp = yc.cmp
    LEFT JOIN vk_costs AS vc
        ON
            TO_CHAR(lt.visit_date, 'YYYY-MM-DD') = vc.day_key
            AND lt.src = vc.src
            AND lt.med = vc.med
            AND lt.cmp = vc.cmp
    GROUP BY
        lt.src,
        lt.med,
        lt.cmp,
        COALESCE(yc.cost, vc.cost)
)

SELECT
    utm_source,
    SUM(visitors_cnt) AS total_visitors,
    SUM(cost_amt) AS total_cost,
    SUM(leads_cnt) AS total_leads,
    ROUND(
        SUM(leads_cnt)::DECIMAL * 100
        / SUM(visitors_cnt)::DECIMAL,
        2
    ) AS users_to_leads_percent,
    ROUND(
        SUM(purchases_cnt)::DECIMAL * 100
        / NULLIF(SUM(leads_cnt), 0),
        2
    ) AS leads_to_purchases_percent,
    SUM(purchases_cnt) AS total_purchases,
    SUM(revenue_amt) AS total_revenue,
    SUM(revenue_amt) - SUM(cost_amt) AS total_profit,
    ROUND(
        (SUM(revenue_amt) - SUM(cost_amt)) * 100
        / NULLIF(SUM(cost_amt), 0),
        2
    ) AS roi,
    ROUND(
        SUM(cost_amt)
        / NULLIF(SUM(visitors_cnt), 0),
        2
    ) AS cpu,
    ROUND(
        SUM(cost_amt)
        / NULLIF(SUM(leads_cnt), 0),
        2
    ) AS cpl,
    ROUND(
        SUM(cost_amt)
        / NULLIF(SUM(purchases_cnt), 0),
        2
    ) AS cppu
FROM utm_level
GROUP BY utm_source
HAVING
    SUM(purchases_cnt) > 0
    AND SUM(cost_amt) > 0
ORDER BY total_profit DESC;


-- Прибыль по компаниям 
WITH t AS (
    SELECT DISTINCT ON (s.visitor_id)
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id
    FROM sessions AS s
    LEFT JOIN leads AS l
        ON
            s.visitor_id = l.visitor_id
            AND s.visit_date <= l.created_at
    WHERE s.medium <> 'organic'
    ORDER BY
        s.visitor_id ASC,
        s.visit_date DESC
),

ya AS (
    SELECT
        utm_source,
        utm_medium,
        utm_campaign,
        TO_CHAR(campaign_date, 'YYYY-MM-DD') AS campaign_date,
        SUM(daily_spent) AS summ
    FROM ya_ads
    GROUP BY
        TO_CHAR(campaign_date, 'YYYY-MM-DD'),
        utm_source,
        utm_medium,
        utm_campaign
),

vk AS (
    SELECT
        utm_source,
        utm_medium,
        utm_campaign,
        TO_CHAR(campaign_date, 'YYYY-MM-DD') AS campaign_date,
        SUM(daily_spent) AS summ
    FROM vk_ads
    GROUP BY
        TO_CHAR(campaign_date, 'YYYY-MM-DD'),
        utm_source,
        utm_medium,
        utm_campaign
),

result AS (
    SELECT
        t.utm_source,
        t.utm_medium,
        t.utm_campaign,
        COUNT(DISTINCT t.visitor_id) AS visitors_count,
        COALESCE(y.summ, v.summ) AS co5t,
        COUNT(DISTINCT t.lead_id) AS leads_count,
        ROUND(
            (COUNT(DISTINCT t.lead_id))::DECIMAL * 100
            / (COUNT(DISTINCT t.visitor_id))::DECIMAL,
            2
        ) AS users_to_leads_percent,
        SUM(
            CASE
                WHEN t.closing_reason = 'Успешная продажа' THEN 1
                ELSE 0
            END
        ) AS purchases_count,
        CASE
            WHEN SUM(
                CASE
                    WHEN t.closing_reason = 'Успешная продажа' THEN 1
                    ELSE 0
                END
            ) = 0 THEN 0
            ELSE ROUND(
                (SUM(
                    CASE
                        WHEN t.closing_reason = 'Успешная продажа' THEN 1
                        ELSE 0
                    END
                ))::DECIMAL * 100
                / (COUNT(DISTINCT t.lead_id))::DECIMAL,
                2
            )
        END AS leads_to_purchases_percent,
        SUM(t.amount) AS revenue
    FROM t
    LEFT JOIN ya AS y
        ON
            TO_CHAR(t.visit_date, 'YYYY-MM-DD') = y.campaign_date
            AND t.utm_source = y.utm_source
            AND t.utm_medium = y.utm_medium
            AND t.utm_campaign = y.utm_campaign
    LEFT JOIN vk AS v
        ON
            TO_CHAR(t.visit_date, 'YYYY-MM-DD') = v.campaign_date
            AND t.utm_source = v.utm_source
            AND t.utm_medium = v.utm_medium
            AND t.utm_campaign = v.utm_campaign
    GROUP BY
        t.utm_source,
        t.utm_medium,
        t.utm_campaign,
        COALESCE(y.summ, v.summ)
)

SELECT
    utm_source,
    utm_medium,
    utm_campaign,
    SUM(visitors_count) AS total_visitors,
    SUM(cost) AS total_cost,
    SUM(leads_count) AS total_leads,
    ROUND(
        (SUM(leads_count))::DECIMAL * 100
        / (SUM(visitors_count))::DECIMAL,
        2
    ) AS users_to_leads_percent,
    CASE
        WHEN SUM(purchases_count) = 0 THEN 0
        ELSE ROUND(
            (SUM(purchases_count))::DECIMAL * 100
            / (SUM(leads_count))::DECIMAL,
            2
        )
    END AS leads_to_purchases_percent,
    SUM(purchases_count) AS total_purchases,
    COALESCE(SUM(revenue), 0) AS total_revenue,
    COALESCE(SUM(revenue), 0) - SUM(cost) AS total_profit
FROM result
GROUP BY
    utm_source,
    utm_medium,
    utm_campaign
HAVING SUM(cost) > 0
ORDER BY total_profit DESC;
