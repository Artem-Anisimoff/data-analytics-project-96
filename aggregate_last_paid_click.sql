-- Создаем CTE для определения последнего платного клика для каждого пользователя
WITH last_paid_click AS (
    SELECT 
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium, 
        s.campaign AS utm_campaign,
        ROW_NUMBER() OVER (
            PARTITION BY s.visitor_id 
            ORDER BY s.visit_date DESC
        ) AS rn
    FROM sessions s
    WHERE s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
      AND s.medium != 'organic'
),

-- CTE для атрибутированных сессий (только последние платные клики)
attributed_sessions AS (
    SELECT 
        visitor_id,
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
    FROM last_paid_click
    WHERE rn = 1
),

-- CTE для агрегации визитов по дням и UTM-меткам
daily_visitors AS (
    SELECT 
        DATE(visit_date) AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(visitor_id) AS visitors_count
    FROM attributed_sessions
    GROUP BY 
        DATE(visit_date),
        utm_source,
        utm_medium,
        utm_campaign
),

-- CTE для расчета расходов из VK Ads
vk_costs AS (
    SELECT 
        DATE(campaign_date) AS cost_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS daily_cost
    FROM vk_ads
    GROUP BY 
        DATE(campaign_date),
        utm_source,
        utm_medium,
        utm_campaign
),

-- CTE для расчета расходов из Yandex Ads
ya_costs AS (
    SELECT 
        DATE(campaign_date) AS cost_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS daily_cost
    FROM ya_ads
    GROUP BY 
        DATE(campaign_date),
        utm_source,
        utm_medium,
        utm_campaign
),

-- CTE для объединения расходов из всех источников
total_costs AS (
    SELECT 
        cost_date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        daily_cost AS total_cost
    FROM vk_costs
    
    UNION ALL
    
    SELECT 
        cost_date AS visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        daily_cost AS total_cost
    FROM ya_costs
),

-- CTE для агрегации лидов и покупок
leads_aggregation AS (
    SELECT 
        DATE(a.visit_date) AS visit_date,
        a.utm_source,
        a.utm_medium,
        a.utm_campaign,
        COUNT(l.lead_id) AS leads_count,
        COUNT(CASE 
                WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 
                THEN l.lead_id 
            END) AS purchases_count,
        SUM(CASE 
                WHEN l.closing_reason = 'Успешно реализовано' OR l.status_id = 142 
                THEN l.amount 
            END) AS revenue
    FROM attributed_sessions a
    LEFT JOIN leads l ON a.visitor_id = l.visitor_id 
        AND l.created_at >= a.visit_date
    GROUP BY 
        DATE(a.visit_date),
        a.utm_source,
        a.utm_medium,
        a.utm_campaign
)

-- объединяем все метрики
SELECT 
    dv.visit_date,
    dv.visitors_count,
    dv.utm_source,
    dv.utm_medium,
    dv.utm_campaign,
    COALESCE(tc.total_cost, 0) AS total_cost,  -- Если расходов нет, считаем 0
    la.leads_count,
    la.purchases_count,
    la.revenue
FROM daily_visitors dv
LEFT JOIN total_costs tc ON dv.visit_date = tc.visit_date 
    AND dv.utm_source = tc.utm_source
    AND dv.utm_medium = tc.utm_medium
    AND dv.utm_campaign = tc.utm_campaign
LEFT JOIN leads_aggregation la ON dv.visit_date = la.visit_date 
    AND dv.utm_source = la.utm_source
    AND dv.utm_medium = la.utm_medium
    AND dv.utm_campaign = la.utm_campaign

ORDER BY 
    dv.visit_date ASC,              
    dv.visitors_count DESC,          -
    dv.utm_source ASC,              
    dv.utm_medium ASC,              
    dv.utm_campaign ASC,            
    la.revenue DESC NULLS LAST      

LIMIT 15;  