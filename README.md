# User-Activity-and-Account-Dynamics-Analysis
Collect data on account creation, email interactions (send, open, click), and user behavior (send intervals, verification, subscription status) to compare activity across countries, identify key markets, and segment users.
WITH account_data AS (
    SELECT
        s.date,
        sp.country,
        acc.send_interval,
        acc.is_verified,
        acc.is_unsubscribed,
        COUNT(DISTINCT acc.id) as account_cnt,
        0 as sent_msg,
        0 as open_msg,
        0 as visit_msg
    FROM `DA.session_params` sp
    JOIN `DA.account_session` acs
        ON acs.ga_session_id = sp.ga_session_id
    JOIN `DA.account` acc
        ON acs.account_id = acc.id
    JOIN `DA.session` s
        ON acs.ga_session_id = s.ga_session_id
    GROUP BY 1, 2, 3, 4, 5
),


mail_metrics AS (
    SELECT
        DATE_ADD(s.date, INTERVAL es.sent_date DAY) as date,
        sp.country,
        acc.send_interval,
        acc.is_verified,
        acc.is_unsubscribed,
        0 as account_cnt,
        COUNT(DISTINCT es.id_message) as sent_msg,
        COUNT(DISTINCT eo.id_message) as open_msg,
        COUNT(DISTINCT ev.id_message) as visit_msg
    FROM `DA.email_sent` es
    LEFT JOIN `DA.email_open` eo
        ON es.id_message = eo.id_message
    LEFT JOIN `DA.email_visit` ev
        ON es.id_message = ev.id_message
    JOIN `DA.account_session` acs
        ON es.id_account = acs.account_id
    JOIN `DA.session_params` sp
        ON acs.ga_session_id = sp.ga_session_id
    JOIN `DA.account` acc
        ON acs.account_id = acc.id
    JOIN `DA.session` s
        ON sp.ga_session_id = s.ga_session_id
    GROUP BY 1, 2, 3, 4, 5
),


unionall AS (
    SELECT *
    FROM account_data
    UNION ALL
    SELECT *
    FROM mail_metrics
),


country_totals AS (
    SELECT
        country,
        SUM(account_cnt) as total_country_account_cnt,
        SUM(sent_msg) as total_country_sent_cnt
    FROM unionall
    GROUP BY country
),


ranking AS (
    SELECT
        u.date,
        u.country,
        u.send_interval,
        u.is_verified,
        u.is_unsubscribed,
        SUM(u.account_cnt) as account_cnt,
        SUM(u.sent_msg) as sent_msg,
        SUM(u.open_msg) as open_msg,
        SUM(u.visit_msg) as visit_msg,
        MAX(ct.total_country_account_cnt) as total_country_account_cnt,
        MAX(ct.total_country_sent_cnt) as total_country_sent_cnt,
        DENSE_RANK() OVER (ORDER BY MAX(ct.total_country_account_cnt) DESC) as total_country_acc_cnt,
        DENSE_RANK() OVER (ORDER BY MAX(ct.total_country_sent_cnt) DESC) as total_cntry_sent_cnt
    FROM unionall u
    JOIN country_totals ct
        ON u.country = ct.country
    GROUP BY 1, 2, 3, 4, 5
)


SELECT *
FROM ranking
WHERE total_country_acc_cnt <= 10 OR total_cntry_sent_cnt <= 10
ORDER BY country, date, total_country_acc_cnt, total_cntry_sent_cnt;
