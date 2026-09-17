-- Curated-layer analytics examples

SELECT
    order_month,
    COUNT(*) AS orders,
    ROUND(SUM(net_amount), 2) AS revenue,
    ROUND(AVG(net_amount), 2) AS average_order_value
FROM curated_orders
WHERE status IN ('COMPLETED', 'SHIPPED')
GROUP BY order_month
ORDER BY order_month;

-- Customer ranking
SELECT
    customer_id,
    ROUND(SUM(net_amount), 2) AS lifetime_value,
    DENSE_RANK() OVER (ORDER BY SUM(net_amount) DESC) AS customer_rank
FROM curated_orders
GROUP BY customer_id;
