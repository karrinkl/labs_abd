SELECT city,
       AVG(amount_eur)           AS avg_amount_eur,
       ROUND(AVG(amount_eur), 2) AS avg_amount_eur_rounded,
       COUNT(*)                  AS transaction_count
FROM transactions_clean
WHERE city IS NOT NULL
  AND city <> 'Unknown'
  AND amount_eur IS NOT NULL
  AND amount_eur > 0
GROUP BY city
ORDER BY AVG(amount_eur) DESC, city ASC   -- сортировка по точному, а не округлённому среднему
LIMIT 5;
