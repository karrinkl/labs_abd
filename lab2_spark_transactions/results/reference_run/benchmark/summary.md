Замер: 2026-10-19T18:31:15+00:00 UTC; повторов 5 (+1 прогревочный); время — медиана; результаты всех режимов совпадают: **True**.

Операция: read CSV -> normalize/validate -> deduplicate -> groupBy(city) avg/count -> orderBy(avg desc) -> collect; writes excluded; SparkSession start measured separately

| Набор | Строк | Режим | Конфигурация | Время, с (медиана) | min–max, с | Старт сессии, с | Партиций (вход / shuffle) | Speedup = T_local1 / T |
|---|---|---|---|---|---|---|---|---|
| transactions_test.csv | 10 100 | local1 | local[1], слотов задач 1, driver 1g | 1.447 | 1.230–1.772 | 4.4 | 1 / 8 | 1.0 |
| transactions_test.csv | 10 100 | local_all | local[*], слотов задач 4, driver 1g | 1.399 | 1.257–1.656 | 4.5 | 1 / 8 | 1.03 |
| transactions_test.csv | 10 100 | cluster | Standalone: Worker 1, executors 1, ядер 2, executor 1g, driver 1g | 1.753 | 1.367–1.981 | 6.4 | 1 / 8 | 0.83 |
| transactions_test.csv | 10 100 | cluster_1core | Standalone: Worker 1, executors 1, ядер 1, executor 1g, driver 1g | 1.692 | 1.472–2.176 | 6.4 | 1 / 8 | 0.85 |
| transactions_medium.csv | 505 000 | local1 | local[1], слотов задач 1, driver 1g | 9.155 | 8.823–9.303 | 4.4 | 2 / 8 | 1.0 |
| transactions_medium.csv | 505 000 | local_all | local[*], слотов задач 4, driver 1g | 3.745 | 3.558–5.110 | 4.5 | 4 / 8 | 2.44 |
| transactions_medium.csv | 505 000 | cluster | Standalone: Worker 1, executors 1, ядер 2, executor 1g, driver 1g | 6.217 | 5.601–6.557 | 7.1 | 2 / 8 | 1.47 |
| transactions_medium.csv | 505 000 | cluster_1core | Standalone: Worker 1, executors 1, ядер 1, executor 1g, driver 1g | 8.970 | 8.541–9.146 | 6.7 | 2 / 8 | 1.02 |
| transactions.csv | 2 020 000 | local1 | local[1], слотов задач 1, driver 1g | 32.984 | 32.409–35.284 | 4.7 | 6 / 8 | 1.0 |
| transactions.csv | 2 020 000 | local_all | local[*], слотов задач 4, driver 1g | 16.063 | 15.492–17.876 | 4.5 | 6 / 8 | 2.05 |
| transactions.csv | 2 020 000 | cluster | Standalone: Worker 1, executors 1, ядер 2, executor 1g, driver 1g | 19.187 | 18.738–20.520 | 6.9 | 6 / 8 | 1.72 |
| transactions.csv | 2 020 000 | cluster_1core | Standalone: Worker 1, executors 1, ядер 1, executor 1g, driver 1g | 33.685 | 33.261–34.022 | 7.5 | 6 / 8 | 0.98 |

| Набор | Режим | Время задач, с/прогон | CPU задач, с/прогон | Shuffle write, МБ/прогон | Задач/прогон | Пик heap executor, МБ | Пик RSS клиента, МБ | CPU клиента, с (весь процесс) |
|---|---|---|---|---|---|---|---|---|
| transactions_test.csv | local1 | 0.33 | 0.29 | 0.84 | 6 | 365.3 | 935.4 | 55.32 |
| transactions_test.csv | local_all | 0.29 | 0.27 | 0.84 | 6 | 247.3 | 1047.1 | 54.09 |
| transactions_test.csv | cluster | 0.36 | 0.32 | 0.84 | 6 | 203.1 | 753.6 | 43.27 |
| transactions_test.csv | cluster_1core | 0.38 | 0.3 | 0.84 | 6 | 158.4 | 688.7 | 44.53 |
| transactions_medium.csv | local1 | 7.79 | 7.46 | 43.65 | 7 | 544.4 | 1089.6 | 111.96 |
| transactions_medium.csv | local_all | 10.1 | 7.82 | 43.9 | 15 | 394.8 | 1245.0 | 121.53 |
| transactions_medium.csv | cluster | 8.36 | 7.59 | 43.73 | 9 | 364.0 | 698.3 | 46.58 |
| transactions_medium.csv | cluster_1core | 7.36 | 7.09 | 43.73 | 9 | 351.5 | 705.7 | 45.33 |
| transactions.csv | local1 | 31.68 | 30.41 | 177.24 | 13 | 626.4 | 1167.3 | 286.79 |
| transactions.csv | local_all | 50.49 | 42.99 | 177.44 | 17 | 934.0 | 1610.5 | 359.03 |
| transactions.csv | cluster | 31.55 | 29.57 | 177.22 | 14 | 802.7 | 752.9 | 48.99 |
| transactions.csv | cluster_1core | 31.62 | 30.25 | 177.24 | 14 | 451.4 | 717.2 | 52.1 |
