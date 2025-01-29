BEGIN;
--запрос на удаление
SELECT client_rk, effective_from_date, COUNT(*)
FROM dm.client
GROUP BY client_rk, effective_from_date
HAVING COUNT(*) > 1;
DELETE FROM dm.client
--ctid в PostgreSQL — это внутренний столбец, который определяет физическое расположение данных таблицы на диске.
--значение ctid изменится при выполнении VACUUM FULL
WHERE ctid NOT IN (
    SELECT MIN(ctid)
    FROM dm.client
    GROUP BY client_rk, effective_from_date
);
SELECT * FROM dm.client;
COMMIT; -- или
ROLLBACK; --в случае необходимости


-- begin;

-- WITH dm.client AS (
--   SELECT *, ROW_NUMBER() OVER (PARTITION BY client_rk, effective_from_date) AS rn
--   FROM dm.client
-- )

-- DELETE FROM dm.client WHERE rn > 1;

-- rollback;
-- commit;