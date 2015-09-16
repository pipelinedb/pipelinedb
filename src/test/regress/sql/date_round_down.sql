-- nulls
SELECT date_round_down(NULL, NULL);
SELECT date_round_down(NULL, INTERVAL '5 minute');
SELECT date_round_down(TIMESTAMP '2001-02-16 20:38:40', NULL);

-- valid floors
SELECT date_round_down(TIMESTAMP '2001-02-16 20:38:40', INTERVAL '15 second');
SELECT date_round_down(TIMESTAMP '2001-02-16 20:38:40', INTERVAL '10 minute');
SELECT date_round_down(TIMESTAMP '2001-02-16 20:38:40', INTERVAL '3 hour');
SELECT date_round_down(TIMESTAMP '2001-02-18 20:38:40', INTERVAL '5 day');
SELECT date_round_down(TIMESTAMP '2001-02-16 20:38:40', INTERVAL '2 month');
SELECT date_round_down(TIMESTAMP '2001-02-16 20:38:40', INTERVAL '7 year');

-- invalid floors
SELECT date_round_down(TIMESTAMP '2001-02-16 20:38:40', INTERVAL '150 second');
SELECT date_round_down(TIMESTAMP '2001-02-16 20:38:40', INTERVAL '65 minute');
SELECT date_round_down(TIMESTAMP '2001-02-16 20:38:40', INTERVAL '48 hour');
SELECT date_round_down(TIMESTAMP '2001-02-16 20:38:40', INTERVAL '36 day');
SELECT date_round_down(TIMESTAMP '2001-02-16 20:38:40', INTERVAL '18 month');
