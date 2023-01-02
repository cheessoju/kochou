#January 2023 

#2023.1.1
#1951.一道卡住了没想出来的自连接
    WITH temp AS 
    (
        SELECT user1_id, user2_id, COUNT(*) AS k FROM
        (
            SELECT a.user_id AS user1_id, b.user_id AS user2_id, a.follower_id FROM Relations a
            JOIN Relations b ON a.follower_id = b.follower_id AND a.user_id<b.user_id
        ) AS temp
        GROUP BY user1_id, user2_id
    )

    SELECT user1_id, user2_id FROM temp
    WHERE k = (SELECT MAX(k) FROM temp)

#2023.1.2
#今天做不动了其实
#法一
    SELECT gender, day, 
    SUM(score_points) OVER (PARTITION BY gender ORDER BY day) total
FROM scores

#法二
#递归子集的非等值自连接法
    SELECT
        t1.gender,
        t1.day,
        (
            SELECT
                SUM(t2.score_points)
            FROM
                Scores AS t2
            WHERE t1.day >= t2.day
            AND t1.gender = t2.gender
        ) AS 'total'
    FROM
        Scores AS t1
    ORDER BY gender, day
