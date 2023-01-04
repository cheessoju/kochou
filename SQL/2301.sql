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

#2023.1.4
#MAX()函数/MIN()函数特别作用：MAX('Diana',null,null)-->'Diana',MAX(null,null)-->null
#开窗
    SELECT
        MAX(CASE WHEN continent = 'America' THEN name ELSE null END) America,
        MAX(CASE WHEN continent = 'Asia' THEN name ELSE null END) Asia,
        MAX(CASE WHEN continent = 'Europe' THEN name ELSE null END) Europe
    FROM
        (SELECT 
            name, 
            continent, 
            ROW_NUMBER() OVER(PARTITION BY continent ORDER BY name) cur_rank
        FROM
            student)t 
    GROUP BY cur_rank

#变量法***
    SELECT 
    MAX(CASE continent WHEN 'America' THEN name ELSE null END) America,
    MAX(CASE continent WHEN 'Asia' THEN name ELSE null END) Asia,
    MAX(CASE continent WHEN 'Europe' THEN name ELSE null END) Europe
    FROM
    (SELECT name,continent,
    IF(@tmp=continent,@rownum:=@rownum+1,@rownum:=1) AS rnk,@tmp:=continent 
    FROM 
    student s1,(SELECT @tmp:=0,@rownum:=1) r1
    order by continent,name asc) t
    group by rnk
