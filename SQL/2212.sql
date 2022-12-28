
#22.12.19
#196 法二：开窗函数
DELETE FROM Person
WHERE Id IN
    (
        SELECT Id
        from
            (
                SELECT Id,
                    row_number() OVER(partition by Email OVER by Id) rn
                FROM Person
            ) t1
        WHERE rn>1
    )

#22.12.20
#185
#法一：经典开窗*
#step 1
#第一个表 join 往结果上靠 出轮廓
    SELECT Employee.name,salary,Department.name Department
    FROM Employee JOIN Department
    ON Employee.departmentId=Department.id) f

#step 2
#第二表 进行分组排序 
    SELECT NAME,Department,salary,
    dense_rank() over(PARTITION BY department ORDER BY salary DESC) num
    FROM 
    (SELECT Employee.name,salary,Department.name Department
    FROM Employee JOIN Department
    ON Employee.departmentId=Department.id) f


#组合
    SELECT Department,k.name Employee , Salary 
    FROM
    (SELECT NAME,Department,salary,
    dense_rank() over(PARTITION BY department ORDER BY salary DESC) num
    FROM
    (SELECT Employee.name,salary,Department.name Department
    FROM Employee JOIN Department
    ON Employee.departmentId=Department.id) f) k
    WHERE k.num<=3

#法二：官解 但怪
    SELECT
     d.Name AS 'Department', e1.Name AS 'Employee', e1.Salary
    FROM
        Employee e1
            JOIN
        Department d ON e1.DepartmentId = d.Id
    WHERE
        3 > (SELECT
                COUNT(DISTINCT e2.Salary)
            FROM
                Employee e2
            WHERE
                e2.Salary > e1.Salary
                    AND e1.DepartmentId = e2.DepartmentId
            )

#22.12.21
#262.行程与用户
##①两种划分 ②SQL-IF函数

#法一nest:用户
    SELECT T.request_at AS `Day`, 
        ROUND(
                SUM(
                    IF(T.STATUS = 'completed',0,1)
                )
                / 
                COUNT(T.STATUS),
                2
        ) AS `Cancellation Rate`
    FROM Trips AS T
    JOIN Users AS U1 ON (T.client_id = U1.users_id AND U1.banned ='No')
    JOIN Users AS U2 ON (T.driver_id = U2.users_id AND U2.banned ='No')
    WHERE T.request_at BETWEEN '2013-10-01' AND '2013-10-03'
    GROUP BY T.request_at

#法二nest:行程
    SELECT *
    FROM trips AS T LEFT JOIN 
    (
        SELECT users_id
        FROM users
        WHERE banned = 'Yes'
    ) AS A ON (T.Client_Id = A.users_id)
    LEFT JOIN (
        SELECT users_id
        FROM users
        WHERE banned = 'Yes'
    ) AS A1
    ON (T.Driver_Id = A1.users_id)
    WHERE A.users_id IS NULL AND A1.users_id IS NULL

#***行程表的两次LEFT JOIN 两次很关键，如果直接剔掉banned可能导致A为空表，查询结果null
#其实和nest:用户的做法是一回事，注意driver和client在筛选逻辑上的并列
##错误写法：
    SELECT *
    FROM Trips AS T JOIN Users AS U 
    ON (T.client_id = U.users_id  OR T.driver_id = U.users_id )  AND U.banned ='No'

#22.12.22
#601
#检验连续性：开窗函数求差值***[通用]

    WITH s1 AS(
    SELECT id, visit_date,people,
            id-rank() over (order by id) rk_dis
            FROM stadium WHERE people>=100
    )
    SELECT id, visit_date,people from  s1
    WHERE rk_dis in (SELECT rk_dis from s1
    GROUP BY rk_dis HAVING COUNT(*)>=3)

#lead/Lag函数：但是没办法解决三天以上

##LEAD(列名，延续项数，Null默认替换值)
##LAG(列名，滞后项数，Null默认替换值)
    with people as
    (
        select id, visit_date, people,
        Lag(people,2) over(order by id) as pprvPeople,
        Lag(people,1) over(order by id) as prvPeople,
        Lead(people,1) over(order by id) as nextPeople,
        Lead(people,2) over(order by id) as nnextPeople
        from stadium
    )
        select id, visit_date, people from people
        where 
        (people >= 100 and prvPeople>=100 and pprvPeople>=100) ||
        (people >= 100 and nextPeople>=100 and nnextPeople>=100) ||
        (people >= 100 and nextPeople>=100 and prvPeople>=100) 

#22.12.23
#控制流/IF函数
#1393
##法一：单独讨论了只进不出的情况，在本题中更复杂但是可以拿来讨论衍生情况
    SELECT 
        stock_name
        ,SUM(IF(operation = 'Buy' and next_operation = 'UNKNOWN', 0, IF(operation = 'Buy', -price, price))) capital_gain_loss
    FROM (
        SELECT 
        stock_name
        ,operation
        ,price
        ,operation_day
        ,lead(operation, 1, 'UNKNOWN') OVER (PARTITION BY stock_name ORDER BY operation_day) next_operation
        from
        Stocks
    ) T
    GROUP BY stock_name;

#法二：快速写法
    SELECT stock_name, SUM(IF(operation != 'Buy',price,-price)) AS capital_gain_loss FROM Stocks GROUP BY stock_name

#22.12.23
#IFNULL
#1158
#cc-->
    SELECT Users.user_id AS buyer_id, join_date, IFNULL(buyrecords.k,0) AS orders_in_2019
    FROM Users LEFT JOIN (
        SELECT buyer_id, COUNT(order_id) k
        FROM Orders 
        WHERE order_date BETWEEN "2019-01-01" AND "2019-12-31"
        GROUP BY buyer_id 
                ) buyrecords
    ON Users.user_id = buyrecords.buyer_id
#优化-->
# YEAR()函数+CASE WHEN()免嵌套子查询
    select u.user_id as buyer_id, u.join_date,
    case when o.order_id is not null then count(*) else 0 end as orders_in_2019
    from users as u left outer join orders as o
    on u.user_id = o.buyer_id and year(o.order_date) = 2019
    group by u.user_id

#22.12.24
#找中位数：正序/倒叙
#本质是先找出最有可能是中位数的(2个)数求平均值
    SELECT AVG(number) median
    FROM
        (SELECT number,
            SUM(frequency) OVER(ORDER BY number) asc_accumu,
            SUM(frequency) OVER(ORDER BY number DESC) desc_accumu
            FROM numbers) t1, 
        (SELECT SUM(frequency) total from numbers) t2
    WHERE asc_accumu >= total/2 AND desc_accumu >=total/2
    
    
#22.12.25
#1699
#常规，不考虑通话接收发起方顺序
    SELECT person1, person2, COUNT(1) AS call_count, SUM(duration) AS total_duration
    FROM (SELECT 
        IF(from_id > to_id, to_id, from_id) AS person1, IF(to_id > from_id, from_id, to_id) AS person2, duration 
        FROM Calls ) callsReorder
    GROUP BY person1, person2 

#法二***
# LEAST()/GREATEST()交叉筛选-->并入相同分组
    SELECT
        from_id AS `person1`,
        to_id AS `person2`,
        COUNT(*) AS `call_count`,
        SUM(duration) AS `total_duration`
    FROM calls
    GROUP BY least(from_id, to_id), greatest(from_id, to_id)   
    
#22.12.26
#<1>-1445.🍎🍊弱智计算题，但是两种写法 IF&CASE WHEN
#IF
    SELECT sale_date, SUM(IF(fruit="oranges",-sold_num,sold_num)) AS diff
    FROM Sales
    GROUP BY sale_date
    ORDER BY sale_date

#CASE WHEN
    SELECT sale_date, SUM(CASE WHEN fruit="oranges" THEN -sold_num ELSE sold_num END) AS diff
    FROM Sales
    GROUP BY sale_date
    ORDER BY sale_date

#<2>-1193
#DATE_FORMAT()调整日期格式
#COUNT(*)和COUNT(1)都不计NULL
    SELECT DATE_FORMAT(trans_date, "%Y-%m") AS month,
        country,
        COUNT(*) AS trans_count,
        COUNT(IF(state = "approved",1,NULL)) AS approved_count,
        SUM(amount) AS trans_total_amount,
        SUM(IF(state = "approved",amount,0)) AS approved_total_amount
    FROM Transactions
    GROUP BY month,country

#DATE_FORMAT()
    %a  缩写星期名
    %b  缩写月名
    %c  月，数值
    %D  带有英文前缀的月中的天
    %d  月的天，数值(00-31)
    %e  月的天，数值(0-31)
    %f  微秒
    %H  小时 (00-23)
    %h  小时 (01-12)
    %I  小时 (01-12)
    %i  分钟，数值(00-59)
    %j  年的天 (001-366)
    %k  小时 (0-23)
    %l  小时 (1-12)
    %M  月名
    %m  月，数值(00-12)
    %p  AM 或 PM
    %r  时间，12-小时（hh:mm:ss AM 或 PM）
    %S  秒(00-59)
    %s  秒(00-59)
    %T  时间, 24-小时 (hh:mm:ss)
    %U  周 (00-53) 星期日是一周的第一天
    %u  周 (00-53) 星期一是一周的第一天
    %V  周 (01-53) 星期日是一周的第一天，与 %X 使用
    %v  周 (01-53) 星期一是一周的第一天，与 %x 使用
    %W  星期名
    %w  周的天 （0=星期日, 6=星期六）
    %X  年，其中的星期日是周的第一天，4 位，与 %V 使用
    %x  年，其中的星期一是周的第一天，4 位，与 %v 使用
    %Y  年，4 位
    %y  年，2 位

#22.12.27
#1127.用户购买平台
#UNION
#一点点复杂的SUM&IF()

    SELECT 
        t2.spend_date,
        t1.platform,
        sum(if(t1.platform = t2.platform, amount, 0)) total_amount,
        count(if(t1.platform = t2.platform, 1, NULL)) total_users
    FROM (
        SELECT 'mobile' platform union 
        SELECT 'desktop' platform union
        SELECT 'both' platform 
    ) t1, 
    (
        SELECT 
            user_id,
            spend_date, 
            any_value(if(count(platform) = 2, 'both', platform)) platform,
            sum(amount) amount
        FROM spending
        GROUP BY user_id, spend_date
    ) t2 
    GROUP BY t2.spend_date, t1.platform;
#22.12.28
#1501
#INSTR()+SUBSTR() *但是这题里用反而变复杂捏
    WHERE SUBSTR(P.phone_number,0,INSTR(P.phone_number,'-') - 1) = C.country_code 
#LEFT()函数LEFT(string, num)
#本题：UNION ALL 区分UNION(不重复)
    WITH people_country AS
    (
        SELECT id, c.name country
        FROM Person p LEFT JOIN Country c
        on LEFT(p.phone_number,3) = c.country_code
    )

    SELECT country
    FROM
    (
        SELECT country, AVG(duration) avgtime
        FROM
        (
            SELECT caller_id id, duration
            FROM Calls
            UNION ALL
            SELECT callee_id, duration
            FROM Calls
        ) t LEFT JOIN people_country
        USING (id) /* USING()简略用法*/
        GROUP BY country
    ) temp
    WHERE avgtime > 
        (
            SELECT AVG(duration) avgtime
            FROM
            (
                SELECT caller_id, duration
                FROM Calls
                UNION ALL
                SELECT callee_id, duration
                FROM Calls
            ) t
        )
