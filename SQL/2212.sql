
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

