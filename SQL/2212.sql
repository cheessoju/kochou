
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


