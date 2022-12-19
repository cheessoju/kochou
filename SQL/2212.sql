
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

