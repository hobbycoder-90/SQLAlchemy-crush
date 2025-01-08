from sqlalchemy import insert, text, select, update, and_, func, Integer
from database import sync_engine, async_engine
from models import metadata_obj, workers_table, resumes_table, WorkloadsEnum
from sqlalchemy.orm import aliased

        

class SyncCore:
    @staticmethod
    def create_tables_sync_core():
        sync_engine.echo = True
        metadata_obj.drop_all(sync_engine)
        metadata_obj.create_all(sync_engine)
        sync_engine.echo = True

    @staticmethod
    def insert_workers_sync_core():
        with sync_engine.connect() as conn:
            workerss = insert(workers_table).values(
                [
                    {"username": "Fatih Sultan Mehmet"},
                    {"username": "Selahaattin Eyyubi"},
                ]
            )
            conn.execute(workerss)
            conn.commit()
    
    @staticmethod
    def select_workers_sync_core():
        with sync_engine.connect() as conn:
            query = select(workers_table)
            result = conn.execute(query)
            workers = result.all()
            print(f"{workers=}")
    
    @staticmethod
    def update_workers_sync_core(worker_id: int = 1, new_username: str = 'Mustafa Kemal Ataturk'):
        with sync_engine.connect() as conn:
            #stmt = text(""" UPDATE workers SET username=:username WHERE id=:id """)
            #stmt = stmt.bindparams(username=new_username, id=worker_id)
            stmt = (
                update(workers_table)
                .values(username=new_username)
                .filter_by(id=worker_id)
            )
            conn.execute(stmt)
            conn.commit()
    
    @staticmethod
    def insert_resumes_sync_core():
        with sync_engine.connect() as conn:
            resumes = [
                {"title": "Sultan", "compensation": 50000, "workload": WorkloadsEnum.full_time, "worker_id": 1},
                {"title": "Sultanlar sultani", "compensation": 150000, "workload": WorkloadsEnum.full_time, "worker_id": 1},
                {"title": "Sultan Eyyubi", "compensation": 250000, "workload": WorkloadsEnum.part_time, "worker_id": 2},
                {"title": "Eyyubi", "compensation": 300000, "workload": WorkloadsEnum.full_time, "worker_id": 2},
            ]
            stmt = insert(resumes_table).values(resumes)
            conn.execute(stmt)
            conn.commit()

    @staticmethod
    def select_resumes_avg_compensation_sync_core(like_title: str = "Sultan"):
        """
        select workload, avg(compensation)::int as avg_compensation
        from resumes
        where title like '%Python%' and compensation > 40000
        group by workload
        having avg(compensation) > 70000
        """
        with sync_engine.connect() as conn:
            query = (
                select(
                    resumes_table.c.workload,
                    # 1 вариант использования cast
                    # cast(func.avg(ResumesOrm.compensation), Integer).label("avg_compensation"),
                    # 2 вариант использования cast (предпочтительный способ)
                    func.avg(resumes_table.c.compensation).cast(Integer).label("avg_compensation"),
                )
                .select_from(resumes_table)
                .filter(and_(
                    resumes_table.c.title.contains(like_title),
                    resumes_table.c.compensation > 40000,
                ))
                .group_by(resumes_table.c.workload)
                .having(func.avg(resumes_table.c.compensation) > 70000)
            )
            print(query.compile(compile_kwargs={"literal_binds": True}))
            res = conn.execute(query)
            result = res.unique().all()
            print(result[0].avg_compensation)
            return result
    
    @staticmethod
    def insert_additional_resumes_sync_core():
        with sync_engine.connect() as conn:
            workers = [
                {"username": "Yavuz Sultan Selim"},
                {"username": "Kanuni Sultan Suleyman"},
                {"username": "Sultan Abdulhamit"},
            ]
            resumes = [
                {"title": "Sultan Padisah", "compensation": 60000, "workload": "full_time", "worker_id": 3},
                {"title": "Padisah", "compensation": 70000, "workload": "part_time", "worker_id": 3},
                {"title": "Sultan-i Devle", "compensation": 80000, "workload": "part_time", "worker_id": 4},
                {"title": "Sultan-i Evvel", "compensation": 90000, "workload": "full_time", "worker_id": 4},
                {"title": "Sultan-i Sani", "compensation": 100000, "workload": "full_time", "worker_id": 5},
            ]
            insert_workers = insert(workers_table).values(workers)
            insert_resumes = insert(resumes_table).values(resumes)
            conn.execute(insert_workers)
            conn.execute(insert_resumes)
            conn.commit()

    @staticmethod
    def join_cte_subquery_window_func_sync_core():
        """
        WITH helper2 AS (
            SELECT *, compensation-avg_workload_compensation AS compensation_diff
            FROM 
            (SELECT
                w.id,
                w.username,
                r.compensation,
                r.workload,
                avg(r.compensation) OVER (PARTITION BY workload)::int AS avg_workload_compensation
            FROM resumes r
            JOIN workers w ON r.worker_id = w.id) helper1
        )
        SELECT * FROM helper2
        ORDER BY compensation_diff DESC;
        """
        with sync_engine.connect() as conn:
            r = aliased(resumes_table)
            w = aliased(workers_table)
            subq = (
                select(
                    r,
                    w,
                    func.avg(r.c.compensation).over(partition_by=r.c.workload).cast(Integer).label("avg_workload_compensation"),
                )
                # .select_from(r)
                .join(r, r.c.worker_id == w.c.id).subquery("helper1")
            )
            cte = (
                select(
                    subq.c.worker_id,
                    subq.c.username,
                    subq.c.compensation,
                    subq.c.workload,
                    subq.c.avg_workload_compensation,
                    (subq.c.compensation - subq.c.avg_workload_compensation).label("compensation_diff"),
                )
                .cte("helper2")
            )
            query = (
                select(cte)
                .order_by(cte.c.compensation_diff.desc())
            )

            res = conn.execute(query)
            result = res.all()
            print(f"{len(result)=}. {result=}")
            
    # Relationships отсутствуют при использовании Table



class AsyncCore:
    @staticmethod
    async def create_tables_async_core():
        async with async_engine.begin() as conn:
            await conn.run_sync(metadata_obj.drop_all)
            await conn.run_sync(metadata_obj.create_all)

    @staticmethod
    async def insert_workers_async_core():
        async with async_engine.connect() as conn:
            workerss = insert(workers_table).values(
                [
                    {"username": "Fatih Sultan Mehmet"},
                    {"username": "Selahaattin Eyyubi"},
                ]
            )
            await conn.execute(workerss)
            await conn.commit()

    @staticmethod
    async def select_workers_async_core():
        async with async_engine.connect() as conn:
            query = select(workers_table)
            result = await conn.execute(query)
            workers = result.all()
            print(f"{workers=}")

    @staticmethod
    async def update_workers_async_core(worker_id: int = 2, new_username: str = 'Yavuz Sultan Selim'):
        async with async_engine.connect() as conn:
            stmt = (
                update(workers_table)
                .values(username=new_username)
                .filter_by(id = worker_id)
            )
            await conn.execute(stmt)
            await conn.commit()
    
    @staticmethod
    async def insert_resumes_async_core():
        async with async_engine.connect() as conn:
            resumes = [
                {"title": "Sultan", "compensation": 50000, "workload": WorkloadsEnum.full_time, "worker_id": 1},
                {"title": "Sultanlar sultani", "compensation": 150000, "workload": WorkloadsEnum.full_time, "worker_id": 1},
                {"title": "Sultan Eyyubi", "compensation": 250000, "workload": WorkloadsEnum.part_time, "worker_id": 2},
                {"title": "Eyyubi", "compensation": 300000, "workload": WorkloadsEnum.full_time, "worker_id": 2},
            ]
            stmt = insert(resumes_table).values(resumes)
            await conn.execute(stmt)
            await conn.commit()

    @staticmethod
    async def select_resumes_avg_compensation_async_core(like_title: str = "Sultan"):
        """
        select workload, avg(compensation)::int as avg_compensation
        from resumes
        where title like '%Python%' and compensation > 40000
        group by workload
        having avg(compensation) > 70000
        """
        async with async_engine.connect() as conn:
            query = (
                select(
                    resumes_table.c.workload,
                    # 1 вариант использования cast
                    # cast(func.avg(ResumesOrm.compensation), Integer).label("avg_compensation"),
                    # 2 вариант использования cast (предпочтительный способ)
                    func.avg(resumes_table.c.compensation).cast(Integer).label("avg_compensation"),
                )
                .select_from(resumes_table)
                .filter(and_(
                    resumes_table.c.title.contains(like_title),
                    resumes_table.c.compensation > 40000,
                ))
                .group_by(resumes_table.c.workload)
                .having(func.avg(resumes_table.c.compensation) > 70000)
            )
            print(query.compile(compile_kwargs={"literal_binds": True}))
            res = await conn.execute(query)
            result = res.all()
            print(result[0].avg_compensation)

    @staticmethod
    async def insert_additional_resumes_async_core():
        async with async_engine.connect() as conn:
            workers = [
                {"username": "Yavuz Sultan Selim"},
                {"username": "Kanuni Sultan Suleyman"},
                {"username": "Sultan Abdulhamit"},
            ]
            resumes = [
                {"title": "Sultan Padisah", "compensation": 60000, "workload": "full_time", "worker_id": 3},
                {"title": "Padisah", "compensation": 70000, "workload": "part_time", "worker_id": 3},
                {"title": "Sultan-i Devle", "compensation": 80000, "workload": "part_time", "worker_id": 4},
                {"title": "Sultan-i Evvel", "compensation": 90000, "workload": "full_time", "worker_id": 4},
                {"title": "Sultan-i Sani", "compensation": 100000, "workload": "full_time", "worker_id": 5},
            ]
            insert_workers = insert(workers_table).values(workers)
            insert_resumes = insert(resumes_table).values(resumes)
            await conn.execute(insert_workers)
            await conn.execute(insert_resumes)
            await conn.commit()

    @staticmethod
    async def join_cte_subquery_window_func_async_core():
        """
        WITH helper2 AS (
            SELECT *, compensation-avg_workload_compensation AS compensation_diff
            FROM 
            (SELECT
                w.id,
                w.username,
                r.compensation,
                r.workload,
                avg(r.compensation) OVER (PARTITION BY workload)::int AS avg_workload_compensation
            FROM resumes r
            JOIN workers w ON r.worker_id = w.id) helper1
        )
        SELECT * FROM helper2
        ORDER BY compensation_diff DESC;
        """
        async with async_engine.connect() as conn:
            r = aliased(resumes_table)
            w = aliased(workers_table)
            subq = (
                select(
                    r,
                    w,
                    func.avg(r.c.compensation).over(partition_by=r.c.workload).cast(Integer).label("avg_workload_compensation"),
                )
                # .select_from(r)
                .join(r, r.c.worker_id == w.c.id).subquery("helper1")
            )
            cte = (
                select(
                    subq.c.worker_id,
                    subq.c.username,
                    subq.c.compensation,
                    subq.c.workload,
                    subq.c.avg_workload_compensation,
                    (subq.c.compensation - subq.c.avg_workload_compensation).label("compensation_diff"),
                )
                .cte("helper2")
            )
            query = (
                select(cte)
                .order_by(cte.c.compensation_diff.desc())
            )

            res = await conn.execute(query)
            result = res.all()
            print(f"{len(result)=}. {result=}")

    # Relationships отсутствуют при использовании Table
