from sqlalchemy import Integer, and_, cast, func, insert, select
from src.database import sync_engine, async_engine, Base, session_factory, async_session_factory
from models import VacanciesOrm, WorkersOrm, ResumesOrm, WorkloadsEnum
from sqlalchemy.orm import aliased, joinedload, selectinload, contains_eager
from schemas import WorkersRelDTO, ResumesRelVacanciesRepliedDTO,ResumesRelVacanciesRepliedWithoutVacancyCompensationDTO
from database import Base




class SyncORM:
    @staticmethod
    def create_tables_sync_orm():
        sync_engine.echo = True
        Base.metadata.drop_all(sync_engine)
        Base.metadata.create_all(sync_engine)
        sync_engine.echo = True
    
    @staticmethod
    def insert_workers_sync_orm():
        with session_factory() as session:
            worker_Fatih = WorkersOrm(username="Fatih Sultan Mehmet")
            worker_Selahaattin = WorkersOrm(username="Selahaattin Eyyubi")
            session.add_all([worker_Fatih, worker_Selahaattin])
            #session.flush() # flush() is used to get the id of the newly created object
            session.commit()

    @staticmethod
    def select_workers_sync_orm():
        with session_factory() as session:
            query = select(WorkersOrm)
            result = session.execute(query)
            workers = result.scalars().all()
            print(f"{workers=}")

    @staticmethod
    def update_workers_sync_orm(worker_id: int = 1, new_username: str = 'Mustafa Kemal Ataturk'):
        with session_factory() as session:
            worker_Ataturk = session.get(WorkersOrm, worker_id)
            worker_Ataturk.username = new_username
            #session.expire_all() # expire_all() is used to refresh the object, so that the new changes are reflected. 
            session.refresh(worker_Ataturk) # refresh() is used to refresh the object, so that the new changes are reflected
            session.commit()
    
    @staticmethod
    def insert_resumes_sync_orm():
        with session_factory() as session:
            resume_Fatih_1 = ResumesOrm(title="Sultan", compensation=50000, workload=WorkloadsEnum.full_time, worker_id=1)
            resume_Fatih_2 = ResumesOrm(title="Sultanlar sultani", compensation=150000, workload=WorkloadsEnum.full_time, worker_id=1)
            resume_Selahaattin_1 = ResumesOrm(title="Sultan Eyyubi", compensation=250000, workload=WorkloadsEnum.part_time, worker_id=2)
            resume_Selahaattin_2 = ResumesOrm(title="Eyyubi", compensation=300000, workload=WorkloadsEnum.full_time, worker_id=2)
            session.add_all([resume_Fatih_1, resume_Fatih_2, resume_Selahaattin_1, resume_Selahaattin_2])
            session.commit()

    @staticmethod
    def select_resumes_avg_compensation_sync_orm(like_title: str = 'Sultan'):
            """
            select workload, avg(compensation)::int as avg_compensation
            from resumes
            where title like '%Sultan%' and compensation > 40000
            group by workload
            """
            with session_factory() as session:
                query =  (
                    select(
                        ResumesOrm.workload,
                        cast(func.avg(ResumesOrm.compensation), Integer).label("avg_compensation"),
                        )
                        .select_from(ResumesOrm)
                        .filter(and_(ResumesOrm.title.contains(like_title), ResumesOrm.compensation > 40000))
                        .group_by(ResumesOrm.workload)
                        .having(cast(func.avg(ResumesOrm.compensation), Integer) > 70000)
                )

                print(query.compile(compile_kwargs={"literal_binds": True}))
                result = session.execute(query)
                print(f"{result.all()}")
    
    @staticmethod
    def insert_additional_resumes_sync_orm():
        with session_factory() as session:
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
            insert_workers = insert(WorkersOrm).values(workers)
            insert_resumes = insert(ResumesOrm).values(resumes)
            session.execute(insert_workers)
            session.execute(insert_resumes)
            session.commit()

    @staticmethod
    def join_cte_subquery_window_function_sync_orm(like_title: str = 'Sultan'):
        """
        with helper2 as (
            select *, compensation-avg_workload_compensation as compensation_diff
            from
            (select 
                w.id,
                w.username,
                r.compensation,
                r.workload,
                avg(r.compensation) over (partition by workload)::int as avg_workload_compensation
            FROM resumes r
            join workers w on r.worker_id = w.id) helper1
        )
        select * from helper2
        order by compensation_diff desc
        """
        with session_factory() as session:
            r = aliased(ResumesOrm)
            w = aliased(WorkersOrm)
            subq = (
                select(
                    w.id,
                    w.username,
                    r.compensation,
                    r.workload,
                    func.avg(r.compensation).over(partition_by=r.workload).cast(Integer).label("avg_workload_compensation")
                )
                .join(r, r.worker_id == w.id).subquery("helper1")
            )
            cte = (
                select(
                    subq.c.id,
                    subq.c.username, 
                    subq.c.compensation,
                    subq.c.workload,
                    subq.c.avg_workload_compensation,
                    (subq.c.compensation - subq.c.avg_workload_compensation).label("compensation_diff")
                )
                .cte("helper2")
            )

            query = (
                select(cte)
                .order_by(cte.c.compensation_diff.desc())
            )
            #print(query.compile(compile_kwargs={"literal_binds": True}))
            result = session.execute(query)
            print(f"{result.all()}")
    
    @staticmethod
    def select_workers_with_lazy_relationships_sync_orm():
        with session_factory() as session:
            query = (
                select(WorkersOrm) 
            )
            result = session.execute(query)
            workers = result.scalars().all()
            worker_1_resumes = workers[0].resumes
            print(f"{worker_1_resumes=}")
            worker_2_resumes = workers[1].resumes
            print(f"{worker_2_resumes=}")
    
    @staticmethod
    def select_workers_with_joined_relationships_sync_orm():
        with session_factory() as session:
            query = (
                select(WorkersOrm)
                .options(joinedload(WorkersOrm.resumes)) 
            )
            result = session.execute(query)
            workers = result.unique().scalars().all()
            worker_1_resumes = workers[2].resumes
            worker_2_resumes = workers[3].resumes
    
    @staticmethod
    def select_workers_with_selectin_relationships_sync_orm():
        with session_factory() as session:
            query = (
                select(WorkersOrm)
                .options(selectinload(WorkersOrm.resumes)) 
            )
            res = session.execute(query)
            result = res.unique().scalars().all()

            worker_1_resumes = result[0].resumes
            print(f"{worker_1_resumes=}")

            worker_2_resumes = result[1].resumes
            print(f"{worker_2_resumes=}")

    @staticmethod
    def select_workers_with_condition_relationship_sync_orm():
        with session_factory() as session:
            query = (
                select(WorkersOrm)
                .options(selectinload(WorkersOrm.resumes_parttime))
            )

            res = session.execute(query)
            result = res.scalars().all()
            print(result)
            return result

    @staticmethod
    def select_workers_with_condition_relationships_contains_eager_sync_orm():
        with session_factory() as session:
            query = (
                select(WorkersOrm)
                .join(WorkersOrm.resumes)
                .options(contains_eager(WorkersOrm.resumes))
                #.filter(ResumesOrm.workload == 'part_time')
            )

            res = session.execute(query)
            result = res.unique().scalars().all()
            print(f"{result=}")
    
    @staticmethod
    def select_workers_with_relationship_contains_eager_with_limit_sync_orm():
        # Oзнакомиться: https://stackoverflow.com/a/72298903/22259413 
        with session_factory() as session:
            subq = (
                select(ResumesOrm.id.label("parttime_resume_id"))
                .filter(ResumesOrm.worker_id == WorkersOrm.id)
                .order_by(WorkersOrm.id.desc())
                .limit(1)
                .scalar_subquery()
                .correlate(WorkersOrm)
            )

            query = (
                select(WorkersOrm)
                .join(ResumesOrm, ResumesOrm.id.in_(subq))
                .options(contains_eager(WorkersOrm.resumes))
            )

            res = session.execute(query)
            result = res.unique().scalars().all()
            print(result)
            return result

    @staticmethod
    def convert_workers_to_dto_sync_orm():
        with session_factory() as session:
            query = (
                select(WorkersOrm)
                .options(selectinload(WorkersOrm.resumes))
                .limit(5)
            )

            res = session.execute(query)            
            result_orm = res.scalars().all()
            print(f"{result_orm=}")
            result_dto = [WorkersRelDTO.model_validate(row, from_attributes=True) for row in result_orm]
            print(f"{result_dto=}")
            return result_dto


    @staticmethod
    def add_vacancies_and_replies_sync_orm():
        with session_factory() as session:
            vacancy_1 = VacanciesOrm(title="Software Developer", compensation=100000)
            vacancy_2 = VacanciesOrm(title="Data Scientist", compensation=150000)
            vacancy_3 = VacanciesOrm(title="Machine Learning Engineer", compensation=200000)    
            
            resume_1 = session.get(ResumesOrm, 1)
            resume_2 = session.get(ResumesOrm, 2)
            resume_3 = session.get(ResumesOrm, 3)
            resume_1.vacancies_replied.append(vacancy_1)
            resume_1.vacancies_replied.append(vacancy_2)
            resume_1.vacancies_replied.append(vacancy_3)
            resume_2.vacancies_replied.append(vacancy_1)
            resume_2.vacancies_replied.append(vacancy_2)
            resume_3.vacancies_replied.append(vacancy_1)
            resume_3.vacancies_replied.append(vacancy_3)
            session.commit()
    
    @staticmethod
    def select_resumes_with_all_relationships_sync_orm():
        with session_factory() as session:
            query = (
                select(ResumesOrm)
                .options(joinedload(ResumesOrm.worker))
                .options(selectinload(ResumesOrm.vacancies_replied).load_only(VacanciesOrm.title))
            )
            
            res = session.execute(query)
            result_orm = res.unique().scalars().all()
            print(f"{result_orm=}")
            result_dto = [ResumesRelVacanciesRepliedDTO.model_validate(row, from_attributes=True) for row in result_orm]
            print(f"{result_dto=}")
            return result_dto

            

class AsyncORM:
    @staticmethod
    async def create_tables_async_orm():
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)

    @staticmethod
    async def insert_workers_async_orm():
        async with async_session_factory() as session:
            worker_Fatih = WorkersOrm(username="Fatih Sultan Mehmet")
            worker_Selahaattin = WorkersOrm(username="Selahattin Eyyubi")
            session.add_all([worker_Fatih, worker_Selahaattin])
            # flush взаимодействует с БД, поэтому пишем await
            await session.flush()
            await session.commit()

    @staticmethod
    async def select_workers_async_orm():
        async with async_session_factory() as session:
            query = select(WorkersOrm)
            result = await session.execute(query)
            workers = result.scalars().all()
            print(f"{workers=}")

    @staticmethod
    async def update_workers_async_orm(worker_id: int = 1, new_username: str = 'Mustafa Kemal Ataturk'):
        async with async_session_factory() as session:
            worker_Ataturk = await session.get(WorkersOrm, worker_id)
            worker_Ataturk.username = new_username
            await session.refresh(worker_Ataturk)
            await session.commit()

    @staticmethod
    async def insert_resumes_async_orm():
        async with async_session_factory() as session:
            resume_Fatih_1 = ResumesOrm(title="Sultan", compensation=50000, workload=WorkloadsEnum.full_time, worker_id=1)
            resume_Fatih_2 = ResumesOrm(title="Sultanlar sultani", compensation=150000, workload=WorkloadsEnum.full_time, worker_id=1)
            resume_Selahaattin_1 = ResumesOrm(title="Sultan Eyyubi", compensation=250000, workload=WorkloadsEnum.part_time, worker_id=2)
            resume_Selahaattin_2 = ResumesOrm(title="Eyyubi", compensation=300000, workload=WorkloadsEnum.full_time, worker_id=2)
            session.add_all([resume_Fatih_1, resume_Fatih_2, resume_Selahaattin_1, resume_Selahaattin_2])
            await session.commit()

    @staticmethod
    async def select_resumes_avg_compensation_async_orm(like_title: str = 'Sultan'):
            """
            select workload, avg(compensation)::int as avg_compensation
            from resumes
            where title like '%Sultan%' and compensation > 40000
            group by workload"""
            async with async_session_factory() as session:
                query = (
                    select(
                        ResumesOrm.workload,
                        # 1 вариант использования cast
                        # cast(func.avg(ResumesOrm.compensation), Integer).label("avg_compensation"),
                        # 2 вариант использования cast (предпочтительный способ)
                        func.avg(ResumesOrm.compensation).cast(Integer).label("avg_compensation"),
                    )
                    .select_from(ResumesOrm)
                    .filter(and_(
                        ResumesOrm.title.contains(like_title),
                        ResumesOrm.compensation > 40000,
                    ))
                    .group_by(ResumesOrm.workload)
                    .having(func.avg(ResumesOrm.compensation) > 70000)
                )
                print(query.compile(compile_kwargs={"literal_binds": True}))
                res = await session.execute(query)
                result = res.all()
                print(result[0].avg_compensation)

    @staticmethod
    async def insert_additional_resumes_async_orm():
        async with async_session_factory() as session:
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
            insert_workers = insert(WorkersOrm).values(workers)
            insert_resumes = insert(ResumesOrm).values(resumes)
            await session.execute(insert_workers)
            await session.execute(insert_resumes)
            await session.commit()
    
    @staticmethod
    async def join_cte_subquery_window_function_async_orm(like_title: str = 'Sultan'):
        """
        with helper2 as (
            select *, compensation-avg_workload_compensation as compensation_diff
            from
            (select 
                w.id,
                w.username,
                r.compensation,
                r.workload,
                avg(r.compensation) over (partition by workload)::int as avg_workload_compensation
            FROM resumes r
            join workers w on r.worker_id = w.id) helper1
        )
        select * from helper2
        order by compensation_diff desc
        """
        async with async_session_factory() as session:
            r = aliased(ResumesOrm)
            w = aliased(WorkersOrm)
            subq = (
                select(
                    w.id,
                    w.username,
                    r.compensation,
                    r.workload,
                    func.avg(r.compensation).over(partition_by=r.workload).cast(Integer).label("avg_workload_compensation")
                )
                .join(r, r.worker_id == w.id).subquery("helper1")
            )
            cte = (
                select(
                    subq.c.id,
                    subq.c.username, 
                    subq.c.compensation,
                    subq.c.workload,
                    subq.c.avg_workload_compensation,
                    (subq.c.compensation - subq.c.avg_workload_compensation).label("compensation_diff")
                )
                .cte("helper2")
            )

            query = (
                select(cte)
                .order_by(cte.c.compensation_diff.desc())
            )
            #print(query.compile(compile_kwargs={"literal_binds": True}))
            result = await session.execute(query)
            print(f"{result.all()}")

    @staticmethod
    async def select_workers_with_lazy_relationship_async_orm():
        async with async_session_factory() as session:
            query = (
                select(WorkersOrm)
            )
            
            res = await session.execute(query)
            result = res.scalars().all()

            # worker_1_resumes = result[0].resumes  # -> Приведет к ошибке
            # Нельзя использовать ленивую подгрузку в асинхронном варианте!

            # Ошибка: sqlalchemy.exc.MissingGreenlet: greenlet_spawn has not been called; can't call await_only() here. 
            # Was IO attempted in an unexpected place? (Background on this error at: https://sqlalche.me/e/20/xd2s)
            return result

    @staticmethod
    async def select_workers_with_joined_relationship_async_orm():
        async with async_session_factory() as session:
            query = (
                select(WorkersOrm)
                .options(joinedload(WorkersOrm.resumes))
            )
            
            res = await session.execute(query)
            result = res.unique().scalars().all()

            worker_1_resumes = result[0].resumes
            # print(worker_1_resumes)
            
            worker_2_resumes = result[1].resumes
            # print(worker_2_resumes)
            return result

    @staticmethod
    async def select_workers_with_selectin_relationship_async_orm():
        async with async_session_factory() as session:
            query = (
                select(WorkersOrm)
                .options(selectinload(WorkersOrm.resumes))
            )
            
            res = await session.execute(query)
            result = res.scalars().all()

            worker_1_resumes = result[0].resumes
            # print(worker_1_resumes)
            
            worker_2_resumes = result[1].resumes
            # print(worker_2_resumes)
            return result

    @staticmethod
    async def select_workers_with_condition_relationship_async_orm():
        async with async_session_factory() as session:
            query = (
                select(WorkersOrm)
                .options(selectinload(WorkersOrm.resumes_parttime))
            )

            res = await session.execute(query)
            result = res.scalars().all()
            print(result)
            return result


    @staticmethod
    async def select_workers_with_condition_relationship_contains_eager_async_orm():
        async with async_session_factory() as session:
            query = (
                select(WorkersOrm)
                .join(WorkersOrm.resumes)
                .options(contains_eager(WorkersOrm.resumes))
                .filter(ResumesOrm.workload == 'part_time')
            )

            res = await session.execute(query)
            result = res.unique().scalars().all()
            print(result)
            return result

    @staticmethod
    async def select_workers_with_relationship_contains_eager_with_limit_async_orm():
        # Oзнакомиться: https://stackoverflow.com/a/72298903/22259413 
        async with async_session_factory() as session:
            subq = (
                select(ResumesOrm.id.label("parttime_resume_id"))
                .filter(ResumesOrm.worker_id == WorkersOrm.id)
                .order_by(WorkersOrm.id.desc())
                .limit(1)
                .scalar_subquery()
                .correlate(WorkersOrm)
            )

            query = (
                select(WorkersOrm)
                .join(ResumesOrm, ResumesOrm.id.in_(subq))
                .options(contains_eager(WorkersOrm.resumes))
            )

            res = await session.execute(query)
            result = res.unique().scalars().all()
            print(result)
            return result

    @staticmethod
    async def convert_workers_to_dto_async_orm():
        async with async_session_factory() as session:
            query = (
                select(WorkersOrm)
                .options(selectinload(WorkersOrm.resumes))
                .limit(5)
            )

            res = await session.execute(query)            
            result_orm = res.scalars().all()
            print(f"{result_orm=}")
            result_dto = [WorkersRelDTO.model_validate(row, from_attributes=True) for row in result_orm]
            print(f"{result_dto=}")
            return result_dto
        
    @staticmethod
    async def add_vacancies_and_replies_async_orm():
        async with async_session_factory() as session:
            vacancy_1 = VacanciesOrm(title="Software Developer", compensation=100000)
            vacancy_2 = VacanciesOrm(title="Data Scientist", compensation=150000)
            vacancy_3 = VacanciesOrm(title="Machine Learning Engineer", compensation=200000)    
            
            get_resume_1 = select(ResumesOrm).options(selectinload(ResumesOrm.vacancies_replied)).filter_by(id=1)
            get_resume_2 = select(ResumesOrm).options(selectinload(ResumesOrm.vacancies_replied)).filter_by(id=2)
            get_resume_3 = select(ResumesOrm).options(selectinload(ResumesOrm.vacancies_replied)).filter_by(id=3)
            
            resume_1 = (await session.execute(get_resume_1)).scalar_one()
            resume_2 = (await session.execute(get_resume_2)).scalar_one()
            resume_3 = (await session.execute(get_resume_3)).scalar_one()

            resume_1.vacancies_replied.append(vacancy_1)
            resume_1.vacancies_replied.append(vacancy_2)
            resume_1.vacancies_replied.append(vacancy_3)
            resume_2.vacancies_replied.append(vacancy_1)
            resume_2.vacancies_replied.append(vacancy_2)
            resume_3.vacancies_replied.append(vacancy_1)
            resume_3.vacancies_replied.append(vacancy_3)
            await session.commit()

    @staticmethod
    async def select_resumes_with_all_relationships_async_orm():
        async with async_session_factory() as session:
            query = (
                select(ResumesOrm)
                .options(joinedload(ResumesOrm.worker))
                .options(selectinload(ResumesOrm.vacancies_replied).load_only(VacanciesOrm.title))
            )

            res = await session.execute(query)
            result_orm = res.unique().scalars().all()
            print(f"{result_orm=}")
            # Обратите внимание, что созданная в видео модель содержала лишний столбец compensation
            # И так как он есть в схеме ResumesRelVacanciesRepliedDTO, столбец compensation был вызван
            # Алхимией через ленивую загрузку. В асинхронном варианте это приводило к краху программы
            result_dto = [ResumesRelVacanciesRepliedWithoutVacancyCompensationDTO.model_validate(row, from_attributes=True) for row in result_orm]
            print(f"{result_dto=}")
            return result_dto        