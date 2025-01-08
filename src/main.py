import asyncio
import os
import sys

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(1, os.path.join(sys.path[0], '..'))

from src.queries.core import AsyncCore, SyncCore
from src.queries.orm import SyncORM, AsyncORM




async def main():
    # ========== SYNC ==========
    # CORE
    if "--core" in sys.argv and "--sync" in sys.argv:
        SyncCore.create_tables_sync_core()
        SyncCore.insert_workers_sync_core()
        SyncCore.select_workers_sync_core()
        SyncCore.update_workers_sync_core()
        SyncCore.insert_resumes_sync_core()
        SyncCore.select_resumes_avg_compensation_sync_core()
        SyncCore.insert_additional_resumes_sync_core()
        SyncCore.join_cte_subquery_window_func_sync_core()

    # ORM
    elif "--orm" in sys.argv and "--sync" in sys.argv:
        SyncORM.create_tables_sync_orm()
        SyncORM.insert_workers_sync_orm()
        SyncORM.select_workers_sync_orm()
        SyncORM.update_workers_sync_orm()
        SyncORM.insert_resumes_sync_orm()
        SyncORM.select_resumes_avg_compensation_sync_orm()
        SyncORM.insert_additional_resumes_sync_orm()
        SyncORM.join_cte_subquery_window_function_sync_orm()
        SyncORM.select_workers_with_lazy_relationships_sync_orm()
        SyncORM.select_workers_with_joined_relationships_sync_orm()
        SyncORM.select_workers_with_selectin_relationships_sync_orm()
        SyncORM.select_workers_with_condition_relationship_sync_orm()
        SyncORM.select_workers_with_condition_relationships_contains_eager_sync_orm()
        SyncORM.select_workers_with_relationship_contains_eager_with_limit_sync_orm()
        SyncORM.convert_workers_to_dto_sync_orm()
        SyncORM.add_vacancies_and_replies_sync_orm()
        SyncORM.select_resumes_with_all_relationships_sync_orm()
    

    # ========== ASYNC ==========
        # CORE
    elif "--core" in sys.argv and "--async" in sys.argv:
        await AsyncCore.create_tables_async_core()
        await AsyncCore.insert_workers_async_core()
        await AsyncCore.select_workers_async_core()
        await AsyncCore.update_workers_async_core()
        await AsyncCore.insert_resumes_async_core()
        await AsyncCore.select_resumes_avg_compensation_async_core()
        await AsyncCore.insert_additional_resumes_async_core()
        await AsyncCore.join_cte_subquery_window_func_async_core()
        
    # ORM
    elif "--orm" in sys.argv and "--async" in sys.argv:
        await AsyncORM.create_tables_async_orm()
        await AsyncORM.insert_workers_async_orm()
        await AsyncORM.select_workers_async_orm()
        await AsyncORM.update_workers_async_orm()
        await AsyncORM.insert_resumes_async_orm()
        await AsyncORM.select_resumes_avg_compensation_async_orm()
        await AsyncORM.insert_additional_resumes_async_orm()
        await AsyncORM.join_cte_subquery_window_function_async_orm()
        await AsyncORM.select_workers_with_lazy_relationship_async_orm()
        await AsyncORM.select_workers_with_joined_relationship_async_orm()
        await AsyncORM.select_workers_with_selectin_relationship_async_orm()
        await AsyncORM.select_workers_with_condition_relationship_async_orm()
        await AsyncORM.select_workers_with_condition_relationship_contains_eager_async_orm()
        await AsyncORM.select_workers_with_relationship_contains_eager_with_limit_async_orm()
        await AsyncORM.convert_workers_to_dto_async_orm()
        await AsyncORM.add_vacancies_and_replies_async_orm()
        await AsyncORM.select_resumes_with_all_relationships_async_orm()

# ========== FASTAPI ==========
def create_fastapi_app():
    app = FastAPI()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
    )

    @app.get("/workers")
    async def get_workers():
        #workers = await AsyncORM.convert_workers_to_dto_async_orm()
        workers = SyncORM.convert_workers_to_dto_sync_orm()
        return workers

    @app.get("/resumes")
    async def get_resumes():
        #resumes = await AsyncORM.select_resumes_with_all_relationships_async_orm()
        resumes = SyncORM.select_resumes_with_all_relationships_sync_orm()
        return resumes
    

    return app


app = create_fastapi_app()




if __name__ == "__main__":
    asyncio.run(main())
    uvicorn.run(
        app="src.main:app",
        reload=True,
    )
