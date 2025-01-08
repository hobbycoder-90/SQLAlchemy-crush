from sqlalchemy import Column, Integer, String, ForeignKey, Table, MetaData, text, TIMESTAMP, Enum
from database import Base, str_256
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing import Annotated, Optional
import enum
from datetime import datetime, timezone


intpk = Annotated[int, mapped_column(primary_key=True)]
an_created_at = Annotated[datetime, mapped_column(server_default=text("TIMEZONE('utc', now())"))]
an_uptated_at = Annotated[datetime, mapped_column(server_default=text("TIMEZONE('utc', now())"),onupdate=datetime.now(timezone.utc))]

class WorkloadsEnum(enum.Enum):
    full_time = "full_time"
    part_time = "part_time"
    remote = "remote"
    flexible = "flexible"



class WorkersOrm(Base):
    __tablename__ = "workers"

    id : Mapped[int] = mapped_column(primary_key=True, index=True)
    username : Mapped[str]

    resumes: Mapped[list["ResumesOrm"]] = relationship("ResumesOrm", back_populates="worker")

    resumes_parttime: Mapped[list["ResumesOrm"]] = relationship(
        back_populates="worker",
        primaryjoin="and_(WorkersOrm.id == ResumesOrm.worker_id, ResumesOrm.workload == 'part_time')",
        order_by="ResumesOrm.id.desc()",
    )



class ResumesOrm(Base):
    __tablename__ = "resumes"

    id : Mapped[intpk]
    title : Mapped[str_256]
    compensation : Mapped[int | None]
    workload : Mapped[WorkloadsEnum]
    worker_id : Mapped[int] = mapped_column(ForeignKey("workers.id", ondelete="CASCADE"))
    created_at : Mapped[an_created_at]
    updated_at : Mapped[an_uptated_at]

    worker: Mapped[list["WorkersOrm"]] = relationship("WorkersOrm", back_populates="resumes")

    vacancies_replied : Mapped[list["VacanciesOrm"]] = relationship(
        back_populates="resumes_replied",
        secondary="vacancies_replies",
    )


class VacanciesOrm(Base):
    __tablename__ = "vacancies"

    id : Mapped[intpk]
    title : Mapped[str_256]
    compensation : Mapped[int | None]

    resumes_replied : Mapped[list["ResumesOrm"]] = relationship(
        back_populates="vacancies_replied",
        secondary="vacancies_replies"
        )


class VacanciesRepliesOrm(Base):
    __tablename__ = "vacancies_replies"

    id : Mapped[intpk]
    resume_id : Mapped[int] = mapped_column(
        ForeignKey("resumes.id", ondelete="CASCADE"),
        primary_key=True
    )
    vacancy_id : Mapped[int] = mapped_column(
        ForeignKey("vacancies.id", ondelete="CASCADE"),
        primary_key=True
    )
    cover_letter : Mapped[Optional[str]]






metadata_obj = MetaData()

workers_table = Table(
    "workers",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("username", String),
)

resumes_table = Table(
    "resumes",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("title", String(256)),
    Column("compensation", Integer, nullable=True),
    Column("workload", Enum(WorkloadsEnum)),
    Column("worker_id", ForeignKey("workers.id", ondelete="CASCADE")),
    Column("created_at", TIMESTAMP,server_default=text("TIMEZONE('utc', now())")),
    Column("updated_at", TIMESTAMP,server_default=text("TIMEZONE('utc', now())"), onupdate=datetime.now(timezone.utc)),
)

vacancies_table = Table(
    "vacancies",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("title", String),
    Column("compensation", Integer, nullable=True),
)

vacancies_replies_table = Table(
    "vacancies_replies",
    metadata_obj,
    Column("resume_id", ForeignKey("resumes.id", ondelete="CASCADE"), primary_key=True),
    Column("vacancy_id", ForeignKey("vacancies.id", ondelete="CASCADE"), primary_key=True),
)

