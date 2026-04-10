import io
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from typing import Optional
from datetime import date
from pydantic import BaseModel
from app.api.auth import get_current_admin
from app.core.database import get_db

router = APIRouter(prefix="/surveys", tags=["surveys"])


class SurveyCreate(BaseModel):
    name: str
    code: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    survey_url: Optional[str] = None
    status_id: Optional[int] = None
    type_survey_id: Optional[int] = None
    category_id: Optional[int] = None
    expiration_date: Optional[date] = None
    default: Optional[bool] = False


class SurveyUpdate(SurveyCreate):
    pass


class LinkNewsRequest(BaseModel):
    news_id: int


@router.get("")
def list_surveys(
    search: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
    _: dict = Depends(get_current_admin),
):
    offset = (page - 1) * page_size
    conditions = ['s."Deleted" IS DISTINCT FROM TRUE']
    params = []

    if search:
        conditions.append('(s."Name" ILIKE %s OR s."Code" ILIKE %s OR s."Title" ILIKE %s)')
        t = f"%{search}%"
        params.extend([t, t, t])

    where = "WHERE " + " AND ".join(conditions)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM public."Surveys" s {where}', params)
        total = cur.fetchone()["count"]

        cur.execute(
            f"""
            SELECT s."Id", s."Name", s."Code", s."Title", s."Description",
                   s."SurveyUrl", s."Default", s."ExpirationDate",
                   s."CreationDate",
                   stat."Name" AS status_name,
                   type_s."Name" AS type_name,
                   (SELECT COUNT(*) FROM public."UserSurveys" us WHERE us."SurveyId" = s."Id") AS total_enrolled,
                   (SELECT COUNT(*) FROM public."UserSurveys" us WHERE us."SurveyId" = s."Id" AND us."StatusId" = 152) AS total_completed
            FROM public."Surveys" s
            LEFT JOIN public."Settings" stat ON s."StatusId" = stat."Id"
            LEFT JOIN public."Settings" type_s ON s."TypeSurveyId" = type_s."Id"
            {where}
            ORDER BY s."CreationDate" DESC
            LIMIT %s OFFSET %s
            """,
            params + [page_size, offset],
        )
        rows = [dict(r) for r in cur.fetchall()]

    return {"total": total, "page": page, "page_size": page_size, "data": rows}


@router.get("/export")
def export_surveys(
    search: Optional[str] = None,
    format: str = Query("xlsx", pattern="^(xlsx|csv)$"),
    _: dict = Depends(get_current_admin),
):
    conditions = ['s."Deleted" IS DISTINCT FROM TRUE']
    params = []
    if search:
        conditions.append('(s."Name" ILIKE %s OR s."Code" ILIKE %s)')
        t = f"%{search}%"
        params.extend([t, t])
    where = "WHERE " + " AND ".join(conditions)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT s."Id", s."Name", s."Code", s."Title", s."SurveyUrl",
                   stat."Name" AS "Estado", type_s."Name" AS "Tipo",
                   s."ExpirationDate" AS "Expiración",
                   s."CreationDate" AS "Creación",
                   (SELECT COUNT(*) FROM public."UserSurveys" us WHERE us."SurveyId" = s."Id") AS "Total Enrolados",
                   (SELECT COUNT(*) FROM public."UserSurveys" us WHERE us."SurveyId" = s."Id" AND us."StatusId" = 152) AS "Total Completados"
            FROM public."Surveys" s
            LEFT JOIN public."Settings" stat ON s."StatusId" = stat."Id"
            LEFT JOIN public."Settings" type_s ON s."TypeSurveyId" = type_s."Id"
            {where}
            ORDER BY s."CreationDate" DESC
            """,
            params,
        )
        rows = [dict(r) for r in cur.fetchall()]

    df = pd.DataFrame(rows)
    return _export_response(df, "encuestas", format)


@router.get("/{survey_id}")
def get_survey(survey_id: int, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT s.*, stat."Name" AS status_name, type_s."Name" AS type_name
            FROM public."Surveys" s
            LEFT JOIN public."Settings" stat ON s."StatusId" = stat."Id"
            LEFT JOIN public."Settings" type_s ON s."TypeSurveyId" = type_s."Id"
            WHERE s."Id" = %s AND s."Deleted" IS DISTINCT FROM TRUE
            """,
            (survey_id,),
        )
        survey = cur.fetchone()
        if not survey:
            raise HTTPException(404, "Encuesta no encontrada")

        cur.execute(
            """
            SELECT u."Id", u."Name", u."LastName", u."Email",
                   us."StatusId", us."AnsweredDate", us."AnswersJson",
                   st."Name" AS status_name
            FROM public."UserSurveys" us
            JOIN public."Users" u ON us."UserId" = u."Id"
            LEFT JOIN public."Settings" st ON us."StatusId" = st."Id"
            WHERE us."SurveyId" = %s
            ORDER BY us."AnsweredDate" DESC NULLS LAST
            """,
            (survey_id,),
        )
        users = [dict(r) for r in cur.fetchall()]

    return {"survey": dict(survey), "users": users}


@router.get("/{survey_id}/users/export")
def export_survey_users(
    survey_id: int,
    format: str = Query("xlsx", pattern="^(xlsx|csv)$"),
    _: dict = Depends(get_current_admin),
):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT u."Id", u."Name" AS "Nombre", u."LastName" AS "Apellido",
                   u."Email" AS "Correo", u."MobilNumber" AS "Teléfono",
                   st."Name" AS "Estado Encuesta",
                   TO_CHAR(us."AnsweredDate", 'DD/MM/YYYY HH24:MI') AS "Fecha Respuesta"
            FROM public."UserSurveys" us
            JOIN public."Users" u ON us."UserId" = u."Id"
            LEFT JOIN public."Settings" st ON us."StatusId" = st."Id"
            WHERE us."SurveyId" = %s
            ORDER BY us."AnsweredDate" DESC NULLS LAST
            """,
            (survey_id,),
        )
        rows = [dict(r) for r in cur.fetchall()]

    df = pd.DataFrame(rows)
    return _export_response(df, f"encuesta_{survey_id}_usuarios", format)


@router.post("")
def create_survey(data: SurveyCreate, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public."Surveys"
                ("Name","Code","Title","Description","SurveyUrl","StatusId","TypeSurveyId","CategoryId","ExpirationDate","Default","CreationDate","Deleted")
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),FALSE)
            RETURNING "Id"
            """,
            (data.name, data.code, data.title, data.description, data.survey_url,
             data.status_id, data.type_survey_id, data.category_id,
             data.expiration_date, data.default),
        )
        new_id = cur.fetchone()["Id"]
        conn.commit()
    return {"id": new_id, "message": "Encuesta creada"}


@router.put("/{survey_id}")
def update_survey(survey_id: int, data: SurveyUpdate, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE public."Surveys" SET
                "Name"=%s,"Code"=%s,"Title"=%s,"Description"=%s,"SurveyUrl"=%s,
                "StatusId"=%s,"TypeSurveyId"=%s,"CategoryId"=%s,
                "ExpirationDate"=%s,"Default"=%s
            WHERE "Id"=%s AND "Deleted" IS DISTINCT FROM TRUE
            """,
            (data.name, data.code, data.title, data.description, data.survey_url,
             data.status_id, data.type_survey_id, data.category_id,
             data.expiration_date, data.default, survey_id),
        )
        conn.commit()
    return {"message": "Encuesta actualizada"}


@router.post("/{survey_id}/link-news")
def link_survey_to_news(survey_id: int, body: LinkNewsRequest, _: dict = Depends(get_current_admin)):
    """Vincula la encuesta a una NewsAndPromotion actualizando su ActionUrl."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT "SurveyUrl" FROM public."Surveys" WHERE "Id"=%s', (survey_id,))
        survey = cur.fetchone()
        if not survey:
            raise HTTPException(404, "Encuesta no encontrada")

        cur.execute(
            'UPDATE public."NewsAndPromotions" SET "ActionUrl"=%s WHERE "Id"=%s',
            (survey["SurveyUrl"], body.news_id),
        )
        conn.commit()
    return {"message": "Encuesta vinculada a la noticia/promoción"}


@router.delete("/{survey_id}")
def delete_survey(survey_id: int, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('UPDATE public."Surveys" SET "Deleted"=TRUE WHERE "Id"=%s', (survey_id,))
        conn.commit()
    return {"message": "Encuesta eliminada"}


def _export_response(df: pd.DataFrame, name: str, format: str):
    if format == "csv":
        out = io.StringIO()
        df.to_csv(out, index=False, encoding="utf-8-sig")
        out.seek(0)
        return StreamingResponse(
            iter([out.getvalue()]), media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={name}.csv"},
        )
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="Datos")
    out.seek(0)
    return StreamingResponse(
        iter([out.read()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={name}.xlsx"},
    )
