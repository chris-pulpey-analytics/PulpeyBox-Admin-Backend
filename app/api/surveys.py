import io
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, Form
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


@router.get("/assign-template")
def download_assign_template(_: dict = Depends(get_current_admin)):
    """Descarga plantilla Excel para asignar usuarios por ID a una encuesta."""
    df = pd.DataFrame({"user_id": [123, 456, 789]})
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="Usuarios")
    out.seek(0)
    return StreamingResponse(
        iter([out.read()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=plantilla_asignar_usuarios.xlsx"},
    )


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


@router.get("/{survey_id}/responses")
def get_survey_responses(survey_id: int, _: dict = Depends(get_current_admin)):
    """Analiza las respuestas de una encuesta: distribución por pregunta y demografía de respondentes."""
    with get_db() as conn:
        cur = conn.cursor()

        # Info básica de la encuesta
        cur.execute(
            'SELECT "Id","Name","Code","Title" FROM public."Surveys" WHERE "Id"=%s',
            (survey_id,),
        )
        survey = cur.fetchone()
        if not survey:
            raise HTTPException(404, "Encuesta no encontrada")

        # Respuestas completadas con AnswersJson y datos demográficos
        cur.execute(
            """
            SELECT
                us."AnswersJson",
                us."AnsweredDate",
                COALESCE(gender."Name",'Sin dato') AS gender,
                CASE
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 18 AND 22 THEN '18-22'
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 23 AND 27 THEN '23-27'
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 28 AND 32 THEN '28-32'
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 33 AND 37 THEN '33-37'
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 38 AND 45 THEN '38-45'
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) > 45 THEN '45+'
                    ELSE 'Sin dato'
                END AS age_range,
                COALESCE(d."DepartmentName",'Sin dato') AS department
            FROM public."UserSurveys" us
            JOIN public."Users" u ON us."UserId" = u."Id"
            LEFT JOIN public."UserProfiles" up ON u."Id" = up."UserId"
            LEFT JOIN public."Settings" gender ON up."GenderId" = gender."Id"
            LEFT JOIN public."Cities" c ON up."CityId" = c."Id"
            LEFT JOIN public."Departments" d ON c."DepartmentId" = d."Id"
            WHERE us."SurveyId" = %s AND us."AnswersJson" IS NOT NULL
            ORDER BY us."AnsweredDate" DESC
            """,
            (survey_id,),
        )
        rows = cur.fetchall()

    # Procesar respuestas
    import json
    question_counts = {}
    gender_counts = {}
    age_counts = {}
    dept_counts = {}
    total_responses = len(rows)

    for row in rows:
        # Demografía
        g = row["gender"] or "Sin dato"
        a = row["age_range"] or "Sin dato"
        d = row["department"] or "Sin dato"
        gender_counts[g] = gender_counts.get(g, 0) + 1
        age_counts[a] = age_counts.get(a, 0) + 1
        dept_counts[d] = dept_counts.get(d, 0) + 1

        # Respuestas por pregunta
        if not row["AnswersJson"]:
            continue
        try:
            answers = json.loads(row["AnswersJson"]) if isinstance(row["AnswersJson"], str) else row["AnswersJson"]
            if isinstance(answers, dict):
                for q_key, answer in answers.items():
                    # Format: {"110": {"question": {"question": "..."}, "option": {"answer": "..."}}}
                    if isinstance(answer, dict) and "question" in answer and "option" in answer:
                        q_text = answer["question"].get("question", q_key)
                        ans_str = answer["option"].get("answer", "Sin respuesta")
                    else:
                        q_text = q_key
                        ans_str = str(answer) if answer is not None else "Sin respuesta"
                    if q_text not in question_counts:
                        question_counts[q_text] = {}
                    question_counts[q_text][ans_str] = question_counts[q_text].get(ans_str, 0) + 1
            elif isinstance(answers, list):
                for item in answers:
                    if isinstance(item, dict):
                        q_text = item.get("question", item.get("id", item.get("key", "Pregunta")))
                        ans_str = str(item.get("answer", item.get("value", "Sin respuesta")))
                        if q_text not in question_counts:
                            question_counts[q_text] = {}
                        question_counts[q_text][ans_str] = question_counts[q_text].get(q_text, 0) + 1
        except Exception:
            continue

    # Formatear para el frontend
    questions_formatted = [
        {
            "question": q,
            "total": sum(v for v in counts.values()),
            "answers": [
                {"label": label, "count": cnt}
                for label, cnt in sorted(counts.items(), key=lambda x: -x[1])
            ],
        }
        for q, counts in question_counts.items()
    ]

    return {
        "survey": dict(survey),
        "total_responses": total_responses,
        "questions": questions_formatted,
        "demographics": {
            "gender": [{"name": k, "value": v} for k, v in sorted(gender_counts.items(), key=lambda x: -x[1])],
            "age": [{"name": k, "value": v} for k, v in sorted(age_counts.items())],
            "department": [{"name": k, "value": v} for k, v in sorted(dept_counts.items(), key=lambda x: -x[1])[:10]],
        },
    }


# ─── Questions (ProductsSurvey) ───────────────────────────────────────────────

class QuestionCreate(BaseModel):
    product_name: str


@router.get("/{survey_id}/questions")
def get_survey_questions(survey_id: int, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'SELECT "Id", "SurveyId", "ProductName", "CreationDate" FROM public."ProductsSurvey" '
            'WHERE "SurveyId"=%s AND "Deleted" IS DISTINCT FROM TRUE ORDER BY "Id"',
            (survey_id,),
        )
        return [dict(r) for r in cur.fetchall()]


@router.post("/{survey_id}/questions")
def create_survey_question(survey_id: int, data: QuestionCreate, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'INSERT INTO public."ProductsSurvey" ("SurveyId","ProductName","CreationDate","Deleted") '
            'VALUES (%s,%s,NOW(),FALSE) RETURNING "Id"',
            (survey_id, data.product_name),
        )
        new_id = cur.fetchone()["Id"]
        conn.commit()
    return {"id": new_id}


@router.put("/{survey_id}/questions/{q_id}")
def update_survey_question(survey_id: int, q_id: int, data: QuestionCreate, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'UPDATE public."ProductsSurvey" SET "ProductName"=%s WHERE "Id"=%s AND "SurveyId"=%s',
            (data.product_name, q_id, survey_id),
        )
        conn.commit()
    return {"message": "Pregunta actualizada"}


@router.delete("/{survey_id}/questions/{q_id}")
def delete_survey_question(survey_id: int, q_id: int, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'UPDATE public."ProductsSurvey" SET "Deleted"=TRUE WHERE "Id"=%s AND "SurveyId"=%s',
            (q_id, survey_id),
        )
        conn.commit()
    return {"message": "Pregunta eliminada"}


# ─── Answers (AnswersSurveys) ─────────────────────────────────────────────────

class AnswerCreate(BaseModel):
    answer: str


@router.get("/{survey_id}/answers")
def get_survey_answers(survey_id: int, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'SELECT "Id", "SurveyId", "Answer", "CreationDate" FROM public."AnswersSurveys" '
            'WHERE "SurveyId"=%s AND "Deleted" IS DISTINCT FROM TRUE ORDER BY "Id"',
            (survey_id,),
        )
        return [dict(r) for r in cur.fetchall()]


@router.post("/{survey_id}/answers")
def create_survey_answer(survey_id: int, data: AnswerCreate, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'INSERT INTO public."AnswersSurveys" ("SurveyId","Answer","CreationDate","Deleted") '
            'VALUES (%s,%s,NOW(),FALSE) RETURNING "Id"',
            (survey_id, data.answer),
        )
        new_id = cur.fetchone()["Id"]
        conn.commit()
    return {"id": new_id}


@router.put("/{survey_id}/answers/{a_id}")
def update_survey_answer(survey_id: int, a_id: int, data: AnswerCreate, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'UPDATE public."AnswersSurveys" SET "Answer"=%s WHERE "Id"=%s AND "SurveyId"=%s',
            (data.answer, a_id, survey_id),
        )
        conn.commit()
    return {"message": "Respuesta actualizada"}


@router.delete("/{survey_id}/answers/{a_id}")
def delete_survey_answer(survey_id: int, a_id: int, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'UPDATE public."AnswersSurveys" SET "Deleted"=TRUE WHERE "Id"=%s AND "SurveyId"=%s',
            (a_id, survey_id),
        )
        conn.commit()
    return {"message": "Respuesta eliminada"}


# ─── Assign users to survey ───────────────────────────────────────────────────

class AssignUsersRequest(BaseModel):
    user_ids: list
    status_id: int = 146  # default Pendiente


@router.post("/{survey_id}/assign-users")
def assign_users_to_survey(survey_id: int, body: AssignUsersRequest, _: dict = Depends(get_current_admin)):
    """Asigna una lista de usuarios a la encuesta (omite duplicados, reporta errores)."""
    if not body.user_ids:
        return {"assigned": 0, "skipped": 0, "errors": []}

    assigned = 0
    skipped = 0
    errors = []
    with get_db() as conn:
        cur = conn.cursor()
        for uid in body.user_ids:
            try:
                uid = int(uid)
            except (ValueError, TypeError):
                errors.append({"user_id": str(uid), "reason": "ID inválido"})
                continue
            cur.execute(
                'SELECT "Id" FROM public."Users" WHERE "Id"=%s AND "Deleted" IS DISTINCT FROM TRUE',
                (uid,),
            )
            if not cur.fetchone():
                errors.append({"user_id": uid, "reason": "Usuario no encontrado"})
                continue
            cur.execute(
                'SELECT "Id" FROM public."UserSurveys" WHERE "UserId"=%s AND "SurveyId"=%s',
                (uid, survey_id),
            )
            if cur.fetchone():
                skipped += 1
                continue
            cur.execute(
                'INSERT INTO public."UserSurveys" ("UserId","SurveyId","AnswersJson","CreationDate","Deleted","StatusId","AnsweredDate") '
                "VALUES (%s,%s,NULL,NOW() AT TIME ZONE 'UTC',FALSE,%s,NULL)",
                (uid, survey_id, body.status_id),
            )
            assigned += 1
        conn.commit()

    return {"assigned": assigned, "skipped": skipped, "errors": errors}


@router.post("/{survey_id}/assign-users-excel")
async def assign_users_excel_survey(
    survey_id: int,
    file: UploadFile = File(...),
    status_id: int = Form(146),
    _: dict = Depends(get_current_admin),
):
    """Asigna usuarios a la encuesta desde un Excel con columna 'user_id'."""
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents))
    except Exception:
        raise HTTPException(400, "Archivo inválido. Sube un .xlsx o .xls válido")

    col = next((c for c in df.columns if c.strip().lower() == "user_id"), None)
    if col is None:
        raise HTTPException(400, "El archivo debe tener una columna llamada 'user_id'")

    assigned = 0
    skipped = 0
    errors = []
    with get_db() as conn:
        cur = conn.cursor()
        for i, row in df.iterrows():
            row_num = int(i) + 2
            raw = row[col]
            try:
                uid = int(raw)
            except (ValueError, TypeError):
                errors.append({"row": row_num, "user_id": str(raw), "reason": "ID inválido (no es un número)"})
                continue
            cur.execute(
                'SELECT "Id" FROM public."Users" WHERE "Id"=%s AND "Deleted" IS DISTINCT FROM TRUE',
                (uid,),
            )
            if not cur.fetchone():
                errors.append({"row": row_num, "user_id": uid, "reason": "Usuario no encontrado"})
                continue
            cur.execute(
                'SELECT "Id" FROM public."UserSurveys" WHERE "UserId"=%s AND "SurveyId"=%s',
                (uid, survey_id),
            )
            if cur.fetchone():
                skipped += 1
                continue
            cur.execute(
                'INSERT INTO public."UserSurveys" ("UserId","SurveyId","AnswersJson","CreationDate","Deleted","StatusId","AnsweredDate") '
                "VALUES (%s,%s,NULL,NOW() AT TIME ZONE 'UTC',FALSE,%s,NULL)",
                (uid, survey_id, status_id),
            )
            assigned += 1
        conn.commit()
    return {"assigned": assigned, "skipped": skipped, "errors": errors}


# ─── Preview users for survey assignment ─────────────────────────────────────

@router.get("/{survey_id}/preview-users")
def preview_users_for_survey(
    survey_id: int,
    gender_id: Optional[int] = None,
    marital_status_id: Optional[int] = None,
    income_range_id: Optional[int] = None,
    profession_id: Optional[int] = None,
    department_id: Optional[int] = None,
    city_id: Optional[int] = None,
    age_min: Optional[int] = None,
    age_max: Optional[int] = None,
    is_buy_manager_home: Optional[bool] = None,
    is_pregnant: Optional[bool] = None,
    is_interested_technology: Optional[bool] = None,
    is_alcohol_consume: Optional[bool] = None,
    is_tobacco_consume: Optional[bool] = None,
    survey_answer_id: Optional[int] = None,  # filter by answer given in any survey
    page_size: int = 100,
    _: dict = Depends(get_current_admin),
):
    """Previsualiza qué usuarios recibirían esta encuesta según filtros demográficos."""
    conditions = [
        'u."Deleted" IS DISTINCT FROM TRUE',
        'NOT EXISTS (SELECT 1 FROM public."UserSurveys" us2 WHERE us2."UserId"=u."Id" AND us2."SurveyId"=%s)',
    ]
    params = [survey_id]

    if gender_id:
        conditions.append('up."GenderId"=%s')
        params.append(gender_id)
    if marital_status_id:
        conditions.append('up."MaritalStatusId"=%s')
        params.append(marital_status_id)
    if income_range_id:
        conditions.append('up."IncomeRangeId"=%s')
        params.append(income_range_id)
    if profession_id:
        conditions.append('up."ProfessionsId"=%s')
        params.append(profession_id)
    if department_id:
        conditions.append('d."Id"=%s')
        params.append(department_id)
    if city_id:
        conditions.append('up."CityId"=%s')
        params.append(city_id)
    if age_min is not None:
        conditions.append("EXTRACT(YEAR FROM AGE(up.\"BirthDate\")) >= %s")
        params.append(age_min)
    if age_max is not None:
        conditions.append("EXTRACT(YEAR FROM AGE(up.\"BirthDate\")) <= %s")
        params.append(age_max)
    if is_buy_manager_home is not None:
        conditions.append('up."IsBuyManagerHome"=%s')
        params.append(is_buy_manager_home)
    if is_pregnant is not None:
        conditions.append('up."IsPregnant"=%s')
        params.append(is_pregnant)
    if is_interested_technology is not None:
        conditions.append('up."IsInterestedTechnology"=%s')
        params.append(is_interested_technology)
    if is_alcohol_consume is not None:
        conditions.append('up."IsAlcoholConsume"=%s')
        params.append(is_alcohol_consume)
    if is_tobacco_consume is not None:
        conditions.append('up."IsTobaccoConsume"=%s')
        params.append(is_tobacco_consume)
    if survey_answer_id is not None:
        conditions.append(
            "EXISTS (SELECT 1 FROM public.\"UserSurveys\" us3 WHERE us3.\"UserId\"=u.\"Id\" "
            "AND us3.\"AnswersJson\" IS NOT NULL "
            "AND us3.\"AnswersJson\"::text LIKE %s)"
        )
        params.append(f'%"id":{survey_answer_id}%')

    where = "WHERE " + " AND ".join(conditions)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            f'SELECT COUNT(DISTINCT u."Id") FROM public."Users" u '
            f'JOIN public."UserProfiles" up ON u."Id"=up."UserId" '
            f'LEFT JOIN public."Cities" c ON up."CityId"=c."Id" '
            f'LEFT JOIN public."Departments" d ON c."DepartmentId"=d."Id" '
            f'{where}',
            params,
        )
        total = cur.fetchone()["count"]

        cur.execute(
            f'SELECT DISTINCT u."Id", COALESCE(u."Name",\'\') ||\' \'|| COALESCE(u."LastName",\'\') AS full_name, '
            f'u."Email", u."MobilNumber" AS phone '
            f'FROM public."Users" u '
            f'JOIN public."UserProfiles" up ON u."Id"=up."UserId" '
            f'LEFT JOIN public."Cities" c ON up."CityId"=c."Id" '
            f'LEFT JOIN public."Departments" d ON c."DepartmentId"=d."Id" '
            f'{where} LIMIT %s',
            params + [page_size],
        )
        sample = [dict(r) for r in cur.fetchall()]

    return {"total": total, "sample": sample}


# ─── Internal survey report (Excel) ──────────────────────────────────────────

class InternalReportRequest(BaseModel):
    survey_ids: list
    status_id: Optional[int] = None


import unicodedata, re

def _clean_sheet_name(name: str) -> str:
    name = unicodedata.normalize('NFKD', str(name)).encode('ASCII', 'ignore').decode('ASCII')
    name = re.sub(r'[^\w\-_ ]', '_', name)
    return name[:31]


@router.post("/internal-report")
def generate_internal_report(body: InternalReportRequest, _: dict = Depends(get_current_admin)):
    """Genera Excel con datos de usuarios + respuestas por encuesta (una hoja por encuesta)."""
    if not body.survey_ids:
        raise HTTPException(400, "Debe proporcionar al menos un ID de encuesta")

    survey_ids = [int(x) for x in body.survey_ids]

    # Build full user query (reuse users.py BASE_SELECT/FROM/GROUP structure)
    from app.api.users import BASE_SELECT, BASE_FROM, BASE_GROUP

    status_cond = "AND us.\"StatusId\" = %s" if body.status_id else ""
    user_sql = f"""
        WITH usuarios_survey AS (
            SELECT DISTINCT "UserId"
            FROM public."UserSurveys"
            WHERE "SurveyId" = ANY(%s) AND "Deleted" IS DISTINCT FROM TRUE
            {status_cond}
        )
        {BASE_SELECT},
        (
            SELECT json_agg(
                json_build_object(
                    'survey_id', us."SurveyId",
                    'survey_name', s."Name",
                    'status_name', st."Name",
                    'status_id', us."StatusId",
                    'answered_date', TO_CHAR(us."AnsweredDate", 'DD/MM/YYYY HH24:MI'),
                    'answers_json', us."AnswersJson"
                ) ORDER BY us."SurveyId"
            )
            FROM public."UserSurveys" us
            LEFT JOIN public."Surveys" s ON us."SurveyId"=s."Id"
            LEFT JOIN public."Settings" st ON us."StatusId"=st."Id"
            WHERE us."UserId"=u."Id" AND us."SurveyId"=ANY(%s) AND us."Deleted" IS DISTINCT FROM TRUE
            {status_cond}
        ) AS "_survey_data"
        {BASE_FROM}
        WHERE u."Id" IN (SELECT "UserId" FROM usuarios_survey)
        AND u."Deleted" IS DISTINCT FROM TRUE
        {BASE_GROUP}
        ORDER BY u."Id"
    """

    sql_params = [survey_ids]
    if body.status_id:
        sql_params.append(body.status_id)
    sql_params.append(survey_ids)
    if body.status_id:
        sql_params.append(body.status_id)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(user_sql, sql_params)
        rows = [dict(r) for r in cur.fetchall()]

        # Get survey names for sheet naming
        cur.execute(
            'SELECT "Id", "Name" FROM public."Surveys" WHERE "Id"=ANY(%s)',
            (survey_ids,),
        )
        survey_names = {r["Id"]: r["Name"] for r in cur.fetchall()}

    # Process rows into per-survey DataFrames
    base_cols = [k for k in (rows[0].keys() if rows else []) if k != "_survey_data"]
    per_survey: dict = {sid: [] for sid in survey_ids}

    import json as _json
    for row in rows:
        surveys_data = row.get("_survey_data") or []
        for entry in surveys_data:
            sid = entry.get("survey_id")
            if sid not in per_survey:
                continue
            user_row = {c: row[c] for c in base_cols if c in row}
            user_row["Estado Encuesta"] = entry.get("status_name", "-")
            user_row["Fecha Respuesta"] = entry.get("answered_date", "-")

            answers_raw = entry.get("answers_json")
            if answers_raw:
                try:
                    answers = _json.loads(answers_raw) if isinstance(answers_raw, str) else answers_raw
                    if isinstance(answers, dict):
                        for _k, v in answers.items():
                            if isinstance(v, dict) and "question" in v and "option" in v:
                                q = v["question"].get("question", _k)
                                a = v["option"].get("answer", "-")
                            else:
                                q, a = str(_k), str(v)
                            user_row[q] = a
                except Exception:
                    pass
            per_survey[sid].append(user_row)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Full users sheet
        df_users = pd.DataFrame([{c: row[c] for c in base_cols if c in row} for row in rows])
        df_users.to_excel(writer, sheet_name="Usuarios", index=False)

        for sid in survey_ids:
            data = per_survey[sid]
            if data:
                sheet_name = _clean_sheet_name(f"{sid} {survey_names.get(sid, '')}")
                pd.DataFrame(data).to_excel(writer, sheet_name=sheet_name, index=False)

        # Summary sheet
        summary_rows = [
            {"Survey ID": sid, "Nombre": survey_names.get(sid, "-"),
             "Usuarios encontrados": len(per_survey[sid]),
             "StatusId filtrado": body.status_id or "Todos"}
            for sid in survey_ids
        ]
        pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Resumen", index=False)

    output.seek(0)
    return StreamingResponse(
        iter([output.read()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=reporte_interno.xlsx"},
    )


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
