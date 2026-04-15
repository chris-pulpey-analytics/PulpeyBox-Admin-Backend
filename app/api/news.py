import io
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from typing import Optional
from datetime import date, datetime
from pydantic import BaseModel
from psycopg2.extras import execute_values
from app.api.auth import get_current_admin
from app.core.database import get_db

router = APIRouter(prefix="/news", tags=["news"])


class NewsCreate(BaseModel):
    title: str
    description: str
    code: Optional[str] = None
    image_url: Optional[str] = None
    action_url: Optional[str] = None
    action_text: Optional[str] = None
    expiration_date: Optional[date] = None
    default: Optional[bool] = False
    link_types_id: Optional[int] = None
    banners_types_id: int


class NewsUpdate(NewsCreate):
    pass


@router.get("")
def list_news(
    search: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
    _: dict = Depends(get_current_admin),
):
    offset = (page - 1) * page_size
    conditions = ['n."Deleted" IS DISTINCT FROM TRUE']
    params = []

    if search:
        conditions.append('(n."Title" ILIKE %s OR n."Code" ILIKE %s OR n."Description" ILIKE %s)')
        t = f"%{search}%"
        params.extend([t, t, t])

    where = "WHERE " + " AND ".join(conditions)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM public."NewsAndPromotions" n {where}', params)
        total = cur.fetchone()["count"]

        cur.execute(
            f"""
            SELECT n."Id", n."Title", n."Description", n."Code",
                   n."ImageUrl", n."ActionUrl", n."ActionText",
                   n."ExpirationDate", n."Default", n."CreationDate",
                   lt."Name" AS link_type,
                   bt."Name" AS banner_type,
                   COUNT(DISTINCT unp."Id") AS total_sent,
                   COUNT(DISTINCT CASE WHEN unp."Status" >= 1 THEN unp."Id" END) AS total_viewed,
                   COUNT(DISTINCT CASE WHEN unp."ClickCount" > 0 THEN unp."Id" END) AS total_clicked,
                   COALESCE(SUM(unp."ClickCount"), 0) AS total_clicks
            FROM public."NewsAndPromotions" n
            LEFT JOIN public."Settings" lt ON n."LinkTypesId" = lt."Id"
            LEFT JOIN public."Settings" bt ON n."BannersTypesId" = bt."Id"
            LEFT JOIN public."UserNewsAndPromotions" unp ON n."Id" = unp."NewsAndPromotionId"
            {where}
            GROUP BY n."Id", n."Title", n."Description", n."Code",
                     n."ImageUrl", n."ActionUrl", n."ActionText",
                     n."ExpirationDate", n."Default", n."CreationDate",
                     lt."Name", bt."Name"
            ORDER BY n."CreationDate" DESC
            LIMIT %s OFFSET %s
            """,
            params + [page_size, offset],
        )
        rows = [dict(r) for r in cur.fetchall()]

    return {"total": total, "page": page, "page_size": page_size, "data": rows}


@router.get("/assign-template")
def download_news_assign_template(_: dict = Depends(get_current_admin)):
    """Descarga plantilla Excel para asignar usuarios por ID a una noticia/promoción."""
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


@router.get("/export")
def export_news(
    search: Optional[str] = None,
    format: str = Query("xlsx", pattern="^(xlsx|csv)$"),
    _: dict = Depends(get_current_admin),
):
    conditions = ['n."Deleted" IS DISTINCT FROM TRUE']
    params = []
    if search:
        conditions.append('(n."Title" ILIKE %s OR n."Code" ILIKE %s)')
        t = f"%{search}%"
        params.extend([t, t])
    where = "WHERE " + " AND ".join(conditions)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT n."Id", n."Title" AS "Título", n."Code" AS "Código",
                   n."ExpirationDate" AS "Expiración", n."CreationDate" AS "Creación",
                   COUNT(DISTINCT unp."Id") AS "Enviados",
                   COUNT(DISTINCT CASE WHEN unp."Status" >= 1 THEN unp."Id" END) AS "Vistos",
                   COALESCE(SUM(unp."ClickCount"), 0) AS "Clicks"
            FROM public."NewsAndPromotions" n
            LEFT JOIN public."UserNewsAndPromotions" unp ON n."Id" = unp."NewsAndPromotionId"
            {where}
            GROUP BY n."Id", n."Title", n."Code", n."ExpirationDate", n."CreationDate"
            ORDER BY n."CreationDate" DESC
            """,
            params,
        )
        rows = [dict(r) for r in cur.fetchall()]

    df = pd.DataFrame(rows)
    return _export_response(df, "noticias", format)


@router.get("/{news_id}")
def get_news(news_id: int, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT n.*, lt."Name" AS link_type, bt."Name" AS banner_type
            FROM public."NewsAndPromotions" n
            LEFT JOIN public."Settings" lt ON n."LinkTypesId" = lt."Id"
            LEFT JOIN public."Settings" bt ON n."BannersTypesId" = bt."Id"
            WHERE n."Id" = %s AND n."Deleted" IS DISTINCT FROM TRUE
            """,
            (news_id,),
        )
        news = cur.fetchone()
        if not news:
            raise HTTPException(404, "Noticia no encontrada")

        cur.execute(
            """
            SELECT u."Id", u."Name", u."LastName", u."Email",
                   unp."Status", unp."ClickCount",
                   unp."FirstClickDate", unp."LastClickDate",
                   COALESCE(EXTRACT(YEAR FROM AGE(up."BirthDate"))::TEXT, '-') AS edad,
                   COALESCE(gender."Name", '-') AS genero,
                   COALESCE(d."DepartmentName", '-') AS departamento,
                   COALESCE(c."CityName", '-') AS ciudad
            FROM public."UserNewsAndPromotions" unp
            JOIN public."Users" u ON unp."UserId" = u."Id"
            LEFT JOIN public."UserProfiles" up ON u."Id" = up."UserId"
            LEFT JOIN public."Settings" gender ON up."GenderId" = gender."Id"
            LEFT JOIN public."Cities" c ON up."CityId" = c."Id"
            LEFT JOIN public."Departments" d ON c."DepartmentId" = d."Id"
            WHERE unp."NewsAndPromotionId" = %s AND unp."Deleted" IS DISTINCT FROM TRUE
            ORDER BY unp."LastClickDate" DESC NULLS LAST
            LIMIT 500
            """,
            (news_id,),
        )
        users = [dict(r) for r in cur.fetchall()]

    return {"news": dict(news), "users": users}


@router.post("")
def create_news(data: NewsCreate, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public."NewsAndPromotions"
                ("Title","Description","Code","ImageUrl","ActionUrl","ActionText",
                 "ExpirationDate","Default","LinkTypesId","BannersTypesId","CreationDate","Deleted")
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),FALSE)
            RETURNING "Id"
            """,
            (data.title, data.description, data.code, data.image_url,
             data.action_url, data.action_text, data.expiration_date,
             data.default, data.link_types_id, data.banners_types_id),
        )
        new_id = cur.fetchone()["Id"]
        conn.commit()
    return {"id": new_id, "message": "Noticia/Promoción creada"}


@router.put("/{news_id}")
def update_news(news_id: int, data: NewsUpdate, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE public."NewsAndPromotions" SET
                "Title"=%s,"Description"=%s,"Code"=%s,"ImageUrl"=%s,
                "ActionUrl"=%s,"ActionText"=%s,"ExpirationDate"=%s,
                "Default"=%s,"LinkTypesId"=%s,"BannersTypesId"=%s
            WHERE "Id"=%s AND "Deleted" IS DISTINCT FROM TRUE
            """,
            (data.title, data.description, data.code, data.image_url,
             data.action_url, data.action_text, data.expiration_date,
             data.default, data.link_types_id, data.banners_types_id, news_id),
        )
        conn.commit()
    return {"message": "Actualizado"}


@router.delete("/{news_id}")
def delete_news(news_id: int, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('UPDATE public."NewsAndPromotions" SET "Deleted"=TRUE WHERE "Id"=%s', (news_id,))
        conn.commit()
    return {"message": "Eliminado"}


@router.get("/{news_id}/preview-users")
def preview_users_for_news(
    news_id: int,
    gender_id: Optional[int] = None,
    income_range_id: Optional[int] = None,
    profession_id: Optional[int] = None,
    department_id: Optional[int] = None,
    age_min: Optional[int] = None,
    age_max: Optional[int] = None,
    is_buy_manager_home: Optional[bool] = None,
    is_pregnant: Optional[bool] = None,
    is_interested_technology: Optional[bool] = None,
    is_alcohol_consume: Optional[bool] = None,
    is_tobacco_consume: Optional[bool] = None,
    page_size: int = 100,
    _: dict = Depends(get_current_admin),
):
    """Previsualiza qué usuarios recibirían esta noticia según filtros demográficos."""
    conditions = [
        'u."Deleted" IS DISTINCT FROM TRUE',
        'NOT EXISTS (SELECT 1 FROM public."UserNewsAndPromotions" unp WHERE unp."UserId"=u."Id" AND unp."NewsAndPromotionId"=%s)',
    ]
    params = [news_id]

    if gender_id:
        conditions.append('up."GenderId"=%s')
        params.append(gender_id)
    if income_range_id:
        conditions.append('up."IncomeRangeId"=%s')
        params.append(income_range_id)
    if profession_id:
        conditions.append('up."ProfessionsId"=%s')
        params.append(profession_id)
    if department_id:
        conditions.append('d."Id"=%s')
        params.append(department_id)
    if age_min is not None:
        conditions.append('EXTRACT(YEAR FROM AGE(up."BirthDate")) >= %s')
        params.append(age_min)
    if age_max is not None:
        conditions.append('EXTRACT(YEAR FROM AGE(up."BirthDate")) <= %s')
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


class AssignNewsUsersRequest(BaseModel):
    user_ids: list
    status: int = 0


def _assign_news_user(cur, uid: int, news_id: int, status: int) -> str:
    """Inserta usuario en UserNewsAndPromotions. Devuelve 'ok', 'skip' o 'error'."""
    cur.execute(
        'SELECT "Id" FROM public."Users" WHERE "Id"=%s AND "Deleted" IS DISTINCT FROM TRUE',
        (uid,),
    )
    if not cur.fetchone():
        return "not_found"
    cur.execute(
        'SELECT "Id" FROM public."UserNewsAndPromotions" WHERE "UserId"=%s AND "NewsAndPromotionId"=%s',
        (uid, news_id),
    )
    if cur.fetchone():
        return "skip"
    cur.execute(
        'INSERT INTO public."UserNewsAndPromotions" ("UserId","NewsAndPromotionId","Status","ClickCount","CreationDate","Deleted") '
        "VALUES (%s,%s,%s,0,NOW(),FALSE)",
        (uid, news_id, status),
    )
    return "ok"


@router.post("/{news_id}/assign-users")
def assign_users_to_news(news_id: int, body: AssignNewsUsersRequest, _: dict = Depends(get_current_admin)):
    """Asigna lista de usuarios a la noticia/promoción (omite duplicados, reporta errores)."""
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
            result = _assign_news_user(cur, uid, news_id, body.status)
            if result == "ok":
                assigned += 1
            elif result == "skip":
                skipped += 1
            else:
                errors.append({"user_id": uid, "reason": "Usuario no encontrado"})
        conn.commit()
    return {"assigned": assigned, "skipped": skipped, "errors": errors}


@router.post("/{news_id}/assign-users-excel")
async def assign_users_excel_news(
    news_id: int,
    file: UploadFile = File(...),
    status: int = Form(0),
    _: dict = Depends(get_current_admin),
):
    """
    Asigna usuarios a la noticia/promoción desde un Excel con columna 'user_id'.
    Usa bulk queries (ANY + execute_values): 3 queries totales en lugar de N×3.
    """
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents))
    except Exception:
        raise HTTPException(400, "Archivo inválido. Sube un .xlsx o .xls válido")

    col = next((c for c in df.columns if c.strip().lower() == "user_id"), None)
    if col is None:
        raise HTTPException(400, "El archivo debe tener una columna llamada 'user_id'")

    # ── 1. Parsear todos los IDs del Excel ──────────────────────────────────
    errors: list = []
    candidate_ids: list[int] = []
    uid_to_row: dict[int, int] = {}

    for i, row in df.iterrows():
        row_num = int(i) + 2
        raw = row[col]
        try:
            uid = int(raw)
            candidate_ids.append(uid)
            uid_to_row.setdefault(uid, row_num)
        except (ValueError, TypeError):
            errors.append({"row": row_num, "user_id": str(raw), "reason": "ID inválido (no es un número)"})

    if not candidate_ids:
        return {"assigned": 0, "skipped": 0, "errors": errors}

    with get_db() as conn:
        cur = conn.cursor()

        # ── 2. Validar existencia en una sola query ─────────────────────────
        cur.execute(
            'SELECT "Id" FROM public."Users" WHERE "Id" = ANY(%s) AND "Deleted" IS DISTINCT FROM TRUE',
            (candidate_ids,),
        )
        valid_ids: set[int] = {r["Id"] for r in cur.fetchall()}

        for uid in set(candidate_ids) - valid_ids:
            errors.append({
                "row": uid_to_row.get(uid),
                "user_id": uid,
                "reason": "Usuario no encontrado",
            })

        if not valid_ids:
            return {"assigned": 0, "skipped": 0, "errors": errors}

        # ── 3. Detectar ya asignados en una sola query ──────────────────────
        cur.execute(
            'SELECT "UserId" FROM public."UserNewsAndPromotions" '
            'WHERE "NewsAndPromotionId"=%s AND "UserId" = ANY(%s)',
            (news_id, list(valid_ids)),
        )
        already_assigned: set[int] = {r["UserId"] for r in cur.fetchall()}
        skipped = len(already_assigned)

        # ── 4. INSERT masivo en una sola query ──────────────────────────────
        to_insert = list(valid_ids - already_assigned)
        assigned = 0
        if to_insert:
            now = datetime.utcnow()
            execute_values(
                cur,
                'INSERT INTO public."UserNewsAndPromotions" '
                '("UserId","NewsAndPromotionId","Status","ClickCount","CreationDate","Deleted") '
                "VALUES %s",
                [(uid, news_id, status, 0, now, False) for uid in to_insert],
            )
            assigned = len(to_insert)

        conn.commit()

    return {"assigned": assigned, "skipped": skipped, "errors": errors}


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
