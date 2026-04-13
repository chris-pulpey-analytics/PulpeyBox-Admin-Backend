import io
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from typing import Optional
from datetime import date
from pydantic import BaseModel
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
                   unp."FirstClickDate", unp."LastClickDate"
            FROM public."UserNewsAndPromotions" unp
            JOIN public."Users" u ON unp."UserId" = u."Id"
            WHERE unp."NewsAndPromotionId" = %s
            ORDER BY unp."LastClickDate" DESC NULLS LAST
            LIMIT 100
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
    """Asigna usuarios a la noticia/promoción desde un Excel con columna 'user_id'."""
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
            result = _assign_news_user(cur, uid, news_id, status)
            if result == "ok":
                assigned += 1
            elif result == "skip":
                skipped += 1
            else:
                errors.append({"row": row_num, "user_id": uid, "reason": "Usuario no encontrado"})
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
