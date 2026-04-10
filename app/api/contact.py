import io
import pandas as pd
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from typing import Optional
from app.api.auth import get_current_admin
from app.core.database import get_db

router = APIRouter(prefix="/contact", tags=["contact"])


@router.get("")
def list_contact(
    search: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
    _: dict = Depends(get_current_admin),
):
    offset = (page - 1) * page_size
    conditions = ['c."Deleted" IS DISTINCT FROM TRUE']
    params = []

    if search:
        conditions.append(
            '(c."Name" ILIKE %s OR c."LastName" ILIKE %s OR c."Email" ILIKE %s OR c."Company" ILIKE %s)'
        )
        t = f"%{search}%"
        params.extend([t, t, t, t])

    where = "WHERE " + " AND ".join(conditions)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM public."ContactUs" c {where}', params)
        total = cur.fetchone()["count"]

        cur.execute(
            f"""
            SELECT c."Id", c."Name", c."LastName", c."Email",
                   c."PhoneNumber", c."Company", c."Message",
                   c."AcceptTermCondition", c."CreationDate",
                   s."Name" AS phone_code
            FROM public."ContactUs" c
            LEFT JOIN public."Settings" s ON c."CodeNumberId" = s."Id"
            {where}
            ORDER BY c."CreationDate" DESC
            LIMIT %s OFFSET %s
            """,
            params + [page_size, offset],
        )
        rows = [dict(r) for r in cur.fetchall()]

    return {"total": total, "page": page, "page_size": page_size, "data": rows}


@router.get("/export")
def export_contact(
    search: Optional[str] = None,
    format: str = Query("xlsx", pattern="^(xlsx|csv)$"),
    _: dict = Depends(get_current_admin),
):
    conditions = ['c."Deleted" IS DISTINCT FROM TRUE']
    params = []
    if search:
        conditions.append('(c."Name" ILIKE %s OR c."Email" ILIKE %s OR c."Company" ILIKE %s)')
        t = f"%{search}%"
        params.extend([t, t, t])
    where = "WHERE " + " AND ".join(conditions)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT c."Id", c."Name" AS "Nombre", c."LastName" AS "Apellido",
                   c."Email" AS "Correo", c."PhoneNumber" AS "Teléfono",
                   c."Company" AS "Empresa", c."Message" AS "Mensaje",
                   TO_CHAR(c."CreationDate", 'DD/MM/YYYY HH24:MI') AS "Fecha"
            FROM public."ContactUs" c {where}
            ORDER BY c."CreationDate" DESC
            """,
            params,
        )
        rows = [dict(r) for r in cur.fetchall()]

    df = pd.DataFrame(rows)
    if format == "csv":
        out = io.StringIO()
        df.to_csv(out, index=False, encoding="utf-8-sig")
        out.seek(0)
        return StreamingResponse(
            iter([out.getvalue()]), media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=contactenos.csv"},
        )
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="Contactenos")
    out.seek(0)
    return StreamingResponse(
        iter([out.read()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=contactenos.xlsx"},
    )
