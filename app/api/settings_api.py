from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional
from pydantic import BaseModel
from app.api.auth import get_current_admin
from app.core.database import get_db

router = APIRouter(prefix="/settings", tags=["settings"])


class GroupCreate(BaseModel):
    name: str


class SettingCreate(BaseModel):
    name: str
    code: Optional[str] = None
    setting_group_id: int


class SettingUpdate(BaseModel):
    name: str
    code: Optional[str] = None


@router.get("/groups")
def list_groups(_: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT gs."Id", gs."Name",
                   COUNT(s."Id") AS settings_count
            FROM public."GroupSettings" gs
            LEFT JOIN public."Settings" s ON s."SettingGroupId" = gs."Id"
                AND s."Deleted" IS DISTINCT FROM TRUE
            WHERE gs."Deleted" IS DISTINCT FROM TRUE
            GROUP BY gs."Id", gs."Name"
            ORDER BY gs."Name"
            """
        )
        return [dict(r) for r in cur.fetchall()]


@router.post("/groups")
def create_group(data: GroupCreate, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'INSERT INTO public."GroupSettings" ("Name","CreationDate","Deleted") VALUES (%s,NOW(),FALSE) RETURNING "Id"',
            (data.name,),
        )
        new_id = cur.fetchone()["Id"]
        conn.commit()
    return {"id": new_id, "message": "Grupo creado"}


@router.put("/groups/{group_id}")
def update_group(group_id: int, data: GroupCreate, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'UPDATE public."GroupSettings" SET "Name"=%s WHERE "Id"=%s', (data.name, group_id)
        )
        conn.commit()
    return {"message": "Grupo actualizado"}


@router.delete("/groups/{group_id}")
def delete_group(group_id: int, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM public."Settings" WHERE "SettingGroupId"=%s AND "Deleted" IS DISTINCT FROM TRUE', (group_id,))
        if cur.fetchone()["count"] > 0:
            raise HTTPException(400, "El grupo tiene catálogos activos. Elimínalos primero.")
        cur.execute('UPDATE public."GroupSettings" SET "Deleted"=TRUE WHERE "Id"=%s', (group_id,))
        conn.commit()
    return {"message": "Grupo eliminado"}


@router.get("")
def list_settings(group_id: Optional[int] = None, _: dict = Depends(get_current_admin)):
    conditions = ['s."Deleted" IS DISTINCT FROM TRUE']
    params = []
    if group_id:
        conditions.append('s."SettingGroupId" = %s')
        params.append(group_id)
    where = "WHERE " + " AND ".join(conditions)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT s."Id", s."Name", s."Code", s."SettingGroupId",
                   gs."Name" AS group_name,
                   s."CreationDate"
            FROM public."Settings" s
            JOIN public."GroupSettings" gs ON s."SettingGroupId" = gs."Id"
            {where}
            ORDER BY gs."Name", s."Name"
            """,
            params,
        )
        return [dict(r) for r in cur.fetchall()]


@router.get("/grouped")
def settings_grouped(
    group_name: Optional[str] = Query(None, description="Filtrar por nombre del grupo exacto o parcial"),
    group_id: Optional[int] = Query(None, description="Filtrar por ID del grupo"),
    setting_id: Optional[int] = Query(None, description="Filtrar por ID del setting"),
    _: dict = Depends(get_current_admin)
):
    """Retorna todos los settings agrupados por GroupSetting para el frontend, con filtros opcionales."""
    with get_db() as conn:
        cur = conn.cursor()
        
        conditions = ['gs."Deleted" IS DISTINCT FROM TRUE']
        params = []
        
        if group_name:
            conditions.append('gs."Name" ILIKE %s')
            params.append(f"%{group_name}%")
        if group_id:
            conditions.append('gs."Id" = %s')
            params.append(group_id)
            
        setting_conditions = ['s."Deleted" IS DISTINCT FROM TRUE']
        if setting_id:
            setting_conditions.append('s."Id" = %s')
            params.append(setting_id)
            
        where_gs = " AND ".join(conditions)
        where_s = " AND ".join(setting_conditions)
        
        query = f"""
            SELECT gs."Id" AS group_id, gs."Name" AS group_name,
                   s."Id", s."Name", s."Code"
            FROM public."GroupSettings" gs
            LEFT JOIN public."Settings" s ON s."SettingGroupId" = gs."Id" AND {where_s}
            WHERE {where_gs}
            ORDER BY gs."Name", s."Name"
        """
        cur.execute(query, params)
        rows = cur.fetchall()

    grouped = {}
    for r in rows:
        gid = r["group_id"]
        if gid not in grouped:
            grouped[gid] = {
                "id": gid, 
                "Id": gid,
                "name": r["group_name"], 
                "group_name": r["group_name"], 
                "settings": []
            }
        if r["Id"]:
            grouped[gid]["settings"].append({
                "id": r["Id"],
                "Id": r["Id"],
                "name": r["Name"],
                "Name": r["Name"],
                "code": r["Code"],
                "Code": r["Code"]
            })

    return list(grouped.values())


@router.post("")
def create_setting(data: SettingCreate, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'INSERT INTO public."Settings" ("Name","Code","SettingGroupId","CreationDate","Deleted") VALUES (%s,%s,%s,NOW(),FALSE) RETURNING "Id"',
            (data.name, data.code, data.setting_group_id),
        )
        new_id = cur.fetchone()["Id"]
        conn.commit()
    return {"id": new_id, "message": "Catálogo creado"}


@router.put("/{setting_id}")
def update_setting(setting_id: int, data: SettingUpdate, _: dict = Depends(get_current_admin)):
    """
    Actualiza el nombre del catálogo.
    El cambio se refleja automáticamente en todos los usuarios que lo tienen
    seleccionado porque UserProfiles almacena el FK (Id), no el nombre.
    """
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'UPDATE public."Settings" SET "Name"=%s, "Code"=%s WHERE "Id"=%s',
            (data.name, data.code, setting_id),
        )
        conn.commit()
    return {"message": "Catálogo actualizado. El cambio se refleja en todos los usuarios asociados."}


@router.delete("/{setting_id}")
def delete_setting(setting_id: int, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('UPDATE public."Settings" SET "Deleted"=TRUE WHERE "Id"=%s', (setting_id,))
        conn.commit()
    return {"message": "Catálogo eliminado"}
