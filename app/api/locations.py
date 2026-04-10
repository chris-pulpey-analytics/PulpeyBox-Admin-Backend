from fastapi import APIRouter, Depends, HTTPException
from typing import Optional
from pydantic import BaseModel
from app.api.auth import get_current_admin
from app.core.database import get_db

router = APIRouter(prefix="/locations", tags=["locations"])


class DepartmentCreate(BaseModel):
    name: str
    code: str


class CityCreate(BaseModel):
    name: str
    code: Optional[str] = None
    department_id: int


@router.get("/departments")
def list_departments(_: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT d."Id", d."DepartmentName", d."Code",
                   COUNT(c."Id") AS city_count
            FROM public."Departments" d
            LEFT JOIN public."Cities" c ON c."DepartmentId" = d."Id"
                AND c."Deleted" IS DISTINCT FROM TRUE
            WHERE d."Deleted" IS DISTINCT FROM TRUE
            GROUP BY d."Id", d."DepartmentName", d."Code"
            ORDER BY d."DepartmentName"
            """
        )
        return [dict(r) for r in cur.fetchall()]


@router.post("/departments")
def create_department(data: DepartmentCreate, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'INSERT INTO public."Departments" ("DepartmentName","Code","CreationDate","Deleted") VALUES (%s,%s,NOW(),FALSE) RETURNING "Id"',
            (data.name, data.code),
        )
        new_id = cur.fetchone()["Id"]
        conn.commit()
    return {"id": new_id, "message": "Departamento creado"}


@router.put("/departments/{dept_id}")
def update_department(dept_id: int, data: DepartmentCreate, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'UPDATE public."Departments" SET "DepartmentName"=%s,"Code"=%s WHERE "Id"=%s',
            (data.name, data.code, dept_id),
        )
        conn.commit()
    return {"message": "Departamento actualizado"}


@router.delete("/departments/{dept_id}")
def delete_department(dept_id: int, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM public."Cities" WHERE "DepartmentId"=%s AND "Deleted" IS DISTINCT FROM TRUE', (dept_id,))
        if cur.fetchone()["count"] > 0:
            raise HTTPException(400, "El departamento tiene municipios activos")
        cur.execute('UPDATE public."Departments" SET "Deleted"=TRUE WHERE "Id"=%s', (dept_id,))
        conn.commit()
    return {"message": "Departamento eliminado"}


@router.get("/cities")
def list_cities(department_id: Optional[int] = None, _: dict = Depends(get_current_admin)):
    conditions = ['c."Deleted" IS DISTINCT FROM TRUE']
    params = []
    if department_id:
        conditions.append('c."DepartmentId"=%s')
        params.append(department_id)
    where = "WHERE " + " AND ".join(conditions)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT c."Id", c."CityName", c."Code", c."DepartmentId",
                   d."DepartmentName"
            FROM public."Cities" c
            JOIN public."Departments" d ON c."DepartmentId" = d."Id"
            {where}
            ORDER BY d."DepartmentName", c."CityName"
            """,
            params,
        )
        return [dict(r) for r in cur.fetchall()]


@router.post("/cities")
def create_city(data: CityCreate, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'INSERT INTO public."Cities" ("CityName","Code","DepartmentId","CreationDate","Deleted") VALUES (%s,%s,%s,NOW(),FALSE) RETURNING "Id"',
            (data.name, data.code, data.department_id),
        )
        new_id = cur.fetchone()["Id"]
        conn.commit()
    return {"id": new_id, "message": "Municipio creado"}


@router.put("/cities/{city_id}")
def update_city(city_id: int, data: CityCreate, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'UPDATE public."Cities" SET "CityName"=%s,"Code"=%s,"DepartmentId"=%s WHERE "Id"=%s',
            (data.name, data.code, data.department_id, city_id),
        )
        conn.commit()
    return {"message": "Municipio actualizado"}


@router.delete("/cities/{city_id}")
def delete_city(city_id: int, _: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('UPDATE public."Cities" SET "Deleted"=TRUE WHERE "Id"=%s', (city_id,))
        conn.commit()
    return {"message": "Municipio eliminado"}
