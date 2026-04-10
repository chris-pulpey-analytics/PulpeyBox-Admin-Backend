from fastapi import APIRouter, Depends
from typing import Optional, List
from pydantic import BaseModel
from shapely.geometry import Point, Polygon
from app.api.auth import get_current_admin
from app.core.database import get_db

router = APIRouter(prefix="/map", tags=["map"])


class AreaQuery(BaseModel):
    coordinates: List[List[float]]  # [[lng, lat], [lng, lat], ...]
    color_by: Optional[str] = None  # gender, age_range, department, profession


@router.get("/points")
def get_map_points(
    gender_id: Optional[int] = None,
    department_id: Optional[int] = None,
    city_id: Optional[int] = None,
    age_min: Optional[int] = None,
    age_max: Optional[int] = None,
    registered_from: Optional[str] = None,
    registered_to: Optional[str] = None,
    _: dict = Depends(get_current_admin),
):
    """Retorna puntos lat/lng de usuarios con datos básicos de perfil para el mapa."""
    conditions = [
        'u."Deleted" IS DISTINCT FROM TRUE',
        'up."Latitude" IS NOT NULL',
        'up."Longitude" IS NOT NULL',
        "up.\"Latitude\" != ''",
        "up.\"Longitude\" != ''",
    ]
    params = []

    if gender_id:
        conditions.append('up."GenderId" = %s')
        params.append(gender_id)
    if department_id:
        conditions.append('d."Id" = %s')
        params.append(department_id)
    if city_id:
        conditions.append('up."CityId" = %s')
        params.append(city_id)
    if age_min is not None:
        conditions.append("EXTRACT(YEAR FROM AGE(up.\"BirthDate\")) >= %s")
        params.append(age_min)
    if age_max is not None:
        conditions.append("EXTRACT(YEAR FROM AGE(up.\"BirthDate\")) <= %s")
        params.append(age_max)
    if registered_from:
        conditions.append('up."CreationDate"::date >= %s')
        params.append(registered_from)
    if registered_to:
        conditions.append('up."CreationDate"::date <= %s')
        params.append(registered_to)

    where = "WHERE " + " AND ".join(conditions)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                u."Id",
                COALESCE(u."Name", '') || ' ' || COALESCE(u."LastName", '') AS full_name,
                up."Latitude"::float AS lat,
                up."Longitude"::float AS lng,
                COALESCE(gender."Name", 'Sin dato') AS gender,
                COALESCE(d."DepartmentName", 'Sin dato') AS department,
                COALESCE(c."CityName", 'Sin dato') AS city,
                COALESCE(profession."Name", 'Sin dato') AS profession,
                CASE
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 18 AND 27 THEN '18-27'
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 28 AND 37 THEN '28-37'
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 38 AND 47 THEN '38-47'
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) > 47 THEN '47+'
                    ELSE 'Sin dato'
                END AS age_range,
                TO_CHAR(up."CreationDate", 'DD/MM/YYYY') AS registration_date
            FROM public."Users" u
            JOIN public."UserProfiles" up ON u."Id" = up."UserId"
            LEFT JOIN public."Cities" c ON up."CityId" = c."Id"
            LEFT JOIN public."Departments" d ON c."DepartmentId" = d."Id"
            LEFT JOIN public."Settings" gender ON up."GenderId" = gender."Id"
            LEFT JOIN public."Settings" profession ON up."ProfessionsId" = profession."Id"
            {where}
            ORDER BY u."Id"
            """,
            params,
        )
        rows = cur.fetchall()

    points = []
    for r in rows:
        try:
            lat = float(r["lat"]) if r["lat"] else None
            lng = float(r["lng"]) if r["lng"] else None
            if lat and lng and -90 <= lat <= 90 and -180 <= lng <= 180:
                points.append({
                    "id": r["Id"],
                    "name": r["full_name"].strip(),
                    "lat": lat,
                    "lng": lng,
                    "gender": r["gender"],
                    "department": r["department"],
                    "city": r["city"],
                    "profession": r["profession"],
                    "age_range": r["age_range"],
                    "registration_date": r["registration_date"],
                })
        except (ValueError, TypeError):
            continue

    return {"total": len(points), "points": points}


@router.post("/area")
def query_area(body: AreaQuery, _: dict = Depends(get_current_admin)):
    """Retorna usuarios dentro de un polígono dibujado en el mapa y sus stats demográficas."""
    if len(body.coordinates) < 3:
        return {"users": [], "stats": {}}

    polygon = Polygon([(c[0], c[1]) for c in body.coordinates])

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                u."Id",
                COALESCE(u."Name", '') || ' ' || COALESCE(u."LastName", '') AS full_name,
                u."Email",
                u."MobilNumber",
                up."Latitude"::float AS lat,
                up."Longitude"::float AS lng,
                COALESCE(gender."Name", 'Sin dato') AS gender,
                COALESCE(d."DepartmentName", 'Sin dato') AS department,
                COALESCE(c."CityName", 'Sin dato') AS city,
                COALESCE(profession."Name", 'Sin dato') AS profession,
                CASE
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 18 AND 27 THEN '18-27'
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 28 AND 37 THEN '28-37'
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 38 AND 47 THEN '38-47'
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) > 47 THEN '47+'
                    ELSE 'Sin dato'
                END AS age_range,
                TO_CHAR(up."CreationDate", 'DD/MM/YYYY') AS registration_date
            FROM public."Users" u
            JOIN public."UserProfiles" up ON u."Id" = up."UserId"
            LEFT JOIN public."Cities" c ON up."CityId" = c."Id"
            LEFT JOIN public."Departments" d ON c."DepartmentId" = d."Id"
            LEFT JOIN public."Settings" gender ON up."GenderId" = gender."Id"
            LEFT JOIN public."Settings" profession ON up."ProfessionsId" = profession."Id"
            WHERE u."Deleted" IS DISTINCT FROM TRUE
              AND up."Latitude" IS NOT NULL AND up."Longitude" IS NOT NULL
              AND up."Latitude" != '' AND up."Longitude" != ''
            """
        )
        all_rows = cur.fetchall()

    users_in_area = []
    for r in all_rows:
        try:
            lat = float(r["lat"])
            lng = float(r["lng"])
            if polygon.contains(Point(lng, lat)):
                users_in_area.append(dict(r))
        except (ValueError, TypeError):
            continue

    # Calcular stats demográficas del área
    gender_counts = {}
    age_counts = {}
    dept_counts = {}
    profession_counts = {}

    for u in users_in_area:
        gender_counts[u["gender"]] = gender_counts.get(u["gender"], 0) + 1
        age_counts[u["age_range"]] = age_counts.get(u["age_range"], 0) + 1
        dept_counts[u["department"]] = dept_counts.get(u["department"], 0) + 1
        profession_counts[u["profession"]] = profession_counts.get(u["profession"], 0) + 1

    def to_chart(d):
        return [{"name": k, "value": v} for k, v in sorted(d.items(), key=lambda x: -x[1])]

    return {
        "total": len(users_in_area),
        "users": users_in_area[:200],  # máx 200 para el panel
        "stats": {
            "gender": to_chart(gender_counts),
            "age_range": to_chart(age_counts),
            "department": to_chart(dept_counts)[:10],
            "profession": to_chart(profession_counts)[:10],
        },
    }
