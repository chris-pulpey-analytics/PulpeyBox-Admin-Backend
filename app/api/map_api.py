from fastapi import APIRouter, Depends, Query
from typing import Optional, List
from pydantic import BaseModel
from shapely.geometry import Point, Polygon
from app.api.auth import get_current_admin
from app.core.database import get_db

router = APIRouter(prefix="/map", tags=["map"])


class AreaQuery(BaseModel):
    coordinates: List[List[float]]
    color_by: Optional[str] = None


def _build_map_conditions(
    gender_id, marital_status_id, income_range_id, profession_id,
    number_children_id, level_academic_id, frequency_activities_id,
    department_id, city_id, zone,
    age_min, age_max,
    is_buy_manager_home, is_pregnant, is_interested_technology,
    is_alcohol_consume, is_tobacco_consume,
    registered_from, registered_to,
    survey_id, survey_status_id,
):
    conditions = [
        'u."Deleted" IS DISTINCT FROM TRUE',
        'up."Latitude" IS NOT NULL',
        'up."Longitude" IS NOT NULL',
        "up.\"Latitude\" != ''",
        "up.\"Longitude\" != ''",
    ]
    params = []

    if gender_id:
        conditions.append('up."GenderId"=%s'); params.append(gender_id)
    if marital_status_id:
        conditions.append('up."MaritalStatusId"=%s'); params.append(marital_status_id)
    if income_range_id:
        conditions.append('up."IncomeRangeId"=%s'); params.append(income_range_id)
    if profession_id:
        conditions.append('up."ProfessionsId"=%s'); params.append(profession_id)
    if number_children_id:
        conditions.append('up."NumberChildrenId"=%s'); params.append(number_children_id)
    if level_academic_id:
        conditions.append('up."LevelAcademicId"=%s'); params.append(level_academic_id)
    if frequency_activities_id:
        conditions.append('up."FrequencyActivitiesPhysicalId"=%s'); params.append(frequency_activities_id)
    if department_id:
        conditions.append('d."Id"=%s'); params.append(department_id)
    if city_id:
        conditions.append('up."CityId"=%s'); params.append(city_id)
    if zone is not None:
        conditions.append('up."Zone"=%s'); params.append(zone)
    if age_min is not None:
        conditions.append('EXTRACT(YEAR FROM AGE(up."BirthDate")) >= %s'); params.append(age_min)
    if age_max is not None:
        conditions.append('EXTRACT(YEAR FROM AGE(up."BirthDate")) <= %s'); params.append(age_max)
    if is_buy_manager_home is not None:
        conditions.append('up."IsBuyManagerHome"=%s'); params.append(is_buy_manager_home)
    if is_pregnant is not None:
        conditions.append('up."IsPregnant"=%s'); params.append(is_pregnant)
    if is_interested_technology is not None:
        conditions.append('up."IsInterestedTechnology"=%s'); params.append(is_interested_technology)
    if is_alcohol_consume is not None:
        conditions.append('up."IsAlcoholConsume"=%s'); params.append(is_alcohol_consume)
    if is_tobacco_consume is not None:
        conditions.append('up."IsTobaccoConsume"=%s'); params.append(is_tobacco_consume)
    if registered_from:
        conditions.append('up."CreationDate"::date >= %s'); params.append(registered_from)
    if registered_to:
        conditions.append('up."CreationDate"::date <= %s'); params.append(registered_to)
    if survey_id and survey_status_id:
        conditions.append(
            'EXISTS (SELECT 1 FROM public."UserSurveys" us WHERE us."UserId"=u."Id" AND us."SurveyId"=%s AND us."StatusId"=%s)'
        )
        params.extend([survey_id, survey_status_id])
    elif survey_id:
        conditions.append(
            'EXISTS (SELECT 1 FROM public."UserSurveys" us WHERE us."UserId"=u."Id" AND us."SurveyId"=%s)'
        )
        params.append(survey_id)

    return conditions, params


MAP_SELECT = """
    SELECT
        u."Id",
        COALESCE(u."Name",'') || ' ' || COALESCE(u."LastName",'') AS full_name,
        up."Latitude"::float AS lat,
        up."Longitude"::float AS lng,
        COALESCE(gender."Name",'Sin dato') AS gender,
        COALESCE(d."DepartmentName",'Sin dato') AS department,
        COALESCE(c."CityName",'Sin dato') AS city,
        COALESCE(profession."Name",'Sin dato') AS profession,
        COALESCE(marital."Name",'Sin dato') AS marital_status,
        COALESCE(income."Name",'Sin dato') AS income_range,
        CASE
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 18 AND 27 THEN '18-27'
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 28 AND 37 THEN '28-37'
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 38 AND 47 THEN '38-47'
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) > 47 THEN '47+'
            ELSE 'Sin dato'
        END AS age_range,
        TO_CHAR(up."CreationDate",'DD/MM/YYYY') AS registration_date
"""

MAP_FROM = """
    FROM public."Users" u
    JOIN public."UserProfiles" up ON u."Id"=up."UserId"
    LEFT JOIN public."Cities" c ON up."CityId"=c."Id"
    LEFT JOIN public."Departments" d ON c."DepartmentId"=d."Id"
    LEFT JOIN public."Settings" gender ON up."GenderId"=gender."Id"
    LEFT JOIN public."Settings" profession ON up."ProfessionsId"=profession."Id"
    LEFT JOIN public."Settings" marital ON up."MaritalStatusId"=marital."Id"
    LEFT JOIN public."Settings" income ON up."IncomeRangeId"=income."Id"
"""


@router.get("/points")
def get_map_points(
    gender_id: Optional[int] = None,
    marital_status_id: Optional[int] = None,
    income_range_id: Optional[int] = None,
    profession_id: Optional[int] = None,
    number_children_id: Optional[int] = None,
    level_academic_id: Optional[int] = None,
    frequency_activities_id: Optional[int] = None,
    department_id: Optional[int] = None,
    city_id: Optional[int] = None,
    zone: Optional[int] = None,
    age_min: Optional[int] = None,
    age_max: Optional[int] = None,
    is_buy_manager_home: Optional[bool] = None,
    is_pregnant: Optional[bool] = None,
    is_interested_technology: Optional[bool] = None,
    is_alcohol_consume: Optional[bool] = None,
    is_tobacco_consume: Optional[bool] = None,
    registered_from: Optional[str] = None,
    registered_to: Optional[str] = None,
    survey_id: Optional[int] = None,
    survey_status_id: Optional[int] = None,
    _: dict = Depends(get_current_admin),
):
    conditions, params = _build_map_conditions(
        gender_id, marital_status_id, income_range_id, profession_id,
        number_children_id, level_academic_id, frequency_activities_id,
        department_id, city_id, zone,
        age_min, age_max,
        is_buy_manager_home, is_pregnant, is_interested_technology,
        is_alcohol_consume, is_tobacco_consume,
        registered_from, registered_to,
        survey_id, survey_status_id,
    )
    where = "WHERE " + " AND ".join(conditions)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(f"{MAP_SELECT} {MAP_FROM} {where} ORDER BY u.\"Id\"", params)
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
                    "lat": lat, "lng": lng,
                    "gender": r["gender"],
                    "department": r["department"],
                    "city": r["city"],
                    "profession": r["profession"],
                    "marital_status": r["marital_status"],
                    "income_range": r["income_range"],
                    "age_range": r["age_range"],
                    "registration_date": r["registration_date"],
                })
        except (ValueError, TypeError):
            continue

    return {"total": len(points), "points": points}


@router.post("/area")
def query_area(body: AreaQuery, _: dict = Depends(get_current_admin)):
    if len(body.coordinates) < 3:
        return {"users": [], "stats": {}}

    polygon = Polygon([(c[0], c[1]) for c in body.coordinates])

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            {MAP_SELECT}, u."Email", u."MobilNumber"
            {MAP_FROM}
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

    gender_counts, age_counts, dept_counts, profession_counts, income_counts = {}, {}, {}, {}, {}
    for u in users_in_area:
        for d, k in [(gender_counts, "gender"), (age_counts, "age_range"),
                     (dept_counts, "department"), (profession_counts, "profession"),
                     (income_counts, "income_range")]:
            d[u[k]] = d.get(u[k], 0) + 1

    def to_chart(d):
        return [{"name": k, "value": v} for k, v in sorted(d.items(), key=lambda x: -x[1])]

    return {
        "total": len(users_in_area),
        "users": users_in_area[:200],
        "stats": {
            "gender": to_chart(gender_counts),
            "age_range": to_chart(age_counts),
            "department": to_chart(dept_counts)[:10],
            "profession": to_chart(profession_counts)[:10],
            "income_range": to_chart(income_counts)[:8],
        },
    }
