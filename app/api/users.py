import io
import pandas as pd
from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import StreamingResponse
from typing import Optional
from datetime import date
from app.api.auth import get_current_admin
from app.core.database import get_db
from app.schemas.user import UserFilters

router = APIRouter(prefix="/users", tags=["users"])

BASE_SELECT = """
    SELECT
        u."Id",
        COALESCE(u."Name", '-') AS "Nombre",
        COALESCE(u."LastName", '-') AS "Apellido",
        COALESCE(u."MobilNumber", '-') AS "Teléfono",
        COALESCE(u."Email", '-') AS "Correo Electrónico",
        COALESCE(
            NULLIF(
                REGEXP_REPLACE(
                    COALESCE(up."Instagram", ''),
                    '^(?:https?://)?(?:www\\.)?instagram\\.com/|@|/$',
                    '', 'gi'
                ), ''
            ), '-'
        ) AS "Usuario Instagram",
        COALESCE(TO_CHAR(up."BirthDate", 'DD/MM/YYYY'), '-') AS "Fecha de Nacimiento",
        COALESCE(EXTRACT(YEAR FROM AGE(up."BirthDate"))::TEXT, '-') AS "Edad",
        CASE
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 18 AND 22 THEN '18-22'
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 23 AND 27 THEN '23-27'
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 28 AND 32 THEN '28-32'
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 33 AND 35 THEN '33-35'
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 36 AND 40 THEN '36-40'
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 41 AND 45 THEN '41-45'
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 46 AND 50 THEN '46-50'
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 51 AND 55 THEN '51-55'
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 56 AND 60 THEN '56-60'
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 61 AND 65 THEN '61-65'
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) > 65 THEN '65+'
            ELSE 'Sin Dato'
        END AS "Rango de Edad",
        COALESCE(gender."Name", '-') AS "Género",
        COALESCE(marital_status."Name", '-') AS "Estado Civil",
        COALESCE(role_house."Name", '-') AS "Rol Familiar",
        COALESCE(income_range."Name", '-') AS "Rango de Ingreso",
        COALESCE(profession."Name", '-') AS "Profesión",
        COALESCE(STRING_AGG(DISTINCT pet."Name", ' - '), '-') AS "Mascotas",
        COALESCE(STRING_AGG(DISTINCT hobby."Name", ' - '), '-') AS "Hobbies",
        COALESCE(frequency_activities."Name", '-') AS "Frecuencia Actividad Física",
        COALESCE(number_children."Name", '-') AS "Número de Hijos",
        COALESCE(level_academic."Name", '-') AS "Nivel Académico",
        CASE WHEN up."IsBuyManagerHome" = TRUE THEN 'SI' WHEN up."IsBuyManagerHome" = FALSE THEN 'NO' ELSE '-' END AS "Compras en el Hogar",
        CASE WHEN up."IsPregnant" = TRUE THEN 'SI' WHEN up."IsPregnant" = FALSE THEN 'NO' ELSE '-' END AS "Embarazo",
        CASE WHEN up."IsInterestedTechnology" = TRUE THEN 'SI' WHEN up."IsInterestedTechnology" = FALSE THEN 'NO' ELSE '-' END AS "Interesado en Tecnología",
        CASE WHEN up."IsAlcoholConsume" = TRUE THEN 'SI' WHEN up."IsAlcoholConsume" = FALSE THEN 'NO' ELSE '-' END AS "Consume Alcohol",
        CASE WHEN up."IsTobaccoConsume" = TRUE THEN 'SI' WHEN up."IsTobaccoConsume" = FALSE THEN 'NO' ELSE '-' END AS "Consume Nicotina",
        REGEXP_REPLACE(COALESCE(NULLIF(up."Address", ''), '-'), '[\\n,;]', '-', 'g') AS "Dirección",
        REGEXP_REPLACE(COALESCE(NULLIF(up."ExactAddress", ''), '-'), '[\\n,;]', '-', 'g') AS "Dirección Exacta",
        REGEXP_REPLACE(COALESCE(NULLIF(up."Instruction", ''), '-'), '[\\n,;]', '-', 'g') AS "Indicaciones",
        COALESCE(up."Zone"::TEXT, '-') AS "Zona",
        'Guatemala' AS "País",
        COALESCE(d."DepartmentName", '-') AS "Departamento",
        COALESCE(c."CityName", '-') AS "Municipio",
        COALESCE(up."Latitude"::TEXT, '-') AS "Latitud",
        COALESCE(up."Longitude"::TEXT, '-') AS "Longitud",
        COALESCE(TO_CHAR(up."CreationDate", 'DD/MM/YYYY'), '-') AS "Fecha de Registro",
        COALESCE(TO_CHAR(up."LastUserProfileDate", 'DD/MM/YYYY'), '-') AS "Último Perfil",
        COALESCE(TO_CHAR(u."LastSession", 'DD/MM/YYYY'), '-') AS "Última Sesión",
        COALESCE(u."IsMigrated"::TEXT, '-') AS "Migrado"
"""

BASE_FROM = """
    FROM public."Users" u
    JOIN public."UserProfiles" up ON u."Id" = up."UserId"
    LEFT JOIN public."Cities" c ON up."CityId" = c."Id"
    LEFT JOIN public."Departments" d ON c."DepartmentId" = d."Id"
    LEFT JOIN public."Settings" gender ON up."GenderId" = gender."Id"
    LEFT JOIN public."Settings" marital_status ON up."MaritalStatusId" = marital_status."Id"
    LEFT JOIN public."Settings" role_house ON up."RoleHouseId" = role_house."Id"
    LEFT JOIN public."Settings" income_range ON up."IncomeRangeId" = income_range."Id"
    LEFT JOIN public."Settings" profession ON up."ProfessionsId" = profession."Id"
    LEFT JOIN public."Settings" frequency_activities ON up."FrequencyActivitiesPhysicalId" = frequency_activities."Id"
    LEFT JOIN public."Settings" pet ON pet."Id" = ANY(string_to_array(up."PetsId", ',')::int[])
    LEFT JOIN public."Settings" hobby ON hobby."Id" = ANY(string_to_array(up."HobbiesId", ',')::int[])
    LEFT JOIN public."Settings" number_children ON up."NumberChildrenId" = number_children."Id"
    LEFT JOIN public."Settings" level_academic ON up."LevelAcademicId" = level_academic."Id"
"""

BASE_GROUP = """
    GROUP BY
        u."Id", u."Name", u."LastName", u."MobilNumber", u."Email",
        up."Instagram", up."BirthDate",
        gender."Name", marital_status."Name", role_house."Name",
        income_range."Name", profession."Name", frequency_activities."Name",
        up."Address", up."ExactAddress", up."Instruction", up."Zone",
        d."DepartmentName", c."CityName",
        up."Latitude", up."Longitude", up."CreationDate",
        up."LastUserProfileDate", u."LastSession", u."IsMigrated",
        up."IsBuyManagerHome", up."IsPregnant", up."IsInterestedTechnology",
        up."IsAlcoholConsume", up."IsTobaccoConsume",
        number_children."Name", level_academic."Name"
"""


def build_where(filters: UserFilters):
    conditions = ["u.\"Deleted\" IS DISTINCT FROM TRUE"]
    params = []

    if filters.search:
        conditions.append(
            """(
                u."Name" ILIKE %s OR u."LastName" ILIKE %s OR
                u."Email" ILIKE %s OR u."MobilNumber" ILIKE %s OR
                CONCAT(u."Name", ' ', u."LastName") ILIKE %s
            )"""
        )
        term = f"%{filters.search}%"
        params.extend([term, term, term, term, term])

    if filters.user_ids:
        ids = [int(i.strip()) for i in filters.user_ids.split(",") if i.strip().isdigit()]
        if ids:
            conditions.append(f'u."Id" = ANY(%s)')
            params.append(ids)

    if filters.emails:
        emails = [e.strip().lower() for e in filters.emails.split(",") if e.strip()]
        if emails:
            conditions.append('LOWER(u."Email") = ANY(%s)')
            params.append(emails)

    if filters.phones:
        phones = [p.strip() for p in filters.phones.split(",") if p.strip()]
        if phones:
            conditions.append('u."MobilNumber" = ANY(%s)')
            params.append(phones)

    if filters.instagram:
        conditions.append('up."Instagram" ILIKE %s')
        params.append(f"%{filters.instagram}%")

    if filters.gender_id:
        conditions.append('up."GenderId" = %s')
        params.append(filters.gender_id)

    if filters.marital_status_id:
        conditions.append('up."MaritalStatusId" = %s')
        params.append(filters.marital_status_id)

    if filters.role_house_id:
        conditions.append('up."RoleHouseId" = %s')
        params.append(filters.role_house_id)

    if filters.income_range_id:
        conditions.append('up."IncomeRangeId" = %s')
        params.append(filters.income_range_id)

    if filters.profession_id:
        conditions.append('up."ProfessionsId" = %s')
        params.append(filters.profession_id)

    if filters.number_children_id:
        conditions.append('up."NumberChildrenId" = %s')
        params.append(filters.number_children_id)

    if filters.level_academic_id:
        conditions.append('up."LevelAcademicId" = %s')
        params.append(filters.level_academic_id)

    if filters.frequency_activities_id:
        conditions.append('up."FrequencyActivitiesPhysicalId" = %s')
        params.append(filters.frequency_activities_id)

    if filters.age_min is not None:
        conditions.append("EXTRACT(YEAR FROM AGE(up.\"BirthDate\")) >= %s")
        params.append(filters.age_min)

    if filters.age_max is not None:
        conditions.append("EXTRACT(YEAR FROM AGE(up.\"BirthDate\")) <= %s")
        params.append(filters.age_max)

    if filters.is_buy_manager_home is not None:
        conditions.append('up."IsBuyManagerHome" = %s')
        params.append(filters.is_buy_manager_home)

    if filters.is_pregnant is not None:
        conditions.append('up."IsPregnant" = %s')
        params.append(filters.is_pregnant)

    if filters.is_interested_technology is not None:
        conditions.append('up."IsInterestedTechnology" = %s')
        params.append(filters.is_interested_technology)

    if filters.is_alcohol_consume is not None:
        conditions.append('up."IsAlcoholConsume" = %s')
        params.append(filters.is_alcohol_consume)

    if filters.is_tobacco_consume is not None:
        conditions.append('up."IsTobaccoConsume" = %s')
        params.append(filters.is_tobacco_consume)

    if filters.department_id:
        conditions.append('d."Id" = %s')
        params.append(filters.department_id)

    if filters.city_id:
        conditions.append('up."CityId" = %s')
        params.append(filters.city_id)

    if filters.zone is not None:
        conditions.append('up."Zone" = %s')
        params.append(filters.zone)

    if filters.registered_from:
        conditions.append('up."CreationDate"::date >= %s')
        params.append(filters.registered_from)

    if filters.registered_to:
        conditions.append('up."CreationDate"::date <= %s')
        params.append(filters.registered_to)

    if filters.last_session_from:
        conditions.append('u."LastSession"::date >= %s')
        params.append(filters.last_session_from)

    if filters.last_session_to:
        conditions.append('u."LastSession"::date <= %s')
        params.append(filters.last_session_to)

    if filters.profile_updated_from:
        conditions.append('up."LastUserProfileDate"::date >= %s')
        params.append(filters.profile_updated_from)

    if filters.survey_id and filters.survey_status_id:
        conditions.append(
            'EXISTS (SELECT 1 FROM public."UserSurveys" us WHERE us."UserId" = u."Id" AND us."SurveyId" = %s AND us."StatusId" = %s)'
        )
        params.extend([filters.survey_id, filters.survey_status_id])
    elif filters.survey_id:
        conditions.append(
            'EXISTS (SELECT 1 FROM public."UserSurveys" us WHERE us."UserId" = u."Id" AND us."SurveyId" = %s)'
        )
        params.append(filters.survey_id)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    return where, params


@router.get("")
def get_users(
    filters: UserFilters = Depends(),
    _: dict = Depends(get_current_admin),
):
    where, params = build_where(filters)
    offset = (filters.page - 1) * filters.page_size

    count_sql = f'SELECT COUNT(DISTINCT u."Id") {BASE_FROM} {where}'
    data_sql = (
        f"SELECT * FROM (SELECT DISTINCT ON (u.\"Id\") {BASE_SELECT} {BASE_FROM} {where} {BASE_GROUP}) sub "
        f"ORDER BY sub.\"Id\" DESC LIMIT %s OFFSET %s"
    )

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(count_sql, params)
        total = cur.fetchone()["count"]

        cur.execute(data_sql, params + [filters.page_size, offset])
        rows = cur.fetchall()

    return {
        "total": total,
        "page": filters.page,
        "page_size": filters.page_size,
        "data": [dict(r) for r in rows],
    }


@router.get("/export")
def export_users(
    filters: UserFilters = Depends(),
    format: str = Query("xlsx", pattern="^(xlsx|csv)$"),
    _: dict = Depends(get_current_admin),
):
    where, params = build_where(filters)
    sql = f"SELECT {BASE_SELECT} {BASE_FROM} {where} {BASE_GROUP} ORDER BY u.\"Id\" DESC"

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()

    df = pd.DataFrame([dict(r) for r in rows])

    if format == "csv":
        output = io.StringIO()
        df.to_csv(output, index=False, encoding="utf-8-sig")
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=usuarios.csv"},
        )

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Usuarios", index=False)
    output.seek(0)
    return StreamingResponse(
        iter([output.read()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=usuarios.xlsx"},
    )


@router.get("/{user_id}")
def get_user(user_id: int, _: dict = Depends(get_current_admin)):
    sql = f"SELECT {BASE_SELECT} {BASE_FROM} WHERE u.\"Id\" = %s {BASE_GROUP}"
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(sql, (user_id,))
        user = cur.fetchone()
        if not user:
            raise HTTPException(404, "Usuario no encontrado")

        cur.execute(
            """
            SELECT us."Id", us."SurveyId", us."StatusId", us."AnsweredDate", us."AnswersJson",
                   st."Name" AS status_name
            FROM public."UserSurveys" us
            LEFT JOIN public."Settings" st ON us."StatusId" = st."Id"
            WHERE us."UserId" = %s
            ORDER BY us."AnsweredDate" DESC NULLS LAST
            """,
            (user_id,),
        )
        surveys = [dict(r) for r in cur.fetchall()]

        cur.execute(
            """
            SELECT unp."Id", unp."NewsAndPromotionId", unp."Status",
                   unp."ClickCount", unp."FirstClickDate", unp."LastClickDate"
            FROM public."UserNewsAndPromotions" unp
            WHERE unp."UserId" = %s
            ORDER BY unp."LastClickDate" DESC NULLS LAST
            """,
            (user_id,),
        )
        news = [dict(r) for r in cur.fetchall()]

    return {"user": dict(user), "surveys": surveys, "news": news}
