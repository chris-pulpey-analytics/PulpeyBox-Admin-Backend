# -*- coding: utf-8 -*-
import io
import json as _json
import unicodedata
import re
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from typing import Optional, List
from datetime import date, datetime
from pydantic import BaseModel
from psycopg2.extras import execute_values
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


@router.get("/categories")
def list_categories(_: dict = Depends(get_current_admin)):
    """Retorna todas las categorías activas de la tabla Categories."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """SELECT "Id", "Name"
               FROM public."Categories"
               WHERE "Deleted" IS DISTINCT FROM TRUE
               ORDER BY "Name" """
        )
        return [dict(r) for r in cur.fetchall()]


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


class InternalReportRequest(BaseModel):
    survey_ids: List[int]
    status_id: Optional[int] = None


def _clean_sheet_name(name: str) -> str:
    name = unicodedata.normalize("NFKD", str(name))
    name = name.encode("ASCII", "ignore").decode("ASCII")
    name = re.sub(r"[^\w\-_]", "_", name)
    return name[:31]


@router.post("/internal-report")
def generate_internal_report(
    data: InternalReportRequest,
    _: dict = Depends(get_current_admin),
):
    """
    Genera un reporte Excel multi-encuesta con el mismo formato que
    Reporte_Surveys_Internos.py:
    - Hoja 'Usuarios': datos de perfil de todos los usuarios únicos
    - Hoja 'Survey_{id}_{nombre}': preguntas como columnas, respuestas como valores
    - Hoja 'Surveys': info y estadísticas por encuesta
    - Hoja 'Resumen': métricas generales
    """
    if not data.survey_ids:
        raise HTTPException(400, "Debe especificar al menos un survey_id")

    survey_ids = data.survey_ids
    status_id = data.status_id
    ids_tuple = tuple(survey_ids)

    with get_db() as conn:
        cur = conn.cursor()

        # ── 1. Validar encuestas ─────────────────────────────────────────────
        val_status_cond = 'AND us."StatusId" = %s' if status_id else ""
        val_params: list = [ids_tuple]
        if status_id:
            val_params.append(status_id)
        val_params.append(ids_tuple)

        cur.execute(
            f"""
            WITH usp AS (
                SELECT us."SurveyId",
                    COUNT(DISTINCT us."UserId") AS usuarios_unicos,
                    COUNT(us."Id")              AS participaciones
                FROM public."UserSurveys" us
                JOIN public."Surveys" s ON us."SurveyId" = s."Id" AND s."Deleted" = false
                WHERE us."SurveyId" IN %s AND us."Deleted" = false {val_status_cond}
                GROUP BY us."SurveyId"
            )
            SELECT s."Id", s."Name",
                COALESCE(usp.usuarios_unicos, 0) AS usuarios_unicos,
                COALESCE(usp.participaciones, 0)  AS participaciones
            FROM public."Surveys" s
            LEFT JOIN usp ON s."Id" = usp."SurveyId"
            WHERE s."Id" IN %s AND s."Deleted" = false
            """,
            val_params,
        )
        val_rows = {r["Id"]: dict(r) for r in cur.fetchall()}

        surveys_validas = [
            val_rows[sid]
            for sid in survey_ids
            if sid in val_rows and val_rows[sid]["usuarios_unicos"] > 0
        ]

        if not surveys_validas:
            raise HTTPException(
                404,
                "No se encontraron usuarios para las encuestas seleccionadas. "
                "Verifica que los IDs sean correctos y que tengan usuarios asignados.",
            )

        valid_ids = tuple(r["Id"] for r in surveys_validas)

        # ── 2. Nombre del status ─────────────────────────────────────────────
        status_name = "Todos"
        if status_id:
            cur.execute(
                'SELECT "Name" FROM public."Settings" WHERE "Id" = %s', (status_id,)
            )
            st = cur.fetchone()
            if st:
                status_name = st["Name"]

        # ── 3. Query principal ───────────────────────────────────────────────
        us_cond  = 'AND us2."StatusId" = %s' if status_id else ""
        sub_cond = 'AND us."StatusId"  = %s' if status_id else ""

        main_sql = f"""
        WITH usuarios_survey AS (
            SELECT DISTINCT us2."UserId"
            FROM public."UserSurveys" us2
            WHERE us2."SurveyId" IN %s AND us2."Deleted" = false {us_cond}
        )
        SELECT
            u."Id",
            COALESCE(u."Name", '-')                                             AS "Nombre",
            COALESCE(u."LastName", '-')                                         AS "Apellido",
            COALESCE(u."MobilNumber", '-')                                      AS "Telefono",
            COALESCE(u."Email", '-')                                            AS "Correo Electronico",
            COALESCE(up."Instagram", '-')                                       AS "Usuario Instagram",
            COALESCE(TO_CHAR(up."BirthDate", 'DD/MM/YYYY'), '-')               AS "Fecha de Nacimiento",
            COALESCE(EXTRACT(YEAR FROM AGE(up."BirthDate"))::TEXT, '-')        AS "Edad",
            COALESCE(gender."Name", '-')                                        AS "Genero",
            COALESCE(marital_status."Name", '-')                               AS "Estado Civil",
            COALESCE(role_house."Name", '-')                                   AS "Rol Familiar",
            COALESCE(income_range."Name", '-')                                 AS "Rango de Ingreso",
            COALESCE(profession."Name", '-')                                   AS "Profesion",
            COALESCE(STRING_AGG(DISTINCT pet."Name", ' - '), '-')              AS "Mascotas",
            COALESCE(STRING_AGG(DISTINCT hobby."Name", ' - '), '-')            AS "Hobbies",
            COALESCE(frequency_activities."Name", '-')                         AS "Frecuencia Actividad Fisica",
            COALESCE(number_children."Name", '-')                              AS "Numero de Hijos",
            COALESCE(level_academic."Name", '-')                               AS "Nivel Academico",
            CASE WHEN up."IsBuyManagerHome"      = TRUE THEN 'SI'
                 WHEN up."IsBuyManagerHome"      = FALSE THEN 'NO' ELSE '-' END AS "Compras en el Hogar",
            CASE WHEN up."IsPregnant"            = TRUE THEN 'SI'
                 WHEN up."IsPregnant"            = FALSE THEN 'NO' ELSE '-' END AS "Embarazo",
            CASE WHEN up."IsInterestedTechnology"= TRUE THEN 'SI'
                 WHEN up."IsInterestedTechnology"= FALSE THEN 'NO' ELSE '-' END AS "Interesado en Tecnologia",
            CASE WHEN up."IsAlcoholConsume"      = TRUE THEN 'SI'
                 WHEN up."IsAlcoholConsume"      = FALSE THEN 'NO' ELSE '-' END AS "Consume alcohol",
            CASE WHEN up."IsTobaccoConsume"      = TRUE THEN 'SI'
                 WHEN up."IsTobaccoConsume"      = FALSE THEN 'NO' ELSE '-' END AS "Consume nicotina",
            REGEXP_REPLACE(COALESCE(NULLIF(up."Address", ''), '-'),      '\\n', '', 'g') AS "Direccion",
            REGEXP_REPLACE(COALESCE(NULLIF(up."ExactAddress", ''), '-'), '\\n', '', 'g') AS "Direccion Exacta",
            REGEXP_REPLACE(COALESCE(NULLIF(up."Instruction", ''), '-'),  '\\n', '', 'g') AS "Indicaciones",
            CONCAT(
                REGEXP_REPLACE(COALESCE(NULLIF(up."Address", ''), '-'),     '\\n', '', 'g'), ' ',
                REGEXP_REPLACE(COALESCE(NULLIF(up."Instruction", ''), '-'), '\\n', '', 'g')
            ) AS "Direccion Completa",
            COALESCE(
                up."Zone",
                (SELECT CAST(
                    (regexp_matches(
                        CONCAT(
                            REGEXP_REPLACE(COALESCE(NULLIF(up."Address", ''), '-'),      '[\\n\\t|$~;,]', '', 'g'), ' ',
                            REGEXP_REPLACE(COALESCE(NULLIF(up."ExactAddress", ''), '-'), '[\\n\\t|$~;,]', '', 'g'), ' ',
                            REGEXP_REPLACE(COALESCE(NULLIF(up."Instruction", ''), '-'),  '[\\n\\t|$~;,]', '', 'g')
                        ),
                        '(?:Zona|ZONA|zona|Z\\.)[\\s\\(/]*(\\d+)[\\s\\)/]*.*'
                    ))[1] AS int
                )),
                -1
            ) AS "Zona",
            'Guatemala'                                                         AS "Pais",
            COALESCE(d."DepartmentName", '-')                                  AS "Departamento",
            COALESCE(c."CityName", '-')                                        AS "Municipio",
            COALESCE(up."Latitude"::TEXT, '-')                                 AS "Latitude",
            COALESCE(up."Longitude"::TEXT, '-')                                AS "Longitude",
            COALESCE(TO_CHAR(up."CreationDate", 'DD/MM/YYYY'), '-')            AS "Fecha de Registro",
            COALESCE(TO_CHAR(up."LastUserProfileDate", 'DD/MM/YYYY'), '-')     AS "Ultima fecha perfil usuario",
            COALESCE(TO_CHAR(up."PreviousUserProfileDate", 'DD/MM/YYYY'), '-') AS "Anterior Perfil Fecha",
            COALESCE(TO_CHAR(u."LastSession", 'DD/MM/YYYY'), '-')              AS "Ultima Sesion",
            COALESCE(u."IsMigrated"::TEXT, '-')                                AS "Migrado",
            (
                SELECT array_agg(
                    json_build_object(
                        'survey_id',           us."SurveyId",
                        'nombre_encuesta',     s."Name",
                        'descripcion_encuesta',s."Description",
                        'categoria',           ct."Name",
                        'url_encuesta',        s."SurveyUrl",
                        'tipo_encuesta',       st_type."Name",
                        'estado_encuesta',     st_status."Name",
                        'fecha_creacion',      TO_CHAR(us."CreationDate", 'DD/MM/YYYY'),
                        'respuestas',          us."AnswersJson"
                    )
                )
                FROM public."UserSurveys" us
                LEFT JOIN public."Surveys" s
                       ON us."SurveyId" = s."Id" AND s."Deleted" = false
                LEFT JOIN public."Categories" ct
                       ON s."CategoryId" = ct."Id" AND ct."Deleted" = false
                LEFT JOIN public."Settings" st_status
                       ON us."StatusId" = st_status."Id" AND st_status."Deleted" = false
                LEFT JOIN public."Settings" st_type
                       ON s."TypeSurveyId" = st_type."Id" AND st_type."Deleted" = false
                WHERE us."UserId" = u."Id"
                  AND us."Deleted" = false
                  AND us."SurveyId" IN %s
                  {sub_cond}
            ) AS "Encuestas"
        FROM public."Users" u
        JOIN  public."UserProfiles" up   ON u."Id" = up."UserId"
        LEFT JOIN public."Cities" c      ON up."CityId" = c."Id"
        LEFT JOIN public."Departments" d ON c."DepartmentId" = d."Id"
        LEFT JOIN public."Settings" gender             ON up."GenderId"                     = gender."Id"
        LEFT JOIN public."Settings" marital_status     ON up."MaritalStatusId"               = marital_status."Id"
        LEFT JOIN public."Settings" role_house         ON up."RoleHouseId"                   = role_house."Id"
        LEFT JOIN public."Settings" income_range       ON up."IncomeRangeId"                 = income_range."Id"
        LEFT JOIN public."Settings" profession         ON up."ProfessionsId"                 = profession."Id"
        LEFT JOIN public."Settings" frequency_activities ON up."FrequencyActivitiesPhysicalId" = frequency_activities."Id"
        LEFT JOIN public."Settings" pet   ON pet."Id"   = ANY(string_to_array(up."PetsId", ',')::int[])
        LEFT JOIN public."Settings" hobby ON hobby."Id" = ANY(string_to_array(up."HobbiesId", ',')::int[])
        LEFT JOIN public."Settings" number_children ON up."NumberChildrenId" = number_children."Id"
        LEFT JOIN public."Settings" level_academic  ON up."LevelAcademicId"  = level_academic."Id"
        WHERE u."Id" IN (SELECT "UserId" FROM usuarios_survey)
        GROUP BY
            u."Id", u."Name", u."LastName", u."MobilNumber", u."Email", up."Instagram",
            up."BirthDate", gender."Name", marital_status."Name", role_house."Name",
            income_range."Name", profession."Name", frequency_activities."Name",
            up."Address", up."ExactAddress", up."Instruction", up."Zone",
            d."DepartmentName", c."CityName", up."Latitude", up."Longitude",
            up."CreationDate", up."LastUserProfileDate", up."PreviousUserProfileDate",
            u."LastSession", u."IsMigrated", up."IsBuyManagerHome", up."IsPregnant",
            up."IsInterestedTechnology", up."IsAlcoholConsume", up."IsTobaccoConsume",
            number_children."Name", level_academic."Name"
        ORDER BY u."Id"
        """

        main_params: list = [valid_ids]
        if status_id:
            main_params.append(status_id)
        main_params.append(valid_ids)
        if status_id:
            main_params.append(status_id)

        cur.execute(main_sql, main_params)
        rows = [dict(r) for r in cur.fetchall()]

    # ── 4. Separar datos de usuario y respuestas ─────────────────────────────
    base_cols = [k for k in (rows[0].keys() if rows else []) if k != "Encuestas"]
    users_data: list = []
    dfs_encuestas: dict = {}  # clave → {'info': {...}, 'datos': [...]}

    for row in rows:
        users_data.append({c: row[c] for c in base_cols})

        info_usuario = {
            "Id":                   row.get("Id"),
            "Nombre":               row.get("Nombre"),
            "Apellido":             row.get("Apellido"),
            "Correo Electronico":   row.get("Correo Electronico"),
            "Telefono":             row.get("Telefono"),
        }

        for enc in (row.get("Encuestas") or []):
            sid   = enc.get("survey_id")
            nombre = enc.get("nombre_encuesta") or str(sid)
            clave  = f"{sid} - {nombre}"

            if clave not in dfs_encuestas:
                dfs_encuestas[clave] = {"info": enc, "datos": []}

            s_row = info_usuario.copy()
            answers_raw = enc.get("respuestas")
            if answers_raw:
                try:
                    answers = (
                        _json.loads(answers_raw)
                        if isinstance(answers_raw, str)
                        else answers_raw
                    )
                    if isinstance(answers, dict):
                        for _k, v in answers.items():
                            if isinstance(v, dict) and "question" in v and "option" in v:
                                q = v["question"].get("question", _k)
                                a = v["option"].get("answer", "-")
                            else:
                                q, a = str(_k), str(v)
                            s_row[str(q).replace(",", " - ")] = str(a).replace(",", " - ")
                except Exception:
                    pass
            dfs_encuestas[clave]["datos"].append(s_row)

    # ── 5. Construir Excel ───────────────────────────────────────────────────
    total_usuarios      = len(users_data)
    total_participaciones = sum(s["participaciones"] for s in surveys_validas)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:

        # Hoja Usuarios
        df_users = (
            pd.DataFrame(users_data)
            if users_data
            else pd.DataFrame([{"Info": "Sin datos"}])
        )
        df_users.to_excel(writer, sheet_name="Usuarios", index=False)

        # Una hoja por encuesta con respuestas expandidas
        used_sheet_names: set = {"Usuarios"}
        for clave, info in dfs_encuestas.items():
            if not info["datos"]:
                continue
            sid    = info["info"]["survey_id"]
            nombre = info["info"]["nombre_encuesta"] or str(sid)
            base   = f"Survey_{sid}_{_clean_sheet_name(nombre)}"[:31]
            # evitar duplicados
            sheet_name = base
            suffix = 2
            while sheet_name in used_sheet_names:
                sheet_name = f"{base[:28]}_{suffix}"
                suffix += 1
            used_sheet_names.add(sheet_name)
            pd.DataFrame(info["datos"]).to_excel(writer, sheet_name=sheet_name, index=False)

        # Hoja Surveys
        surveys_sheet = [
            {
                "Survey ID":            s["Id"],
                "Nombre":               s["Name"],
                "Usuarios Unicos":      s["usuarios_unicos"],
                "Participaciones":      s["participaciones"],
                "StatusId Filtrado":    status_id if status_id else "Todos",
                "Estado de la Encuesta": status_name,
            }
            for s in surveys_validas
        ]
        pd.DataFrame(surveys_sheet).to_excel(writer, sheet_name="Surveys", index=False)

        # Hoja Resumen
        resumen = {
            "Metrica": ["Usuarios Unicos", "Participaciones Totales", "Surveys Incluidas"],
            "Valor":   [total_usuarios, total_participaciones, len(surveys_validas)],
            "StatusId Filtrado":     [status_id if status_id else "Todos"] * 3,
            "Estado de la Encuesta": [status_name] * 3,
        }
        pd.DataFrame(resumen).to_excel(writer, sheet_name="Resumen", index=False)

    output.seek(0)
    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    return StreamingResponse(
        iter([output.read()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename=reporte_encuestas_{fecha}.xlsx"
        },
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
                   us."StatusId", us."AnsweredDate",
                   st."Name" AS status_name,
                   COALESCE(EXTRACT(YEAR FROM AGE(up."BirthDate"))::TEXT, '-') AS edad,
                   COALESCE(gender."Name", '-') AS genero,
                   COALESCE(d."DepartmentName", '-') AS departamento,
                   COALESCE(c."CityName", '-') AS ciudad
            FROM public."UserSurveys" us
            JOIN public."Users" u ON us."UserId" = u."Id"
            LEFT JOIN public."UserProfiles" up ON u."Id" = up."UserId"
            LEFT JOIN public."Settings" st ON us."StatusId" = st."Id"
            LEFT JOIN public."Settings" gender ON up."GenderId" = gender."Id"
            LEFT JOIN public."Cities" c ON up."CityId" = c."Id"
            LEFT JOIN public."Departments" d ON c."DepartmentId" = d."Id"
            WHERE us."SurveyId" = %s AND us."Deleted" IS DISTINCT FROM TRUE
            ORDER BY us."AnsweredDate" DESC NULLS LAST
            """,
            (survey_id,),
        )
        users = [dict(r) for r in cur.fetchall()]

    return {"survey": dict(survey), "users": users}


@router.get("/{survey_id}/users/export")
def export_survey_users(
    survey_id: int,
    status_id: Optional[int] = Query(None, description="Filtro opcional de status"),
    format: Optional[str] = Query("xlsx"),
    _: dict = Depends(get_current_admin),
):
    import json as _json
    import unicodedata
    import re

    def clean_sheet_name(name: str) -> str:
        name = unicodedata.normalize('NFKD', str(name))
        name = name.encode('ASCII', 'ignore').decode('ASCII')
        name = re.sub(r'[^\w\-_]', '_', name)
        return name[:31]

    status_cond = "AND \"StatusId\" = %s" if status_id else ""
    status_cond_us = "AND us.\"StatusId\" = %s" if status_id else ""

    user_sql = f"""
        WITH usuarios_survey AS (
            SELECT DISTINCT "UserId"
            FROM public."UserSurveys"
            WHERE "SurveyId" = %s AND "Deleted" IS DISTINCT FROM TRUE
            {status_cond}
        )
        SELECT 
            u."Id",
            COALESCE(u."Name", '-') AS "Nombre",
            COALESCE(u."LastName", '-') AS "Apellido",
            COALESCE(u."MobilNumber", '-') AS "Teléfono",
            COALESCE(u."Email", '-') AS "Correo Electrónico",
            COALESCE(up."Instagram", '-') AS "Usuario Instragram",
            COALESCE(TO_CHAR(up."BirthDate", 'DD/MM/YYYY'), '-') AS "Fecha de Nacimiento",
            COALESCE(EXTRACT(YEAR FROM AGE(up."BirthDate"))::TEXT, '-') AS "Edad",
            COALESCE(gender."Name", '-') AS "Género",
            COALESCE(marital_status."Name", '-') AS "Estado Civil",
            COALESCE(role_house."Name", '-') AS "Rol Familiar",
            COALESCE(income_range."Name", '-') AS "Rango de Ingreso",
            COALESCE(profession."Name", '-') AS "Profesión",
            COALESCE(STRING_AGG(DISTINCT pet."Name", ' - '), '-') AS "Mascotas",
            COALESCE(STRING_AGG(DISTINCT hobby."Name", ' - '), '-') AS "Hobbies",
            COALESCE(frequency_activities."Name", '-') AS "Frecuencia Actividad Física",
            COALESCE(number_children."Name", '-') AS "Número de Hijos",
            COALESCE(level_academic."Name", '-') AS "Nivel Academico",
            CASE WHEN up."IsBuyManagerHome" = TRUE THEN 'SI' WHEN up."IsBuyManagerHome" = FALSE THEN 'NO' ELSE '-' END AS "Compras en el Hogar",
            CASE WHEN up."IsPregnant" = TRUE THEN 'SI' WHEN up."IsPregnant" = FALSE THEN 'NO' ELSE '-' END AS "Embarazo",
            CASE WHEN up."IsInterestedTechnology" = TRUE THEN 'SI' WHEN up."IsInterestedTechnology" = FALSE THEN 'NO' ELSE '-' END AS "Interesado en Tecnología",
            CASE WHEN up."IsAlcoholConsume" = TRUE THEN 'SI' WHEN up."IsAlcoholConsume" = FALSE THEN 'NO' ELSE '-' END AS "Consume alcohol",
            CASE WHEN up."IsTobaccoConsume" = TRUE THEN 'SI' WHEN up."IsTobaccoConsume" = FALSE THEN 'NO' ELSE '-' END AS "Consume nicotina",
            REGEXP_REPLACE(COALESCE(NULLIF(up."Address", ''), '-'), '\\n', '', 'g') AS "Dirección",
            REGEXP_REPLACE(COALESCE(NULLIF(up."ExactAddress", ''), '-'), '\\n', '', 'g') AS "Dirección Exacta",
            REGEXP_REPLACE(COALESCE(NULLIF(up."Instruction", ''), '-'), '\\n', '', 'g') AS "Indicaciones",
            CONCAT(
                REGEXP_REPLACE(COALESCE(NULLIF(up."Address", ''), '-'), '\\n', '', 'g'), ' ',
                REGEXP_REPLACE(COALESCE(NULLIF(up."Instruction", ''), '-'), '\\n', '', 'g')
            ) AS "Dirección Completa",
            COALESCE(
                up."Zone",
                (
                    SELECT CAST((regexp_matches(
                        CONCAT(
                            REGEXP_REPLACE(COALESCE(NULLIF(up."Address", ''), '-'), '[\\n\\t|$~;,]', '', 'g'), ' ',
                            REGEXP_REPLACE(COALESCE(NULLIF(up."ExactAddress", ''), '-'), '[\\n\\t|$~;,]', '', 'g'), ' ',
                            REGEXP_REPLACE(COALESCE(NULLIF(up."Instruction", ''), '-'), '[\\n\\t|$~;,]', '', 'g')
                        ),
                        '(?:Zona|ZONA|zona|ZONA: |zona: |Zona: |ZONA:|zona:|Zona:|Z\\.|z\\.|Z|z)[\\s\\(/]*(\\d+)[\\s\\)/]*.*'
                    ))[1] AS int)
                ),
                -1
            ) AS "Zona",
            COALESCE(NULL, 'Guatemala') AS "País",
            COALESCE(d."DepartmentName", '-') AS "Departamento",
            COALESCE(c."CityName", '-') AS "Municipio",
            COALESCE(up."Latitude"::TEXT, '-') AS "Latitude",
            COALESCE(up."Longitude"::TEXT, '-') AS "Longitude",
            COALESCE(TO_CHAR(up."CreationDate", 'DD/MM/YYYY'), '-') AS "Fecha de Registro",
            COALESCE(TO_CHAR(up."LastUserProfileDate", 'DD/MM/YYYY'), '-') AS "Última fecha del perfil de usuario",
            COALESCE(TO_CHAR(up."PreviousUserProfileDate", 'DD/MM/YYYY'), '-') AS "Anterior Perfil de usuario Fecha",
            COALESCE(TO_CHAR(u."LastSession", 'DD/MM/YYYY'), '-') AS "Última Sesión",
            COALESCE(u."IsMigrated"::TEXT, '-') AS "Migrado",
            (
                SELECT json_agg(
                    json_build_object(
                        'survey_id', us."SurveyId",
                        'nombre_encuesta', s."Name",
                        'respuestas', us."AnswersJson"
                    ) ORDER BY us."SurveyId"
                )
                FROM public."UserSurveys" us
                LEFT JOIN public."Surveys" s ON us."SurveyId"=s."Id" AND s."Deleted" IS DISTINCT FROM TRUE
                WHERE us."UserId"=u."Id" AND us."SurveyId"=%s AND us."Deleted" IS DISTINCT FROM TRUE
                {status_cond_us}
            ) AS "_survey_data"
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
        WHERE u."Id" IN (SELECT "UserId" FROM usuarios_survey)
        GROUP BY
            u."Id", u."Name", u."LastName", u."MobilNumber", u."Email", up."Instagram",
            up."BirthDate", gender."Name", marital_status."Name", role_house."Name",
            income_range."Name", profession."Name", frequency_activities."Name",
            up."Address", up."ExactAddress", up."Instruction", up."Zone",
            d."DepartmentName", c."CityName", up."Latitude", up."Longitude",
            up."CreationDate", up."LastUserProfileDate", up."PreviousUserProfileDate",
            u."LastSession", u."IsMigrated", up."IsBuyManagerHome", up."IsPregnant",
            up."IsInterestedTechnology", up."IsAlcoholConsume", up."IsTobaccoConsume",
            number_children."Name", level_academic."Name"
        ORDER BY u."Id"
    """

    sql_params = [survey_id]
    if status_id:
        sql_params.append(status_id)
    sql_params.append(survey_id)
    if status_id:
        sql_params.append(status_id)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(user_sql, sql_params)
        rows = [dict(r) for r in cur.fetchall()]

        cur.execute('SELECT "Id", "Name" FROM public."Surveys" WHERE "Id"=%s', (survey_id,))
        survey = cur.fetchone()
        if not survey:
            raise HTTPException(404, "Encuesta no encontrada")
        survey_name = survey["Name"]

        status_name = "Todos"
        if status_id:
            cur.execute('SELECT "Name" FROM public."Settings" WHERE "Id" = %s', (status_id,))
            st_res = cur.fetchone()
            if st_res:
                status_name = st_res["Name"]

    base_cols = [k for k in (rows[0].keys() if rows else []) if k != "_survey_data"]
    users_data = []
    survey_data = []

    for row in rows:
        surveys_res = row.get("_survey_data") or []
        user_row = {c: row[c] for c in base_cols if c in row}
        users_data.append(user_row)

        info_usuario = {
            'Id': row.get('Id'),
            'Nombre': row.get('Nombre'),
            'Apellido': row.get('Apellido'),
            'Correo Electrónico': row.get('Correo Electrónico'),
            'Teléfono': row.get('Teléfono')
        }

        for entry in surveys_res:
            s_row = info_usuario.copy()
            answers_raw = entry.get("respuestas")
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
                            q_clean = str(q).replace(',', ' - ')
                            a_clean = str(a).replace(',', ' - ')
                            s_row[q_clean] = a_clean
                except Exception:
                    pass
            survey_data.append(s_row)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Hoja Usuarios
        df_users = pd.DataFrame(users_data) if users_data else pd.DataFrame([{"Info": "Sin datos"}])
        df_users.to_excel(writer, sheet_name="Usuarios", index=False)

        # Hoja Encuesta específica con respuestas expandidas
        sheet_name = f"Survey_{survey_id}_{clean_sheet_name(survey_name)}"[:31]
        df_survey = pd.DataFrame(survey_data) if survey_data else pd.DataFrame([{"Info": "Sin respuestas"}])
        df_survey.to_excel(writer, sheet_name=sheet_name, index=False)

        # Hoja Surveys
        surveys_info = [{
            'Survey ID': survey_id,
            'Nombre': survey_name,
            'Usuarios Únicos': len(set(r['Id'] for r in survey_data)),
            'Participaciones': len(survey_data),
            'StatusId Filtrado': status_id if status_id else 'Todos',
            'Estado de la Encuesta': status_name
        }]
        pd.DataFrame(surveys_info).to_excel(writer, sheet_name="Surveys", index=False)
        
        # Hoja Resumen
        resumen_data = {
            'Métrica': ['Usuarios Únicos', 'Participaciones Totales', 'Surveys Incluidas'],
            'Valor': [len(users_data), len(survey_data), 1],
            'StatusId Filtrado': [status_id if status_id else 'Todos'] * 3,
            'Estado de la Encuesta': [status_name] * 3
        }
        pd.DataFrame(resumen_data).to_excel(writer, sheet_name="Resumen", index=False)

    output.seek(0)
    return StreamingResponse(
        iter([output.read()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename=reporte_encuesta_{survey_id}.xlsx"},
    )


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
    """
    Asigna usuarios a la encuesta desde un Excel con columna 'user_id'.
    Usa bulk queries (ANY + execute_values) para soportar miles de registros
    sin timeout: 3 queries totales en lugar de N×3.
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
            'SELECT "UserId" FROM public."UserSurveys" WHERE "SurveyId"=%s AND "UserId" = ANY(%s)',
            (survey_id, list(valid_ids)),
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
                'INSERT INTO public."UserSurveys" '
                '("UserId","SurveyId","AnswersJson","CreationDate","Deleted","StatusId","AnsweredDate") '
                "VALUES %s",
                [(uid, survey_id, None, now, False, status_id, None) for uid in to_insert],
            )
            assigned = len(to_insert)

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

import unicodedata, re

def _clean_sheet_name(name: str) -> str:
    name = unicodedata.normalize('NFKD', str(name))
    name = name.encode('ASCII', 'ignore').decode('ASCII')
    name = re.sub(r'[^\w\-_]', '_', name)
    return name[:31]


@router.get("/internal-report")
def generate_internal_report(
    survey_ids: str = Query(..., description="IDs separados por comas, ej: 1,2,3"),
    status_id: Optional[int] = Query(None, description="Filtro opcional de status"),
    _: dict = Depends(get_current_admin)
):
    """Genera Excel con datos de usuarios + respuestas por encuesta (una hoja por encuesta)."""
    s_ids = [int(x.strip()) for x in survey_ids.split(",") if x.strip().isdigit()]
    if not s_ids:
        raise HTTPException(400, "Debe proporcionar al menos un ID de encuesta válido")

    status_cond = "AND \"StatusId\" = %s" if status_id else ""
    status_cond_us = "AND us.\"StatusId\" = %s" if status_id else ""

    user_sql = f"""
        WITH usuarios_survey AS (
            SELECT DISTINCT "UserId"
            FROM public."UserSurveys"
            WHERE "SurveyId" = ANY(%s) AND "Deleted" IS DISTINCT FROM TRUE
            {status_cond}
        )
        SELECT 
            u."Id",
            COALESCE(u."Name", '-') AS "Nombre",
            COALESCE(u."LastName", '-') AS "Apellido",
            COALESCE(u."MobilNumber", '-') AS "Teléfono",
            COALESCE(u."Email", '-') AS "Correo Electrónico",
            COALESCE(up."Instagram", '-') AS "Usuario Instragram",
            COALESCE(TO_CHAR(up."BirthDate", 'DD/MM/YYYY'), '-') AS "Fecha de Nacimiento",
            COALESCE(EXTRACT(YEAR FROM AGE(up."BirthDate"))::TEXT, '-') AS "Edad",
            COALESCE(gender."Name", '-') AS "Género",
            COALESCE(marital_status."Name", '-') AS "Estado Civil",
            COALESCE(role_house."Name", '-') AS "Rol Familiar",
            COALESCE(income_range."Name", '-') AS "Rango de Ingreso",
            COALESCE(profession."Name", '-') AS "Profesión",
            COALESCE(STRING_AGG(DISTINCT pet."Name", ' - '), '-') AS "Mascotas",
            COALESCE(STRING_AGG(DISTINCT hobby."Name", ' - '), '-') AS "Hobbies",
            COALESCE(frequency_activities."Name", '-') AS "Frecuencia Actividad Física",
            COALESCE(number_children."Name", '-') AS "Número de Hijos",
            COALESCE(level_academic."Name", '-') AS "Nivel Academico",
            CASE
                WHEN up."IsBuyManagerHome" = TRUE THEN 'SI'
                WHEN up."IsBuyManagerHome" = FALSE THEN 'NO'
                ELSE '-'
            END AS "Compras en el Hogar",
            CASE
                WHEN up."IsPregnant" = TRUE THEN 'SI'
                WHEN up."IsPregnant" = FALSE THEN 'NO'
                ELSE '-'
            END AS "Embarazo",
            CASE
                WHEN up."IsInterestedTechnology" = TRUE THEN 'SI'
                WHEN up."IsInterestedTechnology" = FALSE THEN 'NO'
                ELSE '-'
            END AS "Interesado en Tecnología",
            CASE
                WHEN up."IsAlcoholConsume" = TRUE THEN 'SI'
                WHEN up."IsAlcoholConsume" = FALSE THEN 'NO'
                ELSE '-'
            END AS "Consume alcohol",
            CASE
                WHEN up."IsTobaccoConsume" = TRUE THEN 'SI'
                WHEN up."IsTobaccoConsume" = FALSE THEN 'NO'
                ELSE '-'
            END AS "Consume nicotina",
            REGEXP_REPLACE(COALESCE(NULLIF(up."Address", ''), '-'), '\\n', '', 'g') AS "Dirección",
            REGEXP_REPLACE(COALESCE(NULLIF(up."ExactAddress", ''), '-'), '\\n', '', 'g') AS "Dirección Exacta",
            REGEXP_REPLACE(COALESCE(NULLIF(up."Instruction", ''), '-'), '\\n', '', 'g') AS "Indicaciones",
            CONCAT(
                REGEXP_REPLACE(COALESCE(NULLIF(up."Address", ''), '-'), '\\n', '', 'g'),
                ' ',
                REGEXP_REPLACE(COALESCE(NULLIF(up."Instruction", ''), '-'), '\\n', '', 'g')
            ) AS "Dirección Completa",
            COALESCE(
                up."Zone",
                (
                    SELECT
                        CAST(
                            (regexp_matches(
                                CONCAT(
                                    REGEXP_REPLACE(COALESCE(NULLIF(up."Address", ''), '-'), '[\\n\\t|$~;,]', '', 'g'),
                                    ' ',
                                    REGEXP_REPLACE(COALESCE(NULLIF(up."ExactAddress", ''), '-'), '[\\n\\t|$~;,]', '', 'g'),
                                    ' ',
                                    REGEXP_REPLACE(COALESCE(NULLIF(up."Instruction", ''), '-'), '[\\n\\t|$~;,]', '', 'g')
                                ),
                                '(?:Zona|ZONA|zona|ZONA: |zona: |Zona: |ZONA:|zona:|Zona:|Z\\.|z\\.|Z|z)[\\s\\(/]*(\\d+)[\\s\\)/]*.*'
                            ))[1] AS int
                        )
                ),
                -1
            ) AS "Zona",
            COALESCE(NULL, 'Guatemala') AS "País",
            COALESCE(d."DepartmentName", '-') AS "Departamento",
            COALESCE(c."CityName", '-') AS "Municipio",
            COALESCE(up."Latitude"::TEXT, '-') AS "Latitude",
            COALESCE(up."Longitude"::TEXT, '-') AS "Longitude",
            COALESCE(TO_CHAR(up."CreationDate", 'DD/MM/YYYY'), '-') AS "Fecha de Registro",
            COALESCE(TO_CHAR(up."LastUserProfileDate", 'DD/MM/YYYY'), '-') AS "Última fecha del perfil de usuario",
            COALESCE(TO_CHAR(up."PreviousUserProfileDate", 'DD/MM/YYYY'), '-') AS "Anterior Perfil de usuario Fecha",
            COALESCE(TO_CHAR(u."LastSession", 'DD/MM/YYYY'), '-') AS "Última Sesión",
            COALESCE(u."IsMigrated"::TEXT, '-') AS "Migrado",
            (
                SELECT json_agg(
                    json_build_object(
                        'survey_id', us."SurveyId",
                        'nombre_encuesta', s."Name",
                        'descripcion_encuesta', s."Description",
                        'categoria', ct."Name",
                        'url_encuesta', s."SurveyUrl",
                        'tipo_encuesta', st_type."Name",
                        'estado_encuesta', st_status."Name", 
                        'fecha_creacion', TO_CHAR(us."CreationDate", 'DD/MM/YYYY'),
                        'respuestas', us."AnswersJson"
                    ) ORDER BY us."SurveyId"
                )
                FROM public."UserSurveys" us
                LEFT JOIN public."Surveys" s ON us."SurveyId"=s."Id" AND s."Deleted" IS DISTINCT FROM TRUE
                LEFT JOIN public."Categories" ct ON s."CategoryId"=ct."Id" AND ct."Deleted" IS DISTINCT FROM TRUE
                LEFT JOIN public."Settings" st_status ON us."StatusId"=st_status."Id" AND st_status."Deleted" IS DISTINCT FROM TRUE
                LEFT JOIN public."Settings" st_type ON s."TypeSurveyId"=st_type."Id" AND st_type."Deleted" IS DISTINCT FROM TRUE
                WHERE us."UserId"=u."Id" AND us."SurveyId"=ANY(%s) AND us."Deleted" IS DISTINCT FROM TRUE
                {status_cond_us}
            ) AS "_survey_data"
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
        WHERE u."Id" IN (SELECT "UserId" FROM usuarios_survey)
        GROUP BY
            u."Id", u."Name", u."LastName", u."MobilNumber", u."Email", up."Instagram",
            up."BirthDate", gender."Name", marital_status."Name", role_house."Name",
            income_range."Name", profession."Name", frequency_activities."Name",
            up."Address", up."ExactAddress", up."Instruction", up."Zone",
            d."DepartmentName", c."CityName", up."Latitude", up."Longitude",
            up."CreationDate", up."LastUserProfileDate", up."PreviousUserProfileDate",
            u."LastSession", u."IsMigrated", up."IsBuyManagerHome", up."IsPregnant",
            up."IsInterestedTechnology", up."IsAlcoholConsume", up."IsTobaccoConsume",
            number_children."Name", level_academic."Name"
        ORDER BY u."Id"
    """

    sql_params = [s_ids]
    if status_id:
        sql_params.append(status_id)
    sql_params.append(s_ids)
    if status_id:
        sql_params.append(status_id)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(user_sql, sql_params)
        rows = [dict(r) for r in cur.fetchall()]

        # Get survey names for sheet naming
        cur.execute(
            'SELECT "Id", "Name" FROM public."Surveys" WHERE "Id"=ANY(%s)',
            (s_ids,),
        )
        survey_names = {r["Id"]: r["Name"] for r in cur.fetchall()}
        
        status_name = "Todos"
        if status_id:
            cur.execute('SELECT "Name" FROM public."Settings" WHERE "Id" = %s', (status_id,))
            st_res = cur.fetchone()
            if st_res:
                status_name = st_res["Name"]

    # Process rows into per-survey DataFrames
    base_cols = [k for k in (rows[0].keys() if rows else []) if k != "_survey_data"]
    per_survey: dict = {sid: [] for sid in s_ids}
    users_data = []

    import json as _json
    for row in rows:
        surveys_data = row.get("_survey_data") or []
        user_row = {c: row[c] for c in base_cols if c in row}
        users_data.append(user_row)

        info_usuario = {
            'Id': row.get('Id'),
            'Nombre': row.get('Nombre'),
            'Apellido': row.get('Apellido'),
            'Correo Electrónico': row.get('Correo Electrónico'),
            'Teléfono': row.get('Teléfono')
        }

        for entry in surveys_data:
            sid = entry.get("survey_id")
            if sid not in per_survey:
                continue
            
            survey_row = info_usuario.copy()

            answers_raw = entry.get("respuestas")
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
                            q_clean = str(q).replace(',', ' - ')
                            a_clean = str(a).replace(',', ' - ')
                            survey_row[q_clean] = a_clean
                except Exception:
                    pass
            per_survey[sid].append(survey_row)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Full users sheet
        df_users = pd.DataFrame(users_data)
        df_users.to_excel(writer, sheet_name="Usuarios", index=False)

        surveys_validas_count = 0

        for sid in s_ids:
            data = per_survey[sid]
            if data:
                surveys_validas_count += 1
                survey_name = survey_names.get(sid, "")
                # Limitamos el nombre a 31 caracteres para prevenir un crash de openpyxl
                sheet_name = f"Survey_{sid}_{_clean_sheet_name(survey_name)}"[:31]
                pd.DataFrame(data).to_excel(writer, sheet_name=sheet_name, index=False)

        # Summary sheet
        surveys_info = [
            {
                'Survey ID': sid,
                'Nombre': survey_names.get(sid, "-"),
                'Usuarios Únicos': len(set(r['Id'] for r in per_survey[sid])),
                'Participaciones': len(per_survey[sid]),
                'StatusId Filtrado': status_id if status_id else 'Todos',
                'Estado de la Encuesta': status_name
            }
            for sid in s_ids if len(per_survey[sid]) > 0
        ]
        if surveys_info:
            pd.DataFrame(surveys_info).to_excel(writer, sheet_name="Surveys", index=False)
            
        resumen_data = {
            'Métrica': ['Usuarios Únicos', 'Participaciones Totales', 'Surveys Incluidas'],
            'Valor': [len(users_data), sum(len(per_survey[sid]) for sid in s_ids), surveys_validas_count],
            'StatusId Filtrado': [status_id if status_id else 'Todos'] * 3,
            'Estado de la Encuesta': [status_name] * 3
        }
        pd.DataFrame(resumen_data).to_excel(writer, sheet_name="Resumen", index=False)

    output.seek(0)
    return StreamingResponse(
        iter([output.read()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet; charset=utf-8",
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
