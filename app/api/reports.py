import io
import pandas as pd
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from app.api.auth import get_current_admin
from app.core.database import get_db

router = APIRouter(prefix="/reports", tags=["reports"])

PULPEY_QUERY = """
    SELECT
        u."Id",
        COALESCE(u."Name", '-') AS "Nombre Pulpey",
        COALESCE(u."LastName", '-') AS "Apellido Pulpey",
        COALESCE(u."MobilNumber", '-') AS "Teléfono",
        COALESCE(u."Email", '-') AS "Correo Electrónico",
        COALESCE(
            NULLIF(
                REGEXP_REPLACE(COALESCE(up."Instagram",''),'^(?:https?://)?(?:www\\.)?instagram\\.com/|@|/$','','gi')
            ,'')
        ,'-') AS "Usuario Instagram",
        COALESCE(TO_CHAR(up."BirthDate",'DD/MM/YYYY'),'-') AS "Fecha de Nacimiento",
        COALESCE(EXTRACT(YEAR FROM AGE(up."BirthDate"))::TEXT,'-') AS "Edad",
        CASE
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 18 AND 22 THEN '18-22'
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 23 AND 27 THEN '23-27'
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 28 AND 32 THEN '28-32'
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 33 AND 37 THEN '33-37'
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 38 AND 45 THEN '38-45'
            WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) > 45 THEN '45+'
            ELSE 'Sin Dato'
        END AS "Rango de Edad",
        COALESCE(gender."Name",'-') AS "Género",
        COALESCE(marital."Name",'-') AS "Estado Civil",
        COALESCE(role_h."Name",'-') AS "Rol Familiar",
        COALESCE(income."Name",'-') AS "Rango de Ingreso",
        COALESCE(prof."Name",'-') AS "Profesión",
        COALESCE(STRING_AGG(DISTINCT pet."Name",' - '),'-') AS "Mascotas",
        COALESCE(STRING_AGG(DISTINCT hobby."Name",' - '),'-') AS "Hobbies",
        COALESCE(freq."Name",'-') AS "Frecuencia Actividad Física",
        COALESCE(nchild."Name",'-') AS "Número de Hijos",
        COALESCE(acad."Name",'-') AS "Nivel Académico",
        CASE WHEN up."IsBuyManagerHome"=TRUE THEN 'SI' WHEN up."IsBuyManagerHome"=FALSE THEN 'NO' ELSE '-' END AS "Compras en el Hogar",
        CASE WHEN up."IsPregnant"=TRUE THEN 'SI' WHEN up."IsPregnant"=FALSE THEN 'NO' ELSE '-' END AS "Embarazo",
        CASE WHEN up."IsInterestedTechnology"=TRUE THEN 'SI' WHEN up."IsInterestedTechnology"=FALSE THEN 'NO' ELSE '-' END AS "Interesado en Tecnología",
        CASE WHEN up."IsAlcoholConsume"=TRUE THEN 'SI' WHEN up."IsAlcoholConsume"=FALSE THEN 'NO' ELSE '-' END AS "Consume Alcohol",
        CASE WHEN up."IsTobaccoConsume"=TRUE THEN 'SI' WHEN up."IsTobaccoConsume"=FALSE THEN 'NO' ELSE '-' END AS "Consume Nicotina",
        REGEXP_REPLACE(COALESCE(NULLIF(up."Address",''),'-'),'[\\n,;]','-','g') AS "Dirección",
        REGEXP_REPLACE(COALESCE(NULLIF(up."ExactAddress",''),'-'),'[\\n,;]','-','g') AS "Dirección Exacta",
        REGEXP_REPLACE(COALESCE(NULLIF(up."Instruction",''),'-'),'[\\n,;]','-','g') AS "Indicaciones",
        COALESCE(up."Zone"::TEXT,'-') AS "Zona",
        'Guatemala' AS "País",
        COALESCE(d."DepartmentName",'-') AS "Departamento",
        COALESCE(c."CityName",'-') AS "Municipio",
        COALESCE(up."Latitude"::TEXT,'-') AS "Latitud",
        COALESCE(up."Longitude"::TEXT,'-') AS "Longitud",
        COALESCE(TO_CHAR(up."CreationDate",'DD/MM/YYYY'),'-') AS "Fecha de Registro",
        COALESCE(TO_CHAR(up."LastUserProfileDate",'DD/MM/YYYY'),'-') AS "Último Perfil",
        COALESCE(TO_CHAR(u."LastSession",'DD/MM/YYYY'),'-') AS "Última Sesión"
    FROM public."Users" u
    JOIN public."UserProfiles" up ON u."Id" = up."UserId"
    LEFT JOIN public."Cities" c ON up."CityId" = c."Id"
    LEFT JOIN public."Departments" d ON c."DepartmentId" = d."Id"
    LEFT JOIN public."Settings" gender ON up."GenderId" = gender."Id"
    LEFT JOIN public."Settings" marital ON up."MaritalStatusId" = marital."Id"
    LEFT JOIN public."Settings" role_h ON up."RoleHouseId" = role_h."Id"
    LEFT JOIN public."Settings" income ON up."IncomeRangeId" = income."Id"
    LEFT JOIN public."Settings" prof ON up."ProfessionsId" = prof."Id"
    LEFT JOIN public."Settings" freq ON up."FrequencyActivitiesPhysicalId" = freq."Id"
    LEFT JOIN public."Settings" pet ON pet."Id" = ANY(string_to_array(up."PetsId",',')::int[])
    LEFT JOIN public."Settings" hobby ON hobby."Id" = ANY(string_to_array(up."HobbiesId",',')::int[])
    LEFT JOIN public."Settings" nchild ON up."NumberChildrenId" = nchild."Id"
    LEFT JOIN public."Settings" acad ON up."LevelAcademicId" = acad."Id"
    WHERE
        LOWER(u."Email") = ANY(%s)
        OR REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(u."MobilNumber",'+',''),'-',''),'(',''),')',''),' ','') = ANY(%s)
    GROUP BY
        u."Id", u."Name", u."LastName", u."MobilNumber", u."Email",
        up."Instagram", up."BirthDate",
        gender."Name", marital."Name", role_h."Name",
        income."Name", prof."Name", freq."Name",
        up."Address", up."ExactAddress", up."Instruction", up."Zone",
        d."DepartmentName", c."CityName",
        up."Latitude", up."Longitude", up."CreationDate",
        up."LastUserProfileDate", u."LastSession",
        up."IsBuyManagerHome", up."IsPregnant", up."IsInterestedTechnology",
        up."IsAlcoholConsume", up."IsTobaccoConsume",
        nchild."Name", acad."Name"
"""


@router.post("/cross-reference")
async def cross_reference(
    file: UploadFile = File(...),
    _: dict = Depends(get_current_admin),
):
    """
    Sube un archivo Excel (formato QuestionPro), cruza con la DB de Pulpey
    y retorna un nuevo Excel con hoja extra 'Cruce Data Pulpey'.
    """
    contents = await file.read()

    # Intentar leer la hoja "Datos sin procesar" primero, luego la primera hoja
    try:
        df_original = pd.read_excel(io.BytesIO(contents), sheet_name="Datos sin procesar")
    except Exception:
        try:
            df_original = pd.read_excel(io.BytesIO(contents))
        except Exception as e:
            raise HTTPException(400, f"No se pudo leer el archivo Excel: {str(e)}")

    # Detectar columnas de email y teléfono
    email_col = next(
        (c for c in df_original.columns if "correo" in c.lower() and "pulpey" in c.lower()),
        next((c for c in df_original.columns if "correo" in c.lower()), None),
    )
    phone_col = next(
        (c for c in df_original.columns if "celular" in c.lower()),
        next((c for c in df_original.columns if "tel" in c.lower()), None),
    )

    emails = []
    phones = []

    if email_col:
        emails = [
            e.lower().strip()
            for e in df_original[email_col].astype(str).tolist()
            if e.strip() and e.strip().lower() != "nan"
        ]

    if phone_col:
        phones = [
            str(p).replace(".0", "").strip()
            for p in df_original[phone_col].fillna("").tolist()
            if str(p).strip()
        ]

    if not emails and not phones:
        raise HTTPException(400, "No se encontraron columnas de correo o teléfono en el archivo")

    # Consultar DB
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(PULPEY_QUERY, (emails or [""], phones or [""]))
        rows = [dict(r) for r in cur.fetchall()]

    df_pulpey = pd.DataFrame(rows) if rows else pd.DataFrame()

    # Construir Excel de respuesta
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_original.to_excel(writer, sheet_name="Datos Originales", index=False)
        df_pulpey.to_excel(writer, sheet_name="Cruce Data Pulpey", index=False)

        # Hoja combinada: original + datos Pulpey por email
        if email_col and not df_pulpey.empty and "Correo Electrónico" in df_pulpey.columns:
            df_orig_copy = df_original.copy()
            df_orig_copy["_key"] = df_orig_copy[email_col].astype(str).str.lower().str.strip()
            df_pul_copy = df_pulpey.copy()
            df_pul_copy["_key"] = df_pul_copy["Correo Electrónico"].astype(str).str.lower().str.strip()
            df_merged = df_orig_copy.merge(
                df_pul_copy.drop(columns=["Correo Electrónico"], errors="ignore"),
                on="_key",
                how="left",
            )
            df_merged.drop(columns=["_key"], errors="ignore", inplace=True)
            df_merged.fillna("-", inplace=True)
            df_merged.to_excel(writer, sheet_name="Cruce Completo", index=False)

    output.seek(0)
    return StreamingResponse(
        iter([output.read()]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=cruce_pulpey.xlsx"},
    )
