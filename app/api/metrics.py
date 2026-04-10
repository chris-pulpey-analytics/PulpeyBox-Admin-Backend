from fastapi import APIRouter, Depends, Query
from typing import Optional
from datetime import date
from app.api.auth import get_current_admin
from app.core.database import get_db

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("")
def get_metrics(
    date_from: Optional[date] = Query(None, description="Filtrar desde esta fecha de registro"),
    date_to: Optional[date] = Query(None, description="Filtrar hasta esta fecha de registro"),
    _: dict = Depends(get_current_admin),
):
    # Construir condición de fecha dinámica
    date_conditions = ['"Deleted" IS DISTINCT FROM TRUE']
    date_params = []
    if date_from:
        date_conditions.append('"CreationDate"::date >= %s')
        date_params.append(date_from)
    if date_to:
        date_conditions.append('"CreationDate"::date <= %s')
        date_params.append(date_to)
    base_where = "WHERE " + " AND ".join(date_conditions)
    base_where_u = base_where.replace('"Deleted"', 'u."Deleted"').replace('"CreationDate"', 'up."CreationDate"')

    with get_db() as conn:
        cur = conn.cursor()

        cur.execute(f'SELECT COUNT(*) FROM public."Users" {base_where}', date_params)
        total_users = cur.fetchone()["count"]

        cur.execute(
            f"""SELECT COUNT(*) FROM public."Users"
               {base_where}
               AND DATE_TRUNC('month',"CreationDate") = DATE_TRUNC('month',NOW())""",
            date_params,
        )
        new_this_month = cur.fetchone()["count"]

        cur.execute(
            f"""SELECT COUNT(*) FROM public."Users"
               {base_where}
               AND "LastSession" >= NOW() - INTERVAL '30 days'""",
            date_params,
        )
        active_30d = cur.fetchone()["count"]

        cur.execute(
            f"""SELECT COUNT(*) FROM public."Users" u
               JOIN public."UserProfiles" up ON u."Id" = up."UserId"
               WHERE u."Deleted" IS DISTINCT FROM TRUE
               {"AND up.\"CreationDate\"::date >= %s" if date_from else ""}
               {"AND up.\"CreationDate\"::date <= %s" if date_to else ""}
               AND up."BirthDate" IS NOT NULL""",
            date_params,
        )
        with_profile = cur.fetchone()["count"]

        # Sin verificar email
        cur.execute(
            f"""SELECT COUNT(*) FROM public."Users" {base_where}
               AND "IsAccountValidated" IS DISTINCT FROM TRUE""",
            date_params,
        )
        unverified = cur.fetchone()["count"]

        # Registro incompleto
        cur.execute(
            f"""SELECT COUNT(*) FROM public."Users" u
               LEFT JOIN public."UserProfiles" up ON u."Id" = up."UserId"
               WHERE u."Deleted" IS DISTINCT FROM TRUE
               {"AND up.\"CreationDate\"::date >= %s" if date_from else ""}
               {"AND up.\"CreationDate\"::date <= %s" if date_to else ""}
               AND (up."Id" IS NULL OR up."BirthDate" IS NULL)""",
            date_params,
        )
        incomplete = cur.fetchone()["count"]

        # Migrados
        cur.execute(
            f"""SELECT COUNT(*) FROM public."Users" {base_where}
               AND "IsMigrated" IS TRUE""",
            date_params,
        )
        migrated = cur.fetchone()["count"]

        # Distribución por género (respeta filtro de fecha por CreationDate del perfil)
        dist_where = "WHERE u.\"Deleted\" IS DISTINCT FROM TRUE"
        dist_params = []
        if date_from:
            dist_where += ' AND up."CreationDate"::date >= %s'
            dist_params.append(date_from)
        if date_to:
            dist_where += ' AND up."CreationDate"::date <= %s'
            dist_params.append(date_to)

        cur.execute(
            f"""SELECT COALESCE(s."Name",'Sin dato') AS name, COUNT(*) AS value
               FROM public."Users" u
               JOIN public."UserProfiles" up ON u."Id" = up."UserId"
               LEFT JOIN public."Settings" s ON up."GenderId" = s."Id"
               {dist_where}
               GROUP BY s."Name" ORDER BY value DESC""",
            dist_params,
        )
        gender_dist = [dict(r) for r in cur.fetchall()]

        cur.execute(
            f"""SELECT
                CASE
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 18 AND 22 THEN '18-22'
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 23 AND 27 THEN '23-27'
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 28 AND 32 THEN '28-32'
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 33 AND 37 THEN '33-37'
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) BETWEEN 38 AND 45 THEN '38-45'
                    WHEN EXTRACT(YEAR FROM AGE(up."BirthDate")) > 45 THEN '45+'
                    ELSE 'Sin dato'
                END AS name,
                COUNT(*) AS value
               FROM public."Users" u
               JOIN public."UserProfiles" up ON u."Id" = up."UserId"
               {dist_where}
               GROUP BY 1 ORDER BY 1""",
            dist_params,
        )
        age_dist = [dict(r) for r in cur.fetchall()]

        cur.execute(
            f"""SELECT COALESCE(d."DepartmentName",'Sin dato') AS name, COUNT(*) AS value
               FROM public."Users" u
               JOIN public."UserProfiles" up ON u."Id" = up."UserId"
               LEFT JOIN public."Cities" c ON up."CityId" = c."Id"
               LEFT JOIN public."Departments" d ON c."DepartmentId" = d."Id"
               {dist_where}
               GROUP BY d."DepartmentName" ORDER BY value DESC LIMIT 15""",
            dist_params,
        )
        dept_dist = [dict(r) for r in cur.fetchall()]

        cur.execute(
            f"""SELECT COALESCE(s."Name",'Sin dato') AS name, COUNT(*) AS value
               FROM public."Users" u
               JOIN public."UserProfiles" up ON u."Id" = up."UserId"
               LEFT JOIN public."Settings" s ON up."ProfessionsId" = s."Id"
               {dist_where} AND s."Name" IS NOT NULL
               GROUP BY s."Name" ORDER BY value DESC LIMIT 10""",
            dist_params,
        )
        profession_dist = [dict(r) for r in cur.fetchall()]

        # Distribución por estado civil
        cur.execute(
            f"""SELECT COALESCE(s."Name",'Sin dato') AS name, COUNT(*) AS value
               FROM public."Users" u
               JOIN public."UserProfiles" up ON u."Id" = up."UserId"
               LEFT JOIN public."Settings" s ON up."MaritalStatusId" = s."Id"
               {dist_where}
               GROUP BY s."Name" ORDER BY value DESC""",
            dist_params,
        )
        marital_dist = [dict(r) for r in cur.fetchall()]

        # Distribución por rango de ingreso
        cur.execute(
            f"""SELECT COALESCE(s."Name",'Sin dato') AS name, COUNT(*) AS value
               FROM public."Users" u
               JOIN public."UserProfiles" up ON u."Id" = up."UserId"
               LEFT JOIN public."Settings" s ON up."IncomeRangeId" = s."Id"
               {dist_where}
               GROUP BY s."Name" ORDER BY value DESC""",
            dist_params,
        )
        income_dist = [dict(r) for r in cur.fetchall()]

        # Registros por mes
        reg_date_cond = ""
        reg_params = []
        if date_from:
            reg_date_cond += ' AND "CreationDate"::date >= %s'
            reg_params.append(date_from)
        if date_to:
            reg_date_cond += ' AND "CreationDate"::date <= %s'
            reg_params.append(date_to)
        if not date_from and not date_to:
            reg_date_cond = ' AND "CreationDate" >= NOW() - INTERVAL \'12 months\''

        cur.execute(
            f"""SELECT TO_CHAR(DATE_TRUNC('month',"CreationDate"),'Mon YY') AS name,
                      COUNT(*) AS value
               FROM public."Users"
               WHERE "Deleted" IS DISTINCT FROM TRUE
               {reg_date_cond}
               GROUP BY DATE_TRUNC('month',"CreationDate"), name
               ORDER BY DATE_TRUNC('month',"CreationDate")""",
            reg_params,
        )
        monthly_reg = [dict(r) for r in cur.fetchall()]

        # Usuarios nuevos vs activos (últimos 6 meses o rango seleccionado)
        cur.execute(
            """SELECT
                TO_CHAR(DATE_TRUNC('month', gen_date), 'Mon YY') AS name,
                COALESCE(new_users.cnt, 0) AS nuevos,
                COALESCE(active_users.cnt, 0) AS activos
               FROM generate_series(
                   DATE_TRUNC('month', NOW() - INTERVAL '5 months'),
                   DATE_TRUNC('month', NOW()),
                   '1 month'::interval
               ) AS gen_date
               LEFT JOIN (
                   SELECT DATE_TRUNC('month',"CreationDate") AS m, COUNT(*) AS cnt
                   FROM public."Users" WHERE "Deleted" IS DISTINCT FROM TRUE
                   GROUP BY m
               ) new_users ON new_users.m = gen_date
               LEFT JOIN (
                   SELECT DATE_TRUNC('month',"LastSession") AS m, COUNT(*) AS cnt
                   FROM public."Users" WHERE "Deleted" IS DISTINCT FROM TRUE AND "LastSession" IS NOT NULL
                   GROUP BY m
               ) active_users ON active_users.m = gen_date
               ORDER BY gen_date"""
        )
        comparison = [dict(r) for r in cur.fetchall()]

    return {
        "summary": {
            "total_users": total_users,
            "new_this_month": new_this_month,
            "active_30d": active_30d,
            "with_profile": with_profile,
            "unverified": unverified,
            "incomplete": incomplete,
            "migrated": migrated,
        },
        "gender_distribution": gender_dist,
        "age_distribution": age_dist,
        "department_distribution": dept_dist,
        "monthly_registrations": monthly_reg,
        "profession_distribution": profession_dist,
        "monthly_comparison": comparison,
        "marital_distribution": marital_dist,
        "income_distribution": income_dist,
    }
