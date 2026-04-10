from fastapi import APIRouter, Depends
from app.api.auth import get_current_admin
from app.core.database import get_db

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("")
def get_metrics(_: dict = Depends(get_current_admin)):
    with get_db() as conn:
        cur = conn.cursor()

        cur.execute('SELECT COUNT(*) FROM public."Users" WHERE "Deleted" IS DISTINCT FROM TRUE')
        total_users = cur.fetchone()["count"]

        cur.execute(
            """SELECT COUNT(*) FROM public."Users"
               WHERE "Deleted" IS DISTINCT FROM TRUE
               AND DATE_TRUNC('month',"CreationDate") = DATE_TRUNC('month',NOW())"""
        )
        new_this_month = cur.fetchone()["count"]

        cur.execute(
            """SELECT COUNT(*) FROM public."Users"
               WHERE "Deleted" IS DISTINCT FROM TRUE
               AND "LastSession" >= NOW() - INTERVAL '30 days'"""
        )
        active_30d = cur.fetchone()["count"]

        cur.execute(
            """SELECT COUNT(*) FROM public."Users" u
               JOIN public."UserProfiles" up ON u."Id" = up."UserId"
               WHERE u."Deleted" IS DISTINCT FROM TRUE
               AND up."BirthDate" IS NOT NULL"""
        )
        with_profile = cur.fetchone()["count"]

        # Sin verificar email
        cur.execute(
            """SELECT COUNT(*) FROM public."Users"
               WHERE "Deleted" IS DISTINCT FROM TRUE
               AND "IsAccountValidated" IS DISTINCT FROM TRUE"""
        )
        unverified = cur.fetchone()["count"]

        # Registro incompleto (sin profile o sin fecha de nacimiento)
        cur.execute(
            """SELECT COUNT(*) FROM public."Users" u
               LEFT JOIN public."UserProfiles" up ON u."Id" = up."UserId"
               WHERE u."Deleted" IS DISTINCT FROM TRUE
               AND (up."Id" IS NULL OR up."BirthDate" IS NULL)"""
        )
        incomplete = cur.fetchone()["count"]

        # Migrados
        cur.execute(
            """SELECT COUNT(*) FROM public."Users"
               WHERE "Deleted" IS DISTINCT FROM TRUE AND "IsMigrated" IS TRUE"""
        )
        migrated = cur.fetchone()["count"]

        # Distribución por género
        cur.execute(
            """SELECT COALESCE(s."Name",'Sin dato') AS name, COUNT(*) AS value
               FROM public."Users" u
               JOIN public."UserProfiles" up ON u."Id" = up."UserId"
               LEFT JOIN public."Settings" s ON up."GenderId" = s."Id"
               WHERE u."Deleted" IS DISTINCT FROM TRUE
               GROUP BY s."Name" ORDER BY value DESC"""
        )
        gender_dist = [dict(r) for r in cur.fetchall()]

        # Distribución por rango de edad
        cur.execute(
            """SELECT
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
               WHERE u."Deleted" IS DISTINCT FROM TRUE
               GROUP BY 1 ORDER BY 1"""
        )
        age_dist = [dict(r) for r in cur.fetchall()]

        # Distribución por departamento
        cur.execute(
            """SELECT COALESCE(d."DepartmentName",'Sin dato') AS name, COUNT(*) AS value
               FROM public."Users" u
               JOIN public."UserProfiles" up ON u."Id" = up."UserId"
               LEFT JOIN public."Cities" c ON up."CityId" = c."Id"
               LEFT JOIN public."Departments" d ON c."DepartmentId" = d."Id"
               WHERE u."Deleted" IS DISTINCT FROM TRUE
               GROUP BY d."DepartmentName" ORDER BY value DESC LIMIT 15"""
        )
        dept_dist = [dict(r) for r in cur.fetchall()]

        # Registros por mes (últimos 12 meses)
        cur.execute(
            """SELECT TO_CHAR(DATE_TRUNC('month',"CreationDate"),'Mon YY') AS name,
                      COUNT(*) AS value
               FROM public."Users"
               WHERE "Deleted" IS DISTINCT FROM TRUE
               AND "CreationDate" >= NOW() - INTERVAL '12 months'
               GROUP BY DATE_TRUNC('month',"CreationDate"), name
               ORDER BY DATE_TRUNC('month',"CreationDate")"""
        )
        monthly_reg = [dict(r) for r in cur.fetchall()]

        # Top profesiones
        cur.execute(
            """SELECT COALESCE(s."Name",'Sin dato') AS name, COUNT(*) AS value
               FROM public."Users" u
               JOIN public."UserProfiles" up ON u."Id" = up."UserId"
               LEFT JOIN public."Settings" s ON up."ProfessionsId" = s."Id"
               WHERE u."Deleted" IS DISTINCT FROM TRUE AND s."Name" IS NOT NULL
               GROUP BY s."Name" ORDER BY value DESC LIMIT 10"""
        )
        profession_dist = [dict(r) for r in cur.fetchall()]

        # Usuarios nuevos vs activos últimos 6 meses (para gráfica comparativa)
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
    }
