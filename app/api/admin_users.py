from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from app.api.auth import get_current_admin, require_admin
from app.core.database import get_db
from app.core.security import create_password
from app.core.config import settings

router = APIRouter(prefix="/admin-users", tags=["admin-users"])


class RoleCreate(BaseModel):
    rol_name: str
    description: Optional[str] = ""


# ─── Roles CRUD ──────────────────────────────────────────────────────────────

@router.post("/roles")
def create_role(data: RoleCreate, _: dict = Depends(require_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'INSERT INTO public."Roles" ("RolName", "Description", "CreationDate", "Deleted") VALUES (%s, %s, NOW(), FALSE) RETURNING "Id"',
            (data.rol_name, data.description or ""),
        )
        new_id = cur.fetchone()["Id"]
        conn.commit()
    return {"id": new_id, "message": "Rol creado"}


@router.put("/roles/{role_id}")
def update_role(role_id: int, data: RoleCreate, _: dict = Depends(require_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'SELECT "RolName" FROM public."Roles" WHERE "Id"=%s',
            (role_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Rol no encontrado")
        if row["RolName"].lower() == settings.USER_ROLE_NAME.lower():
            raise HTTPException(400, "No se puede modificar el rol de usuario básico")
        cur.execute(
            'UPDATE public."Roles" SET "RolName"=%s, "Description"=%s WHERE "Id"=%s',
            (data.rol_name, data.description or "", role_id),
        )
        conn.commit()
    return {"message": "Rol actualizado"}


@router.delete("/roles/{role_id}")
def delete_role(role_id: int, _: dict = Depends(require_admin)):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT "RolName" FROM public."Roles" WHERE "Id"=%s', (role_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Rol no encontrado")
        if row["RolName"].lower() in (settings.USER_ROLE_NAME.lower(), settings.ADMIN_ROLE_NAME.lower()):
            raise HTTPException(400, "No se puede eliminar roles del sistema")
        cur.execute('SELECT COUNT(*) FROM public."Users" WHERE "RoleId"=%s', (role_id,))
        if cur.fetchone()["count"] > 0:
            raise HTTPException(400, "El rol tiene usuarios asignados, reasígnalos primero")
        cur.execute('DELETE FROM public."Roles" WHERE "Id"=%s', (role_id,))
        conn.commit()
    return {"message": "Rol eliminado"}


class AdminUserCreate(BaseModel):
    name: str
    last_name: str
    email: str
    password: str
    role_id: int
    mobil_number: str = ""
    mobil_number_code_id: Optional[int] = None  # si None, se busca el primer código disponible


class AdminUserUpdate(BaseModel):
    name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    role_id: Optional[int] = None
    mobil_number: Optional[str] = None


class PasswordReset(BaseModel):
    new_password: str


@router.get("/roles")
def get_roles(_: dict = Depends(get_current_admin)):
    """Lista todos los roles disponibles excepto el rol de usuario básico."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'SELECT "Id", "RolName", COALESCE("Description", \'\') AS "Description" FROM public."Roles" WHERE LOWER("RolName") != LOWER(%s) ORDER BY "Id"',
            (settings.USER_ROLE_NAME,),
        )
        return [dict(r) for r in cur.fetchall()]


@router.get("")
def list_admin_users(_: dict = Depends(get_current_admin)):
    """Lista todos los usuarios del panel admin (cualquier rol excepto User)."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT u."Id", u."Name", u."LastName", u."Email",
                   u."IsAccountValidated", u."Deleted",
                   r."Id" AS role_id, r."RolName" AS role_name,
                   u."CreationDate", u."LastSession"
            FROM public."Users" u
            JOIN public."Roles" r ON u."RoleId" = r."Id"
            WHERE LOWER(r."RolName") != LOWER(%s)
            ORDER BY u."CreationDate" DESC
            """,
            (settings.USER_ROLE_NAME,),
        )
        return [dict(r) for r in cur.fetchall()]


@router.post("")
def create_admin_user(data: AdminUserCreate, _: dict = Depends(require_admin)):
    """Crea un nuevo usuario del panel admin. Solo Admin puede hacerlo."""
    pwd_hash, pwd_salt = create_password(data.password)
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'SELECT "Id" FROM public."Users" WHERE LOWER("Email") = LOWER(%s)',
            (data.email,),
        )
        if cur.fetchone():
            raise HTTPException(400, "El email ya está registrado")

        # Resolver MobilNumberCodeId: usar el proporcionado o el primero disponible en Settings
        code_id = data.mobil_number_code_id
        if not code_id:
            cur.execute('SELECT "Id" FROM public."Settings" ORDER BY "Id" LIMIT 1')
            row = cur.fetchone()
            code_id = row["Id"] if row else 1

        cur.execute(
            """
            INSERT INTO public."Users"
                ("Name","LastName","Email","PasswordHash","PasswordSalt","RoleId",
                 "MobilNumber","MobilNumberCodeId","Status",
                 "IsAccountValidated","Deleted","CreationDate")
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1,TRUE,FALSE,NOW())
            RETURNING "Id"
            """,
            (data.name, data.last_name, data.email, pwd_hash, pwd_salt, data.role_id,
             data.mobil_number or "", code_id),
        )
        new_id = cur.fetchone()["Id"]
        conn.commit()
    return {"id": new_id, "message": "Usuario admin creado exitosamente"}


@router.put("/{user_id}")
def update_admin_user(user_id: int, data: AdminUserUpdate, _: dict = Depends(require_admin)):
    """Actualiza datos de un usuario admin."""
    with get_db() as conn:
        cur = conn.cursor()
        updates = []
        params = []
        if data.name is not None:
            updates.append('"Name"=%s')
            params.append(data.name)
        if data.last_name is not None:
            updates.append('"LastName"=%s')
            params.append(data.last_name)
        if data.email is not None:
            updates.append('"Email"=%s')
            params.append(data.email)
        if data.role_id is not None:
            updates.append('"RoleId"=%s')
            params.append(data.role_id)
        if data.mobil_number is not None:
            updates.append('"MobilNumber"=%s')
            params.append(data.mobil_number)
        if updates:
            params.append(user_id)
            cur.execute(
                f'UPDATE public."Users" SET {",".join(updates)} WHERE "Id"=%s',
                params,
            )
            conn.commit()
    return {"message": "Usuario actualizado"}


@router.post("/{user_id}/reset-password")
def reset_password(user_id: int, data: PasswordReset, _: dict = Depends(require_admin)):
    """Resetea la contraseña de un usuario admin."""
    pwd_hash, pwd_salt = create_password(data.new_password)
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'SELECT "Id" FROM public."Users" WHERE "Id"=%s',
            (user_id,),
        )
        if not cur.fetchone():
            raise HTTPException(404, "Usuario no encontrado")
        cur.execute(
            'UPDATE public."Users" SET "PasswordHash"=%s,"PasswordSalt"=%s WHERE "Id"=%s',
            (pwd_hash, pwd_salt, user_id),
        )
        conn.commit()
    return {"message": "Contraseña actualizada"}


@router.patch("/{user_id}/toggle-active")
def toggle_active(user_id: int, _: dict = Depends(require_admin)):
    """Activa o desactiva un usuario admin."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('SELECT "Deleted" FROM public."Users" WHERE "Id"=%s', (user_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Usuario no encontrado")
        new_state = not bool(row["Deleted"])
        cur.execute(
            'UPDATE public."Users" SET "Deleted"=%s WHERE "Id"=%s',
            (new_state, user_id),
        )
        conn.commit()
    return {"deleted": new_state, "message": "Estado actualizado"}


@router.delete("/{user_id}")
def delete_admin_user(user_id: int, _: dict = Depends(require_admin)):
    """Elimina (soft delete) un usuario admin."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            'UPDATE public."Users" SET "Deleted"=TRUE WHERE "Id"=%s',
            (user_id,),
        )
        conn.commit()
    return {"message": "Usuario eliminado"}
