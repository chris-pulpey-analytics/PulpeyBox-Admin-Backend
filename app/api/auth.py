from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.schemas.auth import LoginRequest, TokenResponse
from app.core.database import get_db
from app.core.security import verify_password, create_access_token, decode_token
from app.core.config import settings

router = APIRouter(prefix="/auth", tags=["auth"])
bearer_scheme = HTTPBearer()


@router.post("/login", response_model=TokenResponse)
def login(data: LoginRequest):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT u."Id", u."Name", u."LastName", u."Email",
                   u."PasswordHash", u."PasswordSalt",
                   u."Deleted", u."IsAccountValidated",
                   r."RolName"
            FROM public."Users" u
            JOIN public."Roles" r ON u."RoleId" = r."Id"
            WHERE LOWER(u."Email") = LOWER(%s)
            """,
            (data.email,),
        )
        user = cur.fetchone()

    if not user:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

    if user["Deleted"]:
        raise HTTPException(status_code=403, detail="Cuenta eliminada")

    if not verify_password(data.password, user["PasswordHash"], user["PasswordSalt"]):
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

    if user["RolName"].lower() != settings.ADMIN_ROLE_NAME.lower():
        raise HTTPException(status_code=403, detail="Acceso restringido a administradores")

    token = create_access_token(
        {"sub": str(user["Id"]), "email": user["Email"], "role": user["RolName"]}
    )

    return TokenResponse(
        access_token=token,
        user_name=f"{user['Name']} {user['LastName']}",
        email=user["Email"],
        role=user["RolName"],
    )


def get_current_admin(credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)):
    payload = decode_token(credentials.credentials)
    if not payload:
        raise HTTPException(status_code=401, detail="Token inválido o expirado")
    if payload.get("role", "").lower() != settings.ADMIN_ROLE_NAME.lower():
        raise HTTPException(status_code=403, detail="Acceso denegado")
    return payload
