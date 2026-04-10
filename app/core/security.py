import hmac
import hashlib
from datetime import datetime, timedelta
from jose import jwt, JWTError
from app.core.config import settings


def verify_password(plain_password: str, stored_hash: bytes, stored_salt: bytes) -> bool:
    """
    Verifica contraseña usando HMACSHA512, compatible con la implementación en C#:
    new HMACSHA512(passwordSalt).ComputeHash(Encoding.UTF8.GetBytes(password))
    """
    h = hmac.new(bytes(stored_salt), plain_password.encode("utf-8"), hashlib.sha512)
    computed = h.digest()
    return hmac.compare_digest(computed, bytes(stored_hash))


def create_access_token(data: dict) -> str:
    payload = data.copy()
    expire = datetime.utcnow() + timedelta(hours=settings.JWT_EXPIRE_HOURS)
    payload.update({"exp": expire})
    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
    except JWTError:
        return None
