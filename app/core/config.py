from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DB_HOST: str
    DB_PORT: int = 5432
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str

    JWT_SECRET_KEY: str
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_HOURS: int = 8

    ADMIN_ROLE_NAME: str = "Admin"
    USER_ROLE_NAME: str = "User"

    class Config:
        env_file = ".env"


settings = Settings()
