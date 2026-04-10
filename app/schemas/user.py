from pydantic import BaseModel
from typing import Optional
from datetime import date


class UserFilters(BaseModel):
    # Búsqueda general
    search: Optional[str] = None          # nombre, apellido, email, teléfono
    user_ids: Optional[str] = None        # IDs separados por coma
    emails: Optional[str] = None          # emails separados por coma
    phones: Optional[str] = None          # teléfonos separados por coma
    instagram: Optional[str] = None

    # Demográficos
    gender_id: Optional[int] = None
    marital_status_id: Optional[int] = None
    role_house_id: Optional[int] = None
    income_range_id: Optional[int] = None
    profession_id: Optional[int] = None
    number_children_id: Optional[int] = None
    level_academic_id: Optional[int] = None
    frequency_activities_id: Optional[int] = None

    age_min: Optional[int] = None
    age_max: Optional[int] = None

    # Booleanos de perfil
    is_buy_manager_home: Optional[bool] = None
    is_pregnant: Optional[bool] = None
    is_interested_technology: Optional[bool] = None
    is_alcohol_consume: Optional[bool] = None
    is_tobacco_consume: Optional[bool] = None

    # Ubicación
    department_id: Optional[int] = None
    city_id: Optional[int] = None
    zone: Optional[int] = None

    # Fechas
    registered_from: Optional[date] = None
    registered_to: Optional[date] = None
    last_session_from: Optional[date] = None
    last_session_to: Optional[date] = None
    profile_updated_from: Optional[date] = None

    # Encuestas
    survey_id: Optional[int] = None
    survey_status_id: Optional[int] = None

    # Paginación
    page: int = 1
    page_size: int = 50
