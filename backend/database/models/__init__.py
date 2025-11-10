"""
Exporta todos os modelos de banco de dados.
"""

from backend.database.models.admin_user import AdminUser
from backend.database.models.api_variables import APIVariables
from backend.database.models.climate_data import ClimateData
from backend.database.models.user_cache import CacheMetadata, UserSessionCache
from backend.database.models.user_favorites import (
    FavoriteLocation,
    UserFavorites,
)
from backend.database.models.visitor_stats import VisitorStats

__all__ = [
    "AdminUser",
    "APIVariables",
    "ClimateData",
    "UserSessionCache",
    "CacheMetadata",
    "UserFavorites",
    "FavoriteLocation",
    "VisitorStats",
]
