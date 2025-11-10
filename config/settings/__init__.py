"""
Sistema de Configuracoes Moderno - EVAonline

Arquitetura modular com Pydantic Settings para maxima
robustez e manutenibilidade.
"""

from config.settings.app_config import (
    LegacySettingsAdapter,
    get_database_url,
    get_legacy_settings,
    get_redis_url,
    get_settings,
)

__all__ = [
    "get_settings",
    "get_database_url",
    "get_redis_url",
    "LegacySettingsAdapter",
    "get_legacy_settings",
]
