"""
Módulo de serviços de banco de dados para API.
Re-exporta funcionalidades do módulo database principal.
"""

from backend.database.connection import (
    Base,
    SessionLocal,
    engine,
    get_db_context,
)


def get_db():
    """
    Dependency para FastAPI - fornece sessão de banco de dados.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Re-exportar para compatibilidade
__all__ = ["get_db", "get_db_context", "engine", "Base"]
