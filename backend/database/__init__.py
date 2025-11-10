"""
Inicializa e configura o banco de dados.
"""

from loguru import logger

from backend.database.connection import Base, engine


def init_db():
    """
    Inicializa o banco de dados e cria as tabelas.
    """
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Tabelas criadas com sucesso!")
    except Exception as e:
        logger.error(f"❌ Erro ao criar tabelas: {e}")
        raise
