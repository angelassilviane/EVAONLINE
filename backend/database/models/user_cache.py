"""
Modelos de banco de dados para gerenciamento de cache de usuários anônimos.

Armazena:
1. UserSessionCache: Metadados de sessão de usuário anônimo
2. CacheMetadata: Rastreamento de dados em cache por tipo
"""

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
)
from sqlalchemy.sql import func

from backend.database.connection import Base


class UserSessionCache(Base):
    """
    Rastreia sessões de usuários anônimos para cache de dados climáticos.

    Uma sessão representa um usuário anônimo que interage com a aplicação.
    Cada sessão pode ter múltiplas localizações em cache.

    Attributes:
        id: Chave primária auto-incrementada
        session_id: UUID única por sessão (formato: "sess_<uuid>")
        user_agent: User-Agent do navegador para identificação de cliente
        created_at: Data/hora de criação da sessão
        last_access: Última vez que a sessão foi acessada
        cache_size_mb: Tamanho aproximado de dados em cache (bytes)

    Indexes:
        idx_user_session_cache_session_id: Unique para busca rápida
        idx_user_session_cache_last_access: Para limpeza de sessões expiradas
    """

    __tablename__ = "user_session_cache"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(
        String(50),
        unique=True,
        nullable=False,
        index=True,
        comment="UUID única da sessão (formato: sess_<uuid>)",
    )
    user_agent = Column(
        String(500), nullable=True, comment="User-Agent do navegador"
    )
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        comment="Data/hora de criação",
    )
    last_access = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment="Última vez acessada",
    )
    cache_size_mb = Column(
        Float,
        nullable=False,
        default=0.0,
        comment="Tamanho em MB dos dados em cache",
    )

    def __repr__(self) -> str:
        try:
            # Tentar acessar como valores de instância
            sid = getattr(self, "session_id", "uninitialized")
            size = getattr(self, "cache_size_mb", 0.0)
            access = getattr(self, "last_access", None)

            session_id = (
                sid[:12] + "..."
                if sid and len(str(sid)) > 12
                else str(sid or "None")
            )
            cache_size = f"{size:.1f}"
            last_access = access.isoformat() if access else "None"

            return (
                f"<UserSessionCache("
                f"session_id='{session_id}', "
                f"cache_size={cache_size}, "
                f"accessed={last_access})>"
            )
        except Exception:
            # Fallback seguro
            return "<UserSessionCache(uninitialized)>"

    def to_dict(self) -> dict:
        """
        Converte para dicionário.

        Returns:
            dict: Dados da sessão
        """
        return {
            "id": self.id,
            "session_id": self.session_id,
            "user_agent": self.user_agent,
            "created_at": (
                self.created_at.isoformat() if self.created_at else None
            ),
            "last_access": (
                self.last_access.isoformat() if self.last_access else None
            ),
            "cache_size_mb": round(self.cache_size_mb, 2),
        }


class CacheMetadata(Base):
    """
    Metadados sobre dados climáticos em cache por sessão e localização.

    Permite rastreamento granular de:
    - Quais dados estão em cache para qual sessão
    - Tipo de dado (climate, elevation, etc)
    - Quando foi cacheado
    - TTL (time-to-live)

    Attributes:
        id: Chave primária auto-incrementada
        session_id: FK para UserSessionCache
        location_id: ID da localização (referência para world_locations)
        data_type: Tipo de dado (climate, elevation, historical)
        last_updated: Data/hora do último update do cache
        ttl: Time-to-live em segundos (padrão 3600 = 1h)
        data_source: Fonte dos dados (nasa_power, met_norway, nws_usa, data_fusion)

    Indexes:
        idx_cache_metadata_session_location: Para buscar por sessão + localização
        idx_cache_metadata_expires: Para limpeza de expirados
    """

    __tablename__ = "cache_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(
        String(50),
        ForeignKey("user_session_cache.session_id"),
        nullable=False,
        index=True,
        comment="Referência para sessão do usuário",
    )
    location_id = Column(
        Integer,
        nullable=False,
        index=True,
        comment="ID da localização (world_locations.id)",
    )
    data_type = Column(
        String(50),
        nullable=False,
        default="climate",
        comment="Tipo de dado: climate, elevation, historical, eto",
    )
    last_updated = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment="Quando foi atualizado no cache",
    )
    ttl = Column(
        Integer,
        nullable=False,
        default=3600,
        comment="Time-to-live em segundos (default 1h)",
    )
    data_source = Column(
        String(100),
        nullable=True,
        comment="Fonte: nasa_power, met_norway, nws_usa, data_fusion",
    )

    def __repr__(self) -> str:
        return (
            f"<CacheMetadata("
            f"session='{self.session_id[:12]}...', "
            f"location_id={self.location_id}, "
            f"type='{self.data_type}', "
            f"source='{self.data_source}')>"
        )

    def to_dict(self) -> dict:
        """
        Converte para dicionário.

        Returns:
            dict: Metadados do cache
        """
        return {
            "id": self.id,
            "session_id": self.session_id,
            "location_id": self.location_id,
            "data_type": self.data_type,
            "last_updated": (
                self.last_updated.isoformat() if self.last_updated else None
            ),
            "ttl": self.ttl,
            "data_source": self.data_source,
        }


# Índices para performance
Index(
    "idx_cache_metadata_session_location",
    CacheMetadata.session_id,
    CacheMetadata.location_id,
    unique=True,
    postgresql_using="btree",
)

Index(
    "idx_cache_metadata_expires",
    CacheMetadata.last_updated,
    postgresql_using="btree",
)


__all__ = ["UserSessionCache", "CacheMetadata"]
