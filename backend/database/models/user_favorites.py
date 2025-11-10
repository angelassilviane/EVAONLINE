"""
Modelos de banco de dados para sistema de favoritos de usuários anônimos.

Armazena:
1. UserFavorites: Coleção de favoritos por sessão/usuário
2. FavoriteLocation: Localização individual no favorito
"""

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.sql import func

from backend.database.connection import Base


class UserFavorites(Base):
    """
    Coleção de favoritos para um usuário (anônimo ou autenticado).

    Uma coleção de favoritos pertence a uma sessão (usuário anônimo)
    ou a um usuário autenticado. Permite sincronização entre localStorage
    e backend.

    Attributes:
        id: Chave primária auto-incrementada
        session_id: ID da sessão anônima (único se user_id é NULL)
        user_id: ID do usuário autenticado (único se session_id é NULL)
        created_at: Data/hora de criação
        updated_at: Data/hora da última atualização

    Constraints:
        - Máximo 20 favoritos por coleção (verificado em lógica de aplicação)
        - Apenas um de session_id ou user_id pode ser preenchido

    Indexes:
        idx_user_favorites_session: Para busca rápida por sessão
        idx_user_favorites_user: Para busca rápida por usuário
    """

    __tablename__ = "user_favorites"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(
        String(50),
        nullable=True,
        unique=True,
        comment="Sessão de usuário anônimo (mutualmente exclusivo com user_id)",
    )
    user_id = Column(
        Integer,
        nullable=True,
        unique=True,
        comment="ID de usuário autenticado (mutualmente exclusivo com session_id)",
    )
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        comment="Data/hora de criação",
    )
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment="Data/hora de última atualização",
    )

    def __repr__(self) -> str:
        try:
            # Acessar atributos com segurança
            sid = getattr(self, "session_id", None)
            uid = getattr(self, "user_id", None)
            updated = getattr(self, "updated_at", None)

            identifier = (
                f"session='{sid[:12]}...'" if sid else f"user_id={uid}"
            )
            updated_str = updated.isoformat() if updated else "None"

            return (
                f"<UserFavorites(" f"{identifier}, " f"updated={updated_str})>"
            )
        except Exception:
            return "<UserFavorites(uninitialized)>"

    def to_dict(self) -> dict:
        """
        Converte para dicionário.

        Returns:
            dict: Dados da coleção de favoritos
        """
        return {
            "id": self.id,
            "session_id": self.session_id,
            "user_id": self.user_id,
            "created_at": (
                self.created_at.isoformat() if self.created_at else None
            ),
            "updated_at": (
                self.updated_at.isoformat() if self.updated_at else None
            ),
        }


class FavoriteLocation(Base):
    """
    Uma localização marcada como favorito.

    Cada favorito referencia uma localização e pertence a uma coleção.
    Suporta anotações opcionais e rastreamento de quando foi marcado
    como favorito.

    Attributes:
        id: Chave primária auto-incrementada
        user_favorites_id: FK para UserFavorites
        location_id: ID da localização (referência para world_locations)
        added_at: Data/hora em que foi marcado como favorito
        notes: Anotações opcionais do usuário (max 500 chars)

    Constraints:
        - Máximo 20 registros por UserFavorites
        - Unique constraint em (user_favorites_id, location_id)

    Indexes:
        idx_favorite_location_user_favorites: Para busca rápida
        idx_favorite_location_location: Para análise de favoritos populares
    """

    __tablename__ = "favorite_location"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_favorites_id = Column(
        Integer,
        ForeignKey("user_favorites.id"),
        nullable=False,
        index=True,
        comment="Referência para coleção de favoritos",
    )
    location_id = Column(
        Integer,
        nullable=False,
        index=True,
        comment="ID da localização (world_locations.id)",
    )
    added_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        comment="Data/hora em que foi adicionado aos favoritos",
    )
    notes = Column(
        Text,
        nullable=True,
        comment="Anotações opcionais do usuário (max 500 chars)",
    )

    def __repr__(self) -> str:
        try:
            # Acessar atributos com segurança
            loc_id = getattr(self, "location_id", "uninitialized")
            added = getattr(self, "added_at", None)

            added_str = added.isoformat() if added else "None"

            return (
                f"<FavoriteLocation("
                f"location_id={loc_id}, "
                f"added={added_str})>"
            )
        except Exception:
            return "<FavoriteLocation(uninitialized)>"

    def to_dict(self) -> dict:
        """
        Converte para dicionário.

        Returns:
            dict: Dados do favorito
        """
        return {
            "id": self.id,
            "user_favorites_id": self.user_favorites_id,
            "location_id": self.location_id,
            "added_at": self.added_at.isoformat() if self.added_at else None,
            "notes": self.notes,
        }


# Índices para performance
Index(
    "idx_favorite_location_user_favorites",
    FavoriteLocation.user_favorites_id,
    FavoriteLocation.location_id,
    unique=True,
    postgresql_using="btree",
)

Index(
    "idx_favorite_location_popular",
    FavoriteLocation.location_id,
    postgresql_using="btree",
)


__all__ = ["UserFavorites", "FavoriteLocation"]
