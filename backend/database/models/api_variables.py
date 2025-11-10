"""
Modelo para mapeamento de variáveis climáticas de diferentes APIs.

Permite mapear nomes de variáveis específicos de cada fonte (API)
para nomes padronizados usados internamente pela aplicação.
"""

from sqlalchemy import (
    Boolean,
    Column,
    Index,
    Integer,
    String,
    UniqueConstraint,
)

from backend.database.connection import Base


class APIVariables(Base):
    """
    Mapeia variáveis disponíveis em cada API para nomes padronizados.

    Cada API (NASA POWER, Open-Meteo, MET Norway, etc.) usa nomenclatura
    diferente para as mesmas variáveis climáticas. Este modelo permite
    manter um mapeamento centralizado para harmonização de dados.

    Attributes:
        source_api: Nome da API fonte ('nasa_power', 'openmeteo_archive')
        variable_name: Nome da variável na API original
        standard_name: Nome padronizado interno (temp_max_c, etc.)
        unit: Unidade de medida (°C, m/s, MJ/m²/d, etc.)
        description: Descrição legível da variável
        is_required_for_eto: Se a variável é necessária para ETo

    Examples:
        # NASA POWER temperatura máxima
        APIVariables(
            source_api='nasa_power',
            variable_name='T2M_MAX',
            standard_name='temp_max_c',
            unit='°C',
            description='Temperatura máxima a 2 metros',
            is_required_for_eto=True
        )

        # Open-Meteo temperatura máxima
        APIVariables(
            source_api='openmeteo_archive',
            variable_name='temperature_2m_max',
            standard_name='temp_max_c',
            unit='°C',
            description='Temperatura máxima a 2 metros',
            is_required_for_eto=True
        )
    """

    __tablename__ = "api_variables"
    __table_args__ = (
        # Garante que não há duplicatas de variável por API
        UniqueConstraint(
            "source_api", "variable_name", name="uq_api_variable"
        ),
        # Índices para buscas rápidas
        Index("idx_source_api", "source_api"),
        Index("idx_standard_name", "standard_name"),
        Index("idx_required_eto", "is_required_for_eto"),
        # Schema público
        {"schema": "public"},
    )

    # === Identificação ===
    id = Column(Integer, primary_key=True, autoincrement=True)

    # === Mapeamento API ===
    source_api = Column(
        String(50),
        nullable=False,
        comment="API fonte (nasa_power, openmeteo_archive, etc.)",
    )

    variable_name = Column(
        String(100), nullable=False, comment="Nome da variável na API original"
    )

    # === Padronização ===
    standard_name = Column(
        String(100),
        nullable=False,
        comment="Nome padronizado interno (temp_max_c, etc.)",
    )

    unit = Column(
        String(50),
        nullable=False,
        comment="Unidade de medida (°C, m/s, MJ/m²/d, etc.)",
    )

    description = Column(
        String(500),
        nullable=True,
        comment="Descrição legível da variável climática",
    )

    # === Flags ===
    is_required_for_eto = Column(
        Boolean,
        nullable=False,
        default=False,
        comment="Se a variável é essencial para cálculo de ETo",
    )

    def __repr__(self):
        return (
            f"<APIVariables(source={self.source_api}, "
            f"var={self.variable_name}, "
            f"std={self.standard_name}, "
            f"eto_req={self.is_required_for_eto})>"
        )

    def to_dict(self):
        """Converte para dicionário."""
        return {
            "id": self.id,
            "source_api": self.source_api,
            "variable_name": self.variable_name,
            "standard_name": self.standard_name,
            "unit": self.unit,
            "description": self.description,
            "is_required_for_eto": self.is_required_for_eto,
        }
