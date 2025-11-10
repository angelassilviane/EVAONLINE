"""
Modelo de banco de dados para armazenamento de dados climáticos.

Suporta 6 fontes de dados com JSONB flexível:
- nasa_power: NASA POWER (1981+, histórico global)
- openmeteo_archive: Open-Meteo Archive (1940+, histórico global)
- openmeteo_forecast: Open-Meteo Forecast (-30d a +16d, híbrido)
- met_norway: MET Norway (hoje a +5d, forecast nórdico)
- nws_forecast: NWS Forecast (hoje a +7d, forecast USA)
- nws_stations: NWS Stations (hoje-1d a hoje, real-time USA)
"""

from datetime import datetime

from sqlalchemy import Column, DateTime, Float, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB

from backend.database.connection import Base


class ClimateData(Base):
    """
    Modelo MODERNO para armazenamento flexível de dados climáticos
    de múltiplas APIs.

    Usa campos JSONB para suportar diferentes esquemas de variáveis
    de cada fonte. Permite harmonização de dados e cálculo de ETo
    independente da fonte.

    Attributes:
        source_api: Nome da API fonte ('nasa_power', etc.)
        latitude: Latitude da localização
        longitude: Longitude da localização
        elevation: Elevação em metros (crucial para cálculo de ETo)
        timezone: Timezone IANA (ex: 'America/Sao_Paulo')
        date: Data dos dados climáticos
        raw_data: Dados originais da API em formato JSONB (flexível)
        harmonized_data: Dados normalizados em formato padronizado
        eto_mm_day: Evapotranspiração de referência calculada (mm/dia)
        eto_method: Método usado para cálculo (penman_monteith, etc.)
        quality_flags: Flags de qualidade dos dados
        processing_metadata: Metadados sobre o processamento

    Examples:
        # Dados NASA POWER
        ClimateData(
            source_api='nasa_power',
            raw_data={
                'T2M_MAX': 28.5,
                'T2M_MIN': 18.2,
                'RH2M': 65.0,
                'WS2M': 3.2,
                'ALLSKY_SFC_SW_DWN': 20.5
            },
            harmonized_data={
                'temp_max_c': 28.5,
                'temp_min_c': 18.2,
                'humidity_percent': 65.0,
                'wind_speed_ms': 3.2,
                'solar_radiation_mjm2': 20.5
            }
        )

        # Dados Open-Meteo
        ClimateData(
            source_api='openmeteo_archive',
            raw_data={
                'temperature_2m_max': 28.5,
                'temperature_2m_min': 18.2,
                'relative_humidity_2m_mean': 65.0,
                'wind_speed_10m_max': 3.2,
                'shortwave_radiation_sum': 20.5
            },
            harmonized_data={
                'temp_max_c': 28.5,
                'temp_min_c': 18.2,
                'humidity_percent': 65.0,
                'wind_speed_ms': 3.2,
                'solar_radiation_mjm2': 20.5
            }
        )
    """

    __tablename__ = "climate_data"
    __table_args__ = (
        # Índices compostos para otimização
        Index("idx_climate_location_date", "latitude", "longitude", "date"),
        Index("idx_climate_source_date", "source_api", "date"),
        Index("idx_climate_date", "date"),
        # Schema público
        {"schema": "public"},
    )

    # === Identificação ===
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_api = Column(
        String(50), nullable=False, index=True, comment="Fonte da API"
    )

    # === Localização ===
    latitude = Column(
        Float, nullable=False, comment="Latitude em graus decimais"
    )
    longitude = Column(
        Float, nullable=False, comment="Longitude em graus decimais"
    )
    elevation = Column(
        Float, nullable=True, comment="Elevação em metros (crucial para ETo)"
    )
    timezone = Column(
        String(50),
        nullable=True,
        comment="Timezone IANA (ex: America/Sao_Paulo)",
    )

    # === Temporal ===
    date = Column(
        DateTime, nullable=False, comment="Data dos dados climáticos"
    )
    created_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        comment="Data de criação do registro",
    )
    updated_at = Column(
        DateTime,
        nullable=True,
        onupdate=datetime.utcnow,
        comment="Data de atualização",
    )

    # === Dados Climáticos (JSONB - Flexível) ===
    raw_data = Column(
        JSONB,
        nullable=False,
        comment="Dados originais da API em formato nativo",
    )

    harmonized_data = Column(
        JSONB,
        nullable=True,
        comment="Dados harmonizados em formato padronizado",
    )

    # === Resultado ETo ===
    eto_mm_day = Column(
        Float,
        nullable=True,
        comment="Evapotranspiração de referência (mm/dia)",
    )
    eto_method = Column(
        String(20),
        nullable=True,
        default="penman_monteith",
        comment="Método de cálculo de ETo",
    )

    # === Metadados ===
    quality_flags = Column(
        JSONB,
        nullable=True,
        comment="Flags de qualidade: missing_data, interpolated, etc.",
    )

    processing_metadata = Column(
        JSONB,
        nullable=True,
        comment="Metadados: versão, tempo de processamento, etc.",
    )

    def __repr__(self):
        return (
            f"<ClimateData(id={self.id}, source={self.source_api}, "
            f"lat={self.latitude}, lon={self.longitude}, "
            f"date={self.date.strftime('%Y-%m-%d') if self.date else None}, "
            f"eto={self.eto_mm_day})>"
        )

    def to_dict(self):
        """Converte para dicionário (útil para APIs)."""
        return {
            "id": self.id,
            "source_api": self.source_api,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "elevation": self.elevation,
            "timezone": self.timezone,
            "date": self.date.isoformat() if self.date else None,
            "raw_data": self.raw_data,
            "harmonized_data": self.harmonized_data,
            "eto_mm_day": self.eto_mm_day,
            "eto_method": self.eto_method,
            "quality_flags": self.quality_flags,
            "created_at": (
                self.created_at.isoformat() if self.created_at else None
            ),
        }
