"""
Mapa mundial interativo usando Dash Leaflet.

Features:
- Mapa base OpenStreetMap
- Detecção de cliques para capturar coordenadas
- Marcadores customizados
- Popup/Tooltip com informações do local
- Integração com sistema de favoritos
"""

import logging

import dash_leaflet as dl
from dash import html

logger = logging.getLogger(__name__)


def create_world_map():
    """
    Cria mapa mundial interativo com Dash Leaflet.

    Returns:
        dl.Map: Componente do mapa Leaflet com suporte a eventos de clique
    """
    print("=" * 80)
    print("🔵 CREATE_WORLD_MAP() EXECUTANDO AGORA!")
    print("=" * 80)
    logger.info("🔄 Criando mapa mundial com camadas brasileiras...")

    # Configuração inicial do mapa
    initial_center = [0, 0]  # Centro do mundo (lat, lon)
    initial_zoom = 2  # Zoom inicial para visualizar o globo

    # Camada base: OpenStreetMap
    tile_layer = dl.TileLayer(
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">'
        "OpenStreetMap</a> contributors",
        maxZoom=18,
        minZoom=2,
        noWrap=True,  # ✅ Evita repetição infinita do mapa nos lados
    )

    # Layer Group para marcadores (será atualizado via callback)
    marker_layer = dl.LayerGroup(id="marker-layer", children=[])

    # ✅ NOVO: LocateControl para geolocalização automática do usuário
    locate_control = dl.LocateControl(
        locateOptions={
            "enableHighAccuracy": True,
            "maxZoom": 16,
            "timeout": 10000,
        },
        position="topleft",
        strings={
            "title": "📍 Minha Localização",
        },
        flyTo=True,
    )

    # =========== CAMADAS CONTEXTUAIS (Opcionais via LayersControl) ===========
    # ✅ SOLUÇÃO: Criar FeatureGroups VAZIOS - Callbacks preencherão dinamicamente
    print("\n🔄 Criando FeatureGroups vazios para controle dinâmico...")
    logger.info("🔄 Criando FeatureGroups vazios para controle por callbacks")

    # ✅ FeatureGroups vazios - serão preenchidos pelos callbacks
    brasil_feature = dl.FeatureGroup(children=[], id="brasil-feature-group")
    matopiba_feature = dl.FeatureGroup(
        children=[], id="matopiba-feature-group"
    )
    cities_feature = dl.FeatureGroup(children=[], id="cities-feature-group")
    # Piracicaba começa marcado, então vamos carregar o conteúdo inicial
    piracicaba_marker = load_piracicaba_marker()
    piracicaba_feature = dl.FeatureGroup(
        children=[piracicaba_marker] if piracicaba_marker else [],
        id="piracicaba-feature-group",
    )

    # ✅ SOLUÇÃO: Adicionar FeatureGroups diretamente ao mapa (SEM LayersControl)
    # O LayersControl tem bug com GeoJSON bool(), então usamos controle custom
    map_children = [tile_layer, marker_layer, locate_control]

    # Adicionar FeatureGroups vazios ao mapa
    map_children.append(brasil_feature)
    map_children.append(matopiba_feature)
    map_children.append(cities_feature)
    map_children.append(piracicaba_feature)

    print("✅ Brasil FeatureGroup (vazio) adicionado ao mapa")
    print("✅ MATOPIBA FeatureGroup (vazio) adicionado ao mapa")
    print("✅ Cities FeatureGroup (vazio) adicionado ao mapa")
    print("✅ Piracicaba FeatureGroup (com marcador) adicionado ao mapa")

    print(f"\n✅ Total: {len(map_children)} elementos no mapa")
    logger.info(f"✅ {len(map_children)} FeatureGroups vazios criados")

    # ✅ IMPORTANTE: Usar dl.Map (não MapContainer) para capturar eventos
    # dl.Map tem suporte a clickData, dl.MapContainer NÃO tem
    map_component = dl.Map(
        id="world-map",
        center=initial_center,
        zoom=initial_zoom,
        children=map_children,
        style={
            "width": "100%",
            "height": "600px",  # Aumentado de 400px para 600px
            "borderRadius": "8px",
        },
        # ✅ Configurações para evitar duplicação do mapa mundial
        worldCopyJump=False,  # Desabilita o "pulo" entre cópias do mundo
        maxBounds=[
            [-90, -180],
            [90, 180],
        ],  # Limita visualização a 1 mapa mundial
        maxBoundsViscosity=1.0,  # Impede arrastar fora dos limites
        # ✅ dl.Map gera eventos clickData = {"latlng": {"lat": X, "lng": Y}}
    )

    # ✅ SOLUÇÃO: Criar controle customizado de camadas
    custom_control = create_custom_layer_control()

    # ✅ Retornar mapa + controle customizado em um container
    from dash import html

    map_with_control = html.Div(
        [map_component, custom_control],
        style={"position": "relative", "width": "100%"},
    )

    return map_with_control


def create_map_marker(lat, lon, label="Local Selecionado"):
    """
    Cria marcador customizado para o mapa.

    Args:
        lat (float): Latitude
        lon (float): Longitude
        label (str): Texto do tooltip

    Returns:
        dl.Marker: Marcador Leaflet
    """
    # ✅ Usar ícone padrão do Leaflet (não precisa de arquivo local)
    # O Leaflet CDN fornece ícones automaticamente
    marker = dl.Marker(
        position=[lat, lon],
        children=[
            dl.Tooltip(label),
            dl.Popup(
                html.H6(
                    f"📍 {label}",
                    style={
                        "margin": "8px",
                        "color": "#2c3e50",
                        "fontWeight": "500",
                    },
                )
            ),
        ],
        # Não especificar icon - usa o padrão do Leaflet
    )

    return marker


def create_circle_marker(lat, lon, color="blue", radius=10):
    """
    Cria marcador circular simples (sem ícone customizado).

    Args:
        lat (float): Latitude
        lon (float): Longitude
        color (str): Cor do marcador
        radius (int): Raio do círculo em pixels

    Returns:
        dl.CircleMarker: Marcador circular
    """
    return dl.CircleMarker(
        center=[lat, lon],
        radius=radius,
        color=color,
        fillColor=color,
        fillOpacity=0.6,
    )


def create_location_info_popup(location_data):
    """
    Cria conteúdo HTML para popup com informações do local.

    Args:
        location_data (dict): Dados da localização com keys:
            - lat: Latitude
            - lon: Longitude
            - city: Cidade (opcional)
            - country: País (opcional)
            - timezone: Fuso horário (opcional)
            - elevation: Altitude (opcional)

    Returns:
        html.Div: Conteúdo do popup
    """
    lat = location_data.get("lat", 0)
    lon = location_data.get("lon", 0)
    city = location_data.get("city", "Local desconhecido")
    country = location_data.get("country", "")
    timezone = location_data.get("timezone", "N/A")
    elevation = location_data.get("elevation")

    # Formatação de coordenadas em DMS (Graus, Minutos, Segundos)
    lat_dms = format_coordinate_dms(lat, "lat")
    lon_dms = format_coordinate_dms(lon, "lon")

    popup_content = html.Div(
        [
            html.H6(
                f"📍 {city}",
                style={"marginBottom": "8px", "color": "#2c3e50"},
            ),
            (
                html.P(
                    f"🌍 {country}",
                    style={"marginBottom": "4px", "fontSize": "14px"},
                )
                if country
                else None
            ),
            html.Hr(style={"margin": "8px 0"}),
            html.P(
                [
                    html.Strong("Coordenadas:"),
                    html.Br(),
                    html.Small(f"{lat_dms}"),
                    html.Br(),
                    html.Small(f"{lon_dms}"),
                    html.Br(),
                    html.Small(
                        f"({lat:.4f}°, {lon:.4f}°)",
                        className="text-muted",
                    ),
                ],
                style={"fontSize": "12px", "marginBottom": "8px"},
            ),
            html.P(
                [html.Strong("Fuso: "), f"{timezone}"],
                style={"fontSize": "12px", "marginBottom": "4px"},
            ),
            (
                html.P(
                    [html.Strong("Altitude: "), f"{elevation} m"],
                    style={"fontSize": "12px", "marginBottom": "4px"},
                )
                if elevation
                else None
            ),
        ],
        style={"minWidth": "250px", "padding": "8px"},
    )

    return popup_content


def format_coordinate_dms(decimal_degree, coord_type="lat"):
    """
    Converte coordenada decimal para formato DMS (Graus, Minutos, Segundos).

    Args:
        decimal_degree (float): Coordenada em decimal
        coord_type (str): 'lat' ou 'lon'

    Returns:
        str: Coordenada formatada (ex: "23°32'51.6"S")
    """
    is_positive = decimal_degree >= 0
    abs_degree = abs(decimal_degree)

    # Calcular graus, minutos, segundos
    degrees = int(abs_degree)
    minutes = int((abs_degree - degrees) * 60)
    seconds = ((abs_degree - degrees) * 60 - minutes) * 60

    # Determinar direção (N/S/E/W)
    if coord_type == "lat":
        direction = "N" if is_positive else "S"
    else:  # lon
        direction = "E" if is_positive else "W"

    return f"{degrees}°{minutes}'{seconds:.1f}\"{direction}"


def create_map_controls():
    """
    Cria controles adicionais para o mapa (zoom, layers, etc).

    Returns:
        html.Div: Controles do mapa
    """
    controls = html.Div(
        [
            html.Div(
                [
                    html.Label("Estilo do Mapa:", className="fw-bold mb-2"),
                    html.Select(
                        id="map-style-selector",
                        options=[
                            {
                                "label": "🗺️ OpenStreetMap (Padrão)",
                                "value": "osm",
                            },
                            {
                                "label": "🌍 OpenTopoMap (Topográfico)",
                                "value": "topo",
                            },
                            {
                                "label": "🛰️ Esri Satellite",
                                "value": "satellite",
                            },
                        ],
                        value="osm",
                        className="form-select form-select-sm",
                    ),
                ],
                className="mb-3",
            ),
        ]
    )

    return controls


# Estilos de mapa disponíveis (para futura expansão)
MAP_STYLES = {
    "osm": {
        "url": "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
        "attribution": "&copy; OpenStreetMap contributors",
    },
    "topo": {
        "url": "https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
        "attribution": "&copy; OpenTopoMap contributors",
    },
    "satellite": {
        "url": (
            "https://server.arcgisonline.com/ArcGIS/rest/services/"
            "World_Imagery/MapServer/tile/{z}/{y}/{x}"
        ),
        "attribution": "&copy; Esri",
    },
}


# ================== CAMADAS CONTEXTUAIS ==================
# (Brasil, MATOPIBA, Cidades)


def load_brasil_geojson():
    """
    Carrega GeoJSON dos estados brasileiros para visualização contextual.

    Returns:
        dl.GeoJSON: Camada com fronteiras dos estados do Brasil
    """
    import json
    import os

    # Caminho para o arquivo GeoJSON (resolve caminho absoluto)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    geojson_path = os.path.abspath(
        os.path.join(
            base_dir, "..", "..", "data", "geojson", "BR_UF_2024.geojson"
        )
    )

    try:
        with open(geojson_path, "r", encoding="utf-8") as f:
            brasil_data = json.load(f)

        # Estilo para fronteiras dos estados
        geojson_layer = dl.GeoJSON(
            id="brasil-layer",
            data=brasil_data,
            options={
                "style": {
                    "color": "#3388ff",  # Azul para fronteiras
                    "weight": 2,
                    "opacity": 0.6,
                    "fillOpacity": 0.1,
                }
            },
            hoverStyle={"weight": 3, "fillOpacity": 0.2},
        )

        logger.info("✅ GeoJSON do Brasil carregado: 27 estados")
        return geojson_layer

    except Exception as e:
        logger.error(f"❌ Erro ao carregar GeoJSON do Brasil: {e}")
        return None


def load_matopiba_geojson():
    """
    Carrega GeoJSON do perímetro MATOPIBA (região de estudo de caso).

    Returns:
        dl.GeoJSON: Camada com perímetro da região MATOPIBA
    """
    import json
    import os

    # Caminho para o arquivo GeoJSON (resolve caminho absoluto)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    geojson_path = os.path.abspath(
        os.path.join(
            base_dir,
            "..",
            "..",
            "data",
            "geojson",
            "Matopiba_Perimetro.geojson",
        )
    )

    try:
        with open(geojson_path, "r", encoding="utf-8") as f:
            matopiba_data = json.load(f)

        # Estilo destacado para MATOPIBA (região de estudo)
        geojson_layer = dl.GeoJSON(
            id="matopiba-layer",
            data=matopiba_data,
            options={
                "style": {
                    "color": "#ff7800",  # Laranja para destaque
                    "weight": 3,
                    "opacity": 0.8,
                    "fillColor": "#ffaa00",
                    "fillOpacity": 0.15,
                }
            },
            hoverStyle={"weight": 4, "fillOpacity": 0.3},
        )

        logger.info("✅ GeoJSON do MATOPIBA carregado")
        return geojson_layer

    except Exception as e:
        logger.error(f"❌ Erro ao carregar GeoJSON do MATOPIBA: {e}")
        return None


def load_matopiba_cities_markers():
    """
    Carrega 337 cidades do MATOPIBA do CSV e cria FeatureGroup.

    Returns:
        dl.FeatureGroup: Grupo de CircleMarkers com as cidades (sem clustering)
    """
    import os
    import pandas as pd

    # Caminho para o arquivo CSV (resolve caminho absoluto)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.abspath(
        os.path.join(
            base_dir, "..", "..", "data", "csv", "CITIES_MATOPIBA_337.csv"
        )
    )

    try:
        df = pd.read_csv(csv_path)

        # Criar markers para cada cidade
        markers = []
        for _, row in df.iterrows():
            city_name = row.get("CITY", "Cidade")
            state = row.get("UF", "")
            lat = row.get("LATITUDE")
            lon = row.get("LONGITUDE")
            height = row.get("HEIGHT", 0)

            if pd.notna(lat) and pd.notna(lon):
                # Usar CircleMarker para melhor performance com 337 pontos
                marker = dl.CircleMarker(
                    center=[lat, lon],
                    radius=4,  # Raio pequeno
                    color="#ff6600",
                    fillColor="#ff9933",
                    fillOpacity=0.6,
                    children=[
                        dl.Tooltip(f"{city_name}/{state}"),
                        dl.Popup(
                            html.Div(
                                [
                                    html.H6(
                                        f"📍 {city_name}/{state}",
                                        className="mb-2",
                                    ),
                                    html.P(
                                        [
                                            html.B("Coordenadas: "),
                                            f"{lat:.4f}°, {lon:.4f}°",
                                        ],
                                        className="mb-1 small",
                                    ),
                                    html.P(
                                        [
                                            html.B("Altitude: "),
                                            (
                                                f"{height:.1f}m"
                                                if pd.notna(height)
                                                else "N/D"
                                            ),
                                        ],
                                        className="mb-0 small",
                                    ),
                                ],
                                style={"minWidth": "200px"},
                            )
                        ),
                    ],
                )
                markers.append(marker)

        # ✅ Usar FeatureGroup para agrupar markers (sem clustering)
        feature_group = dl.FeatureGroup(
            id="matopiba-cities-group", children=markers
        )

        logger.info(
            f"✅ {len(markers)} cidades MATOPIBA carregadas (CircleMarkers)"
        )
        return feature_group

    except Exception as e:
        logger.error(f"❌ Erro ao carregar cidades MATOPIBA: {e}")
        return None


def load_piracicaba_marker():
    """
    Carrega marker especial de Piracicaba/SP (local de desenvolvimento).

    Returns:
        dl.Marker: Marker customizado com info da ESALQ/USP
    """
    import os
    import pandas as pd

    # Caminho para o arquivo CSV (resolve caminho absoluto)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.abspath(
        os.path.join(
            base_dir, "..", "..", "data", "csv", "CITY_PIRACICABA_SP.csv"
        )
    )

    try:
        df = pd.read_csv(csv_path)
        row = df.iloc[0]  # Primeira (e única) linha

        lat = row.get("LATITUDE")
        lon = row.get("LONGITUDE")
        height = row.get("HEIGHT", 0)

        # Marker customizado com ícone destacado
        marker = dl.Marker(
            position=[lat, lon],
            children=[
                dl.Tooltip("🎓 Piracicaba/SP - ESALQ/USP"),
                dl.Popup(
                    html.Div(
                        [
                            html.H5(
                                "🎓 Piracicaba/SP",
                                className="mb-2",
                                style={"color": "#2c5282"},
                            ),
                            html.P(
                                html.B("Local de Desenvolvimento:"),
                                className="mb-1",
                            ),
                            html.P(
                                "ESALQ/USP - Escola Superior de Agricultura "
                                '"Luiz de Queiroz"',
                                className="mb-2 small",
                            ),
                            html.Hr(className="my-2"),
                            html.P(
                                [
                                    html.B("Coordenadas: "),
                                    f"{lat:.6f}°, {lon:.6f}°",
                                ],
                                className="mb-1 small",
                            ),
                            html.P(
                                [html.B("Altitude: "), f"{height:.2f}m"],
                                className="mb-0 small",
                            ),
                        ],
                        style={
                            "minWidth": "280px",
                            "padding": "10px",
                        },
                    )
                ),
            ],
            icon={
                "iconUrl": (
                    "https://raw.githubusercontent.com/pointhi/"
                    "leaflet-color-markers/master/img/"
                    "marker-icon-2x-blue.png"
                ),
                "iconSize": [25, 41],
                "iconAnchor": [12, 41],
                "popupAnchor": [1, -34],
            },
        )

        logger.info("✅ Marker de Piracicaba/SP carregado")
        return marker

    except Exception as e:
        logger.error(f"❌ Erro ao carregar marker de Piracicaba: {e}")
        return None


# ========== NOVAS FUNÇÕES COM FEATUREGROUP WRAPPER ==========


def create_brasil_layer():
    """Cria camada do Brasil com FeatureGroup wrapper"""
    import json
    import os

    base_dir = os.path.dirname(os.path.abspath(__file__))
    geojson_path = os.path.abspath(
        os.path.join(
            base_dir, "..", "..", "data", "geojson", "BR_UF_2024.geojson"
        )
    )

    try:
        if not os.path.exists(geojson_path):
            print(f"❌ Arquivo não encontrado: {geojson_path}")
            return None

        with open(geojson_path, "r", encoding="utf-8") as f:
            brasil_data = json.load(f)

        # ✅ SOLUÇÃO: GeoJSON dentro de FeatureGroup
        geojson = dl.GeoJSON(
            data=brasil_data,
            options={
                "style": {
                    "color": "#3388ff",
                    "weight": 2,
                    "opacity": 0.6,
                    "fillOpacity": 0.1,
                }
            },
            hoverStyle={"weight": 3, "fillOpacity": 0.2},
            id="brasil-geojson-layer",  # ID para controle via callback
        )

        # ✅ WRAPPER: FeatureGroup torna a layer "controlável"
        feature_group = dl.FeatureGroup(
            children=[geojson], id="brasil-feature-group"
        )

        print("✅ Camada Brasil criada com FeatureGroup wrapper")
        logger.info("✅ Camada Brasil criada com FeatureGroup wrapper")
        return feature_group

    except Exception as e:
        print(f"❌ Erro ao carregar Brasil: {e}")
        logger.error(f"❌ Erro ao carregar Brasil: {e}")
        return None


def create_matopiba_layer():
    """Cria camada MATOPIBA com FeatureGroup wrapper"""
    import json
    import os

    base_dir = os.path.dirname(os.path.abspath(__file__))
    geojson_path = os.path.abspath(
        os.path.join(
            base_dir,
            "..",
            "..",
            "data",
            "geojson",
            "Matopiba_Perimetro.geojson",
        )
    )

    try:
        if not os.path.exists(geojson_path):
            print(f"❌ Arquivo não encontrado: {geojson_path}")
            return None

        with open(geojson_path, "r", encoding="utf-8") as f:
            matopiba_data = json.load(f)

        geojson = dl.GeoJSON(
            data=matopiba_data,
            options={
                "style": {
                    "color": "#ff7800",
                    "weight": 3,
                    "opacity": 0.8,
                    "fillColor": "#ffaa00",
                    "fillOpacity": 0.15,
                }
            },
            hoverStyle={"weight": 4, "fillOpacity": 0.3},
            id="matopiba-geojson-layer",
        )

        feature_group = dl.FeatureGroup(
            children=[geojson], id="matopiba-feature-group"
        )

        print("✅ Camada MATOPIBA criada com FeatureGroup wrapper")
        logger.info("✅ Camada MATOPIBA criada com FeatureGroup wrapper")
        return feature_group

    except Exception as e:
        print(f"❌ Erro ao carregar MATOPIBA: {e}")
        logger.error(f"❌ Erro ao carregar MATOPIBA: {e}")
        return None


def create_cities_layer():
    """Cria camada de cidades MATOPIBA com FeatureGroup"""
    import os
    import pandas as pd
    from dash import html

    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.abspath(
        os.path.join(
            base_dir, "..", "..", "data", "csv", "CITIES_MATOPIBA_337.csv"
        )
    )

    try:
        if not os.path.exists(csv_path):
            print(f"❌ Arquivo não encontrado: {csv_path}")
            return None

        df = pd.read_csv(csv_path)
        markers = []

        for _, row in df.iterrows():
            city_name = row.get("CITY", "Cidade")
            state = row.get("UF", "")
            lat = row.get("LATITUDE")
            lon = row.get("LONGITUDE")
            height = row.get("HEIGHT", 0)

            if pd.notna(lat) and pd.notna(lon):
                marker = dl.CircleMarker(
                    center=[lat, lon],
                    radius=4,
                    color="#ff6600",
                    fillColor="#ff9933",
                    fillOpacity=0.6,
                    children=[
                        dl.Tooltip(f"{city_name}/{state}"),
                        dl.Popup(
                            html.Div(
                                [
                                    html.H6(
                                        f"📍 {city_name}/{state}",
                                        className="mb-2",
                                    ),
                                    html.P(
                                        [
                                            html.B("Coordenadas: "),
                                            f"{lat:.4f}°, {lon:.4f}°",
                                        ],
                                        className="mb-1 small",
                                    ),
                                    html.P(
                                        [
                                            html.B("Altitude: "),
                                            (
                                                f"{height:.1f}m"
                                                if pd.notna(height)
                                                else "N/D"
                                            ),
                                        ],
                                        className="mb-0 small",
                                    ),
                                ],
                                style={"minWidth": "200px"},
                            )
                        ),
                    ],
                )
                markers.append(marker)

        # ✅ Cities já é FeatureGroup, não precisa de wrapper extra
        feature_group = dl.FeatureGroup(
            children=markers, id="cities-feature-group"
        )

        print(f"✅ {len(markers)} cidades MATOPIBA carregadas")
        logger.info(f"✅ {len(markers)} cidades MATOPIBA carregadas")
        return feature_group

    except Exception as e:
        print(f"❌ Erro ao carregar cidades: {e}")
        logger.error(f"❌ Erro ao carregar cidades: {e}")
        return None


def create_piracicaba_layer():
    """Cria marcador de Piracicaba com FeatureGroup wrapper"""
    import os
    import pandas as pd
    from dash import html

    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.abspath(
        os.path.join(
            base_dir, "..", "..", "data", "csv", "CITY_PIRACICABA_SP.csv"
        )
    )

    try:
        if not os.path.exists(csv_path):
            print(f"❌ Arquivo não encontrado: {csv_path}")
            return None

        df = pd.read_csv(csv_path)
        row = df.iloc[0]

        lat = row.get("LATITUDE")
        lon = row.get("LONGITUDE")
        height = row.get("HEIGHT", 0)

        marker = dl.Marker(
            position=[lat, lon],
            children=[
                dl.Tooltip("🎓 Piracicaba/SP - ESALQ/USP"),
                dl.Popup(
                    html.Div(
                        [
                            html.H5(
                                "🎓 Piracicaba/SP",
                                className="mb-2",
                                style={"color": "#2c5282"},
                            ),
                            html.P(
                                html.B("Local de Desenvolvimento:"),
                                className="mb-1",
                            ),
                            html.P(
                                "ESALQ/USP - Escola Superior de Agricultura "
                                "'Luiz de Queiroz'",
                                className="mb-2 small",
                            ),
                            html.Hr(className="my-2"),
                            html.P(
                                [
                                    html.B("Coordenadas: "),
                                    f"{lat:.6f}°, {lon:.6f}°",
                                ],
                                className="mb-1 small",
                            ),
                            html.P(
                                [html.B("Altitude: "), f"{height:.2f}m"],
                                className="mb-0 small",
                            ),
                        ],
                        style={"minWidth": "280px", "padding": "10px"},
                    )
                ),
            ],
            icon={
                "iconUrl": (
                    "https://raw.githubusercontent.com/pointhi/"
                    "leaflet-color-markers/master/img/"
                    "marker-icon-2x-blue.png"
                ),
                "iconSize": [25, 41],
                "iconAnchor": [12, 41],
                "popupAnchor": [1, -34],
            },
        )

        # ✅ Piracicaba dentro de FeatureGroup
        feature_group = dl.FeatureGroup(
            children=[marker], id="piracicaba-feature-group"
        )

        print("✅ Marker Piracicaba criado com FeatureGroup wrapper")
        logger.info("✅ Marker Piracicaba criado com FeatureGroup wrapper")
        return feature_group

    except Exception as e:
        print(f"❌ Erro ao carregar Piracicaba: {e}")
        logger.error(f"❌ Erro ao carregar Piracicaba: {e}")
        return None


def create_custom_layer_control():
    """
    Cria controle de camadas collapsible (expansível/recolhível)
    com botão toggle flutuante para substituir o LayersControl bugado.
    """
    from dash import html, dcc

    control_html = html.Div(
        [
            # ✅ Botão Toggle Flutuante (sempre visível)
            html.Button(
                "🗺️",
                id="layer-control-toggle",
                n_clicks=0,
                title="Alternar Camadas",
                style={
                    "position": "absolute",
                    "top": "10px",
                    "right": "10px",
                    "zIndex": 1001,
                    "background": "white",
                    "border": "2px solid #3388ff",
                    "borderRadius": "50%",
                    "width": "40px",
                    "height": "40px",
                    "fontSize": "20px",
                    "cursor": "pointer",
                    "boxShadow": "0 2px 8px rgba(0,0,0,0.3)",
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "center",
                    "transition": "all 0.3s",
                },
            ),
            # ✅ Painel Collapsible (inicialmente oculto)
            html.Div(
                [
                    # Header com título + botão fechar
                    html.Div(
                        [
                            html.H6(
                                "🗺️ Camadas do Mapa",
                                style={
                                    "margin": "0",
                                    "fontWeight": "bold",
                                    "color": "#2c3e50",
                                    "flex": "1",
                                },
                            ),
                            html.Button(
                                "✕",
                                id="layer-control-close",
                                n_clicks=0,
                                title="Fechar",
                                style={
                                    "background": "transparent",
                                    "border": "none",
                                    "fontSize": "20px",
                                    "cursor": "pointer",
                                    "color": "#7f8c8d",
                                    "padding": "0",
                                    "width": "24px",
                                    "height": "24px",
                                    "lineHeight": "20px",
                                },
                            ),
                        ],
                        style={
                            "display": "flex",
                            "alignItems": "center",
                            "marginBottom": "15px",
                            "paddingBottom": "10px",
                            "borderBottom": "1px solid #ecf0f1",
                        },
                    ),
                    # Checkboxes das camadas
                    html.Div(
                        [
                            # Checkbox Brasil
                            html.Label(
                                [
                                    dcc.Checklist(
                                        options=[
                                            {
                                                "label": " 🇧🇷 Brasil - Estados",
                                                "value": "brasil",
                                            }
                                        ],
                                        value=[],
                                        id="layer-brasil-toggle",
                                        style={
                                            "display": "inline-block",
                                            "marginRight": "5px",
                                        },
                                    ),
                                ],
                                style={
                                    "display": "block",
                                    "marginBottom": "10px",
                                    "cursor": "pointer",
                                    "padding": "5px",
                                    "borderRadius": "4px",
                                    "transition": "background 0.2s",
                                },
                                className="layer-checkbox-label",
                            ),
                            # Checkbox MATOPIBA
                            html.Label(
                                [
                                    dcc.Checklist(
                                        options=[
                                            {
                                                "label": " 🌾 MATOPIBA - Região",
                                                "value": "matopiba",
                                            }
                                        ],
                                        value=[],
                                        id="layer-matopiba-toggle",
                                        style={
                                            "display": "inline-block",
                                            "marginRight": "5px",
                                        },
                                    ),
                                ],
                                style={
                                    "display": "block",
                                    "marginBottom": "10px",
                                    "cursor": "pointer",
                                    "padding": "5px",
                                    "borderRadius": "4px",
                                    "transition": "background 0.2s",
                                },
                                className="layer-checkbox-label",
                            ),
                            # Checkbox Cidades
                            html.Label(
                                [
                                    dcc.Checklist(
                                        options=[
                                            {
                                                "label": " 🏘️ 337 Cidades",
                                                "value": "cities",
                                            }
                                        ],
                                        value=[],
                                        id="layer-cities-toggle",
                                        style={
                                            "display": "inline-block",
                                            "marginRight": "5px",
                                        },
                                    ),
                                ],
                                style={
                                    "display": "block",
                                    "marginBottom": "10px",
                                    "cursor": "pointer",
                                    "padding": "5px",
                                    "borderRadius": "4px",
                                    "transition": "background 0.2s",
                                },
                                className="layer-checkbox-label",
                            ),
                            # Checkbox Piracicaba
                            html.Label(
                                [
                                    dcc.Checklist(
                                        options=[
                                            {
                                                "label": " 🎓 Piracicaba/SP",
                                                "value": "piracicaba",
                                            }
                                        ],
                                        value=["piracicaba"],  # Inicia marcado
                                        id="layer-piracicaba-toggle",
                                        style={
                                            "display": "inline-block",
                                            "marginRight": "5px",
                                        },
                                    ),
                                ],
                                style={
                                    "display": "block",
                                    "marginBottom": "5px",
                                    "cursor": "pointer",
                                    "padding": "5px",
                                    "borderRadius": "4px",
                                    "transition": "background 0.2s",
                                },
                                className="layer-checkbox-label",
                            ),
                        ]
                    ),
                ],
                id="layer-control-panel",
                style={
                    "position": "absolute",
                    "top": "10px",
                    "right": "60px",  # Espaço para o botão toggle
                    "zIndex": 1000,
                    "background": "white",
                    "padding": "15px",
                    "borderRadius": "8px",
                    "boxShadow": "0 4px 12px rgba(0,0,0,0.2)",
                    "minWidth": "250px",
                    "maxHeight": "350px",
                    "overflowY": "auto",
                    "border": "2px solid #3388ff",
                    "display": "none",  # Inicia oculto
                    "animation": "fadeIn 0.3s",
                },
            ),
        ],
        id="custom-layer-control-container",
        style={
            "position": "absolute",
            "top": "0",
            "right": "0",
            "zIndex": 1002,
        },
    )

    return control_html


logger.info("✅ Componente Dash Leaflet carregado com sucesso")
