"""
Callbacks para a página inicial - busca dados da API
"""

import json
import logging
import sys

import dash_bootstrap_components as dbc
import requests
from dash import ALL, Input, Output, State, html
from geopy.geocoders import Nominatim

from ..components.world_map_leaflet import (
    create_map_marker,
)

logger = logging.getLogger(__name__)

# Inicializar geocoder para reverse geocoding
geolocator = Nominatim(user_agent="evaonline_v1.0")


def register_home_callbacks(app):
    """Registra callbacks da página inicial."""

    @app.callback(
        Output("api-status-display", "children"),
        Input("interval-component", "n_intervals"),
    )
    def update_api_status(n_intervals):
        """Atualiza o status da API."""
        try:
            # Fazer chamada para a API de health
            response = requests.get(
                "http://localhost:8000/api/v1/health", timeout=5
            )
            data = response.json()

            # Criar cards com informações
            cards = [
                dbc.Card(
                    [
                        dbc.CardBody(
                            [
                                html.H5(
                                    "Status da API", className="card-title"
                                ),
                                html.P(
                                    f"Serviço: {data.get('service', 'N/A')}",
                                    className="card-text",
                                ),
                                html.P(
                                    f"Versão: {data.get('version', 'N/A')}",
                                    className="card-text",
                                ),
                                html.P(
                                    f"Status: {data.get('status', 'N/A')}",
                                    className="card-text",
                                ),
                                dbc.Badge(
                                    (
                                        "Online"
                                        if data.get("status") == "ok"
                                        else "Offline"
                                    ),
                                    color=(
                                        "success"
                                        if data.get("status") == "ok"
                                        else "danger"
                                    ),
                                    className="mt-2",
                                ),
                            ]
                        )
                    ],
                    className="mb-3",
                )
            ]

            return cards

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao conectar com API: {e}")
            return dbc.Alert(
                f"Erro ao conectar com a API: {str(e)}",
                color="danger",
                className="mt-3",
            )
        except Exception as e:
            logger.error(f"Erro inesperado: {e}")
            return dbc.Alert(
                f"Erro inesperado: {str(e)}", color="warning", className="mt-3"
            )

    @app.callback(
        Output("services-status-display", "children"),
        Input("interval-component", "n_intervals"),
    )
    def update_services_status(n_intervals):
        """Atualiza o status dos serviços."""
        try:
            # Fazer chamada para a API de status dos serviços
            response = requests.get(
                "http://localhost:8000/api/v1/api/internal/" "services/status",
                timeout=10,
            )
            data = response.json()

            # Criar cards para cada serviço
            service_cards = []

            for service_id, service_info in data.get("services", {}).items():
                status_color = (
                    "success"
                    if service_info.get("status") == "healthy"
                    else "danger"
                )

                card = dbc.Card(
                    [
                        dbc.CardBody(
                            [
                                html.H6(
                                    service_info.get("name", service_id),
                                    className="card-title",
                                ),
                                html.P(
                                    f"Status: {service_info.get('status', 'unknown')}",
                                    className="card-text",
                                ),
                                dbc.Badge(
                                    (
                                        "Disponível"
                                        if service_info.get("available", False)
                                        else "Indisponível"
                                    ),
                                    color=status_color,
                                    className="mt-2",
                                ),
                            ]
                        )
                    ],
                    className="mb-2",
                )
                service_cards.append(card)

            # Card de resumo
            summary_card = dbc.Card(
                [
                    dbc.CardBody(
                        [
                            html.H5(
                                "Resumo dos Serviços", className="card-title"
                            ),
                            html.P(
                                f"Total: {data.get('total_services', 0)}",
                                className="card-text",
                            ),
                            html.P(
                                f"Saúde: {data.get('healthy_count', 0)}",
                                className="card-text",
                            ),
                        ]
                    )
                ],
                className="mb-3",
            )

            return [summary_card] + service_cards

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao conectar com API de serviços: {e}")
            return dbc.Alert(
                f"Erro ao conectar com a API de serviços: {str(e)}",
                color="danger",
                className="mt-3",
            )
        except Exception as e:
            logger.error(f"Erro inesperado nos serviços: {e}")
            return dbc.Alert(
                f"Erro inesperado: {str(e)}", color="warning", className="mt-3"
            )

    @app.callback(
        [
            Output("map-click-data", "data"),
            Output("selected-location-data", "data"),
            Output("marker-layer", "children"),
            Output("selected-coords-display", "children"),
            Output("add-favorite-btn", "disabled"),
            Output("calculate-eto-btn", "disabled"),
        ],
        Input("world-map", "clickData"),
        prevent_initial_call=True,
    )
    def handle_map_click(click_data):
        """
        Captura clique no mapa e habilita botões.

        Args:
            click_data: Dict com 'latlng' = {"lat": float, "lng": float}

        Returns:
            tuple: (click_data, location_data, marker, coords_display, btn_disabled, btn_disabled)
        """
        # ✅ DEBUG: Log sempre, mesmo se None
        logger.info(
            f"🔍 Callback disparado! click_data cru: "
            f"{json.dumps(click_data, indent=2) if click_data else 'None/Empty'}"
        )

        if not click_data or click_data == {}:
            logger.warning(
                "⚠️ click_data vazio ou None - clique não detectado?"
            )
            # Retorna estado inicial com mensagem de debug
            debug_msg = html.Div(
                "👆 Clique no mapa para ativar (ver logs para debug)",
                className="alert alert-info small",
            )
            return None, None, [], debug_msg, True, True

        try:
            # ✅ CHECK: Verifica estrutura exata do click_data
            if "latlng" not in click_data:
                logger.error(
                    f"❌ 'latlng' ausente em click_data: {click_data}"
                )
                error_msg = html.Div(
                    f"Erro: Estrutura inválida - {click_data}",
                    className="alert alert-danger small",
                )
                return None, None, [], error_msg, True, True

            latlng = click_data["latlng"]
            if (
                not isinstance(latlng, dict)
                or "lat" not in latlng
                or "lng" not in latlng
            ):
                logger.error(f"❌ latlng malformado: {latlng}")
                error_msg = html.Div(
                    f"Erro: latlng inválido - {latlng}",
                    className="alert alert-danger small",
                )
                return None, None, [], error_msg, True, True

            lat = latlng["lat"]
            lon = latlng["lng"]  # ✅ Corrigido: "lng" (Leaflet usa isso)

            logger.info(f"✅ Clique processado: LAT={lat:.6f}, LON={lon:.6f}")

            # Criar dados da localização (SIMPLIFICADO - só coordenadas)
            location_data = {
                "lat": lat,
                "lon": lon,
            }

            # Criar marcador no mapa
            marker = create_map_marker(
                lat, lon, label=f"Lat: {lat:.4f}°, Lon: {lon:.4f}°"
            )

            # Criar display de coordenadas compacto
            coords_display = html.Div(
                [
                    html.Strong("📍 Selecionado:"),
                    html.Br(),
                    html.Small(f"Lat: {lat:.6f}°"),
                    html.Br(),
                    html.Small(f"Lon: {lon:.6f}°"),
                ],
                className="text-center p-2 bg-light rounded",
            )

            return (
                {"lat": lat, "lon": lon},
                location_data,
                [marker],
                coords_display,
                False,  # Habilita botão Adicionar
                False,  # Habilita botão Calcular
            )

        except KeyError as e:
            logger.error(f"❌ KeyError em latlng: {e} - Dados: {click_data}")
            error_msg = html.Div(
                f"KeyError: {e}", className="alert alert-danger small"
            )
            return None, None, [], error_msg, True, True
        except Exception as e:
            logger.error(f"❌ Erro geral: {e} - Stack: {sys.exc_info()}")
            error_msg = html.Div(
                f"Erro inesperado: {str(e)}",
                className="alert alert-danger small",
            )
            return None, None, [], error_msg, True, True

    # ==========================================================================
    # CALLBACKS DE FAVORITOS
    # ==========================================================================

    @app.callback(
        [
            Output("favorites-store", "data"),
            Output("toast-container", "children"),
        ],
        Input("add-favorite-btn", "n_clicks"),
        [
            State("selected-location-data", "data"),
            State("favorites-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def add_to_favorites(n_clicks, location_data, current_favorites):
        """Adiciona localização atual aos favoritos."""
        if not location_data or not n_clicks:
            return current_favorites, None

        # Inicializar lista se None
        if current_favorites is None:
            current_favorites = []

        # Verificar limite máximo (5 favoritos)
        if len(current_favorites) >= 5:
            logger.warning("⚠️ Limite de 5 favoritos atingido")
            toast = dbc.Toast(
                [
                    html.P(
                        "Você já atingiu o limite máximo de 5 favoritos.",
                        className="mb-0",
                    ),
                    html.P(
                        "Remova um favorito para adicionar outro.",
                        className="mb-0 small text-muted",
                    ),
                ],
                header="⚠️ Limite Atingido",
                icon="warning",
                is_open=True,
                dismissable=True,
                duration=4000,
                style={"maxWidth": "350px"},
            )
            return current_favorites, toast

        lat = location_data.get("lat")
        lon = location_data.get("lon")

        # Verificar se já existe (evitar duplicatas)
        for fav in current_favorites:
            if (
                abs(fav["lat"] - lat) < 0.0001
                and abs(fav["lon"] - lon) < 0.0001
            ):
                logger.info(f"📍 Localização já existe: {lat}, {lon}")
                toast = dbc.Toast(
                    "Esta localização já está na lista de favoritos.",
                    header="ℹ️ Favorito Existente",
                    icon="info",
                    is_open=True,
                    dismissable=True,
                    duration=3000,
                    style={"maxWidth": "350px"},
                )
                return current_favorites, toast

        # Adicionar novo favorito
        new_favorite = {
            "lat": lat,
            "lon": lon,
            "label": f"Lat: {lat:.4f}°, Lon: {lon:.4f}°",
        }
        current_favorites.append(new_favorite)

        logger.info(f"⭐ Favorito adicionado: {new_favorite['label']}")

        favorites_count = len(current_favorites)

        # Toast de sucesso
        toast = dbc.Toast(
            [
                html.P(
                    f"Localização adicionada aos favoritos!",
                    className="mb-0",
                ),
                html.Small(
                    f"Total: {favorites_count}/5",
                    className="text-muted",
                ),
            ],
            header="✅ Favorito Adicionado",
            icon="success",
            is_open=True,
            dismissable=True,
            duration=3000,
            style={"maxWidth": "350px"},
        )

        return current_favorites, toast

    @app.callback(
        [
            Output("favorites-list-container", "children"),
            Output("empty-favorites-alert", "style"),
            Output("clear-favorites-button", "disabled"),
            Output("favorites-count-badge", "children"),
        ],
        Input("favorites-store", "data"),
    )
    def update_favorites_display(favorites):
        """Atualiza a exibição da lista de favoritos."""
        # Calcular contador atualizado
        favorites_count = len(favorites) if favorites else 0
        badge_text = f"{favorites_count}/5"

        if not favorites or favorites_count == 0:
            return [], {"display": "block"}, True, badge_text

        # Criar lista de favoritos
        favorites_items = []
        for idx, fav in enumerate(favorites):
            item = dbc.ListGroupItem(
                [
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.I(
                                        className=(
                                            "bi bi-geo-alt-fill "
                                            "me-2 text-primary"
                                        )
                                    ),
                                    html.Strong(fav["label"]),
                                ],
                                className="flex-grow-1",
                            ),
                            html.Div(
                                [
                                    dbc.Button(
                                        [html.I(className="bi bi-calculator")],
                                        id={
                                            "type": "calc-eto-favorite",
                                            "index": idx,
                                        },
                                        color="success",
                                        size="sm",
                                        className="me-2",
                                        title="Calcular ETo",
                                    ),
                                    dbc.Button(
                                        [html.I(className="bi bi-trash")],
                                        id={
                                            "type": "remove-favorite",
                                            "index": idx,
                                        },
                                        color="danger",
                                        size="sm",
                                        title="Remover",
                                    ),
                                ],
                                className="d-flex gap-1",
                            ),
                        ],
                        className=(
                            "d-flex align-items-center "
                            "justify-content-between"
                        ),
                    )
                ],
                className="mb-2",
            )
            favorites_items.append(item)

        return (
            dbc.ListGroup(favorites_items, flush=True),
            {"display": "none"},
            False,
            badge_text,
        )

    @app.callback(
        Output("favorites-store", "data", allow_duplicate=True),
        Input("clear-favorites-button", "n_clicks"),
        prevent_initial_call=True,
    )
    def clear_all_favorites(n_clicks):
        """Limpa todos os favoritos."""
        if n_clicks:
            logger.info("🗑️ Todos os favoritos foram removidos")
            return []
        return []

    @app.callback(
        Output("favorites-store", "data", allow_duplicate=True),
        Input({"type": "remove-favorite", "index": ALL}, "n_clicks"),
        State("favorites-store", "data"),
        prevent_initial_call=True,
    )
    def remove_favorite(n_clicks_list, current_favorites):
        """Remove um favorito específico da lista."""
        if not current_favorites or not any(n_clicks_list):
            return current_favorites

        # Encontrar qual botão foi clicado
        for idx, n_clicks in enumerate(n_clicks_list):
            if n_clicks:
                removed = current_favorites.pop(idx)
                logger.info(f"🗑️ Favorito removido: {removed['label']}")
                break

        return current_favorites


def get_location_info(lat, lon):
    """
    Obtém informações geográficas usando reverse geocoding.

    Args:
        lat (float): Latitude
        lon (float): Longitude

    Returns:
        dict: Informações da localização
    """
    try:
        location = geolocator.reverse(
            f"{lat}, {lon}", language="pt", timeout=10
        )

        if not location:
            return {
                "city": "Local desconhecido",
                "country": "",
                "state": "",
                "timezone": "UTC",
                "display_name": f"Lat: {lat:.4f}, Lon: {lon:.4f}",
            }

        address = location.raw.get("address", {})

        # Extrair cidade (vários campos possíveis)
        city = (
            address.get("city")
            or address.get("town")
            or address.get("village")
            or address.get("municipality")
            or "Local desconhecido"
        )

        # Extrair país e estado
        country = address.get("country", "")
        state = address.get("state", "")

        return {
            "city": city,
            "country": country,
            "state": state,
            "timezone": "UTC",  # TODO: Calcular timezone real
            "display_name": location.address,
        }

    except Exception as e:
        logger.error(f"Erro no reverse geocoding: {e}")
        return {
            "city": "Erro ao obter localização",
            "country": "",
            "state": "",
            "timezone": "UTC",
            "display_name": f"Lat: {lat:.4f}, Lon: {lon:.4f}",
        }


def create_selection_info_card(location_data):
    """
    Cria card com coordenadas e botões de ação.

    Args:
        location_data (dict): Dados com 'lat' e 'lon'

    Returns:
        dbc.Card: Card com coordenadas e botões
    """
    lat = location_data.get("lat", 0)
    lon = location_data.get("lon", 0)

    card = dbc.Card(
        [
            dbc.CardBody(
                [
                    html.H5(
                        "📍 Localização Selecionada",
                        className="card-title mb-3",
                    ),
                    html.Div(
                        [
                            html.P(
                                [
                                    html.Strong("Latitude: "),
                                    f"{lat:.6f}°",
                                ],
                                className="mb-2",
                            ),
                            html.P(
                                [
                                    html.Strong("Longitude: "),
                                    f"{lon:.6f}°",
                                ],
                                className="mb-3",
                            ),
                        ]
                    ),
                    # Botões de ação
                    html.Div(
                        [
                            dbc.Button(
                                [
                                    html.I(className="bi bi-star me-2"),
                                    "Adicionar Favorito",
                                ],
                                id="add-favorite-btn",
                                color="warning",
                                className="me-2",
                                size="sm",
                            ),
                            dbc.Button(
                                [
                                    html.I(className="bi bi-calculator me-2"),
                                    "Calcular ETo",
                                ],
                                id="calculate-eto-btn",
                                color="success",
                                size="sm",
                                # Redireciona para página ETo
                                href="/eto-calculator",
                            ),
                        ],
                        className="d-flex gap-2",
                    ),
                ]
            )
        ],
        className="mt-3",
        color="light",
    )

    return card


# ========== CALLBACKS PARA CONTROLE CUSTOMIZADO DE CAMADAS ==========


def register_layer_control_callbacks(app):
    """Registra callbacks para o controle customizado de camadas."""
    from dash import callback_context

    # ✅ Importar apenas as funções que retornam listas de markers/componentes
    from ..components.world_map_leaflet import (
        load_brasil_geojson,
        load_matopiba_geojson,
        load_matopiba_cities_markers,
        load_piracicaba_marker,
    )

    # ✅ NOVO: Callback para controlar painel collapsible
    @app.callback(
        Output("layer-control-panel", "style"),
        [
            Input("layer-control-toggle", "n_clicks"),
            Input("layer-control-close", "n_clicks"),
        ],
        [State("layer-control-panel", "style")],
        prevent_initial_call=True,
    )
    def toggle_layer_panel(toggle_clicks, close_clicks, current_style):
        """Controla a visibilidade do painel de camadas collapsible."""
        ctx = callback_context

        if not ctx.triggered:
            return current_style

        # Identifica qual botão foi clicado
        trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]

        if trigger_id == "layer-control-toggle":
            # Botão Toggle - alterna entre mostrar/ocultar
            if current_style.get("display") == "none":
                logger.info("🗺️ Painel de camadas ABERTO")
                return {**current_style, "display": "block"}
            else:
                logger.info("🗺️ Painel de camadas FECHADO (toggle)")
                return {**current_style, "display": "none"}

        elif trigger_id == "layer-control-close":
            # Botão Fechar - sempre oculta
            logger.info("🗺️ Painel de camadas FECHADO (close)")
            return {**current_style, "display": "none"}

        return current_style

    @app.callback(
        Output("brasil-feature-group", "children"),
        Input("layer-brasil-toggle", "value"),
        prevent_initial_call=False,
    )
    def toggle_brasil_layer(selected):
        """Controla visibilidade da camada Brasil."""
        if selected and "brasil" in selected:
            brasil_geojson = load_brasil_geojson()
            # ✅ Retornar em lista para o FeatureGroup aceitar
            if brasil_geojson is not None:
                print(f"✅ Brasil GeoJSON carregado: {type(brasil_geojson)}")
                return [brasil_geojson]
            else:
                print("❌ Brasil GeoJSON retornou None")
                return []
        return []

    @app.callback(
        Output("matopiba-feature-group", "children"),
        Input("layer-matopiba-toggle", "value"),
        prevent_initial_call=False,
    )
    def toggle_matopiba_layer(selected):
        """Controla visibilidade da camada MATOPIBA."""
        if selected and "matopiba" in selected:
            matopiba_geojson = load_matopiba_geojson()
            # ✅ Retornar em lista para o FeatureGroup aceitar
            if matopiba_geojson is not None:
                print(
                    f"✅ MATOPIBA GeoJSON carregado: {type(matopiba_geojson)}"
                )
                return [matopiba_geojson]
            else:
                print("❌ MATOPIBA GeoJSON retornou None")
                return []
        return []

    @app.callback(
        Output("cities-feature-group", "children"),
        Input("layer-cities-toggle", "value"),
        prevent_initial_call=False,
    )
    def toggle_cities_layer(selected):
        """Controla visibilidade da camada de cidades."""
        if selected and "cities" in selected:
            cities_markers = load_matopiba_cities_markers()
            return cities_markers if cities_markers else []
        return []

    @app.callback(
        Output("piracicaba-feature-group", "children"),
        Input("layer-piracicaba-toggle", "value"),
        prevent_initial_call=False,
    )
    def toggle_piracicaba_layer(selected):
        """Controla visibilidade da camada Piracicaba."""
        if selected and "piracicaba" in selected:
            piracicaba_marker = load_piracicaba_marker()
            return [piracicaba_marker] if piracicaba_marker else []
        return []

    logger.info("✅ Callbacks de controle de camadas registrados")
