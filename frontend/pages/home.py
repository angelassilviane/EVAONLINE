"""
Página inicial do ETO Calculator com mapa mundial interativo.
"""

import logging

import dash_bootstrap_components as dbc
from dash import dcc, html

from ..components.favorites_components import (
    create_clear_favorites_button,
)
from ..components.world_map_leaflet import create_world_map

logger = logging.getLogger(__name__)

# Layout da página inicial
home_layout = html.Div(
    [
        dbc.Container(
            [
                # Row com Mapa e Favoritos lado a lado
                dbc.Row(
                    [
                        # Coluna do Mapa (8 colunas)
                        dbc.Col(
                            [
                                # Accordion com instruções (colapsável)
                                dbc.Accordion(
                                    [
                                        dbc.AccordionItem(
                                            [
                                                dbc.ListGroup(
                                                    [
                                                        dbc.ListGroupItem(
                                                            [
                                                                html.Span(
                                                                    "1.",
                                                                    className="fw-bold me-2",
                                                                ),
                                                                "Clique em qualquer ponto do mapa para "
                                                                "selecionar coordenadas",
                                                            ]
                                                        ),
                                                        dbc.ListGroupItem(
                                                            [
                                                                html.Span(
                                                                    "2.",
                                                                    className="fw-bold me-2",
                                                                ),
                                                                "Use o botão de localização (📍) para "
                                                                "encontrar sua posição atual",
                                                            ]
                                                        ),
                                                        dbc.ListGroupItem(
                                                            [
                                                                html.Span(
                                                                    "3.",
                                                                    className="fw-bold me-2",
                                                                ),
                                                                "Clique em 'Adicionar Favorito' no card abaixo do mapa",
                                                            ]
                                                        ),
                                                        dbc.ListGroupItem(
                                                            [
                                                                html.Span(
                                                                    "4.",
                                                                    className="fw-bold me-2",
                                                                ),
                                                                "Use 'Calcular ETo' para processar os dados",
                                                            ]
                                                        ),
                                                    ],
                                                    flush=True,
                                                )
                                            ],
                                            title="📋 Como usar o mapa (clique para expandir)",
                                        ),
                                    ],
                                    start_collapsed=True,  # Inicia fechado
                                    className="mb-3",
                                ),
                                # Card do Mapa
                                dbc.Card(
                                    [
                                        dbc.CardBody(
                                            [
                                                create_world_map(),
                                                # Exibir coordenadas selecionadas
                                                html.Div(
                                                    id="current-selection-info",
                                                    className="mt-2",
                                                ),
                                            ],
                                            className="p-2",
                                        ),
                                    ],
                                    className="shadow-sm",
                                ),
                            ],
                            md=8,
                            className="mb-4",
                        ),
                        # Coluna dos Favoritos (4 colunas)
                        dbc.Col(
                            [
                                dbc.Card(
                                    [
                                        dbc.CardHeader(
                                            [
                                                html.Div(
                                                    [
                                                        html.H5(
                                                            "⭐ Favoritos",
                                                            className="mb-0 d-inline",
                                                        ),
                                                        dbc.Badge(
                                                            "0/5",
                                                            color="info",
                                                            className="ms-2",
                                                            id="favorites-count-badge",
                                                        ),
                                                    ],
                                                    className="d-flex align-items-center",
                                                )
                                            ]
                                        ),
                                        dbc.CardBody(
                                            [
                                                # Seção fixa: Botões de ação
                                                html.Div(
                                                    [
                                                        html.H6(
                                                            "Ações",
                                                            className="mb-2",
                                                        ),
                                                        dbc.ButtonGroup(
                                                            [
                                                                dbc.Button(
                                                                    [
                                                                        html.I(
                                                                            className="bi bi-star me-2"
                                                                        ),
                                                                        "Adicionar",
                                                                    ],
                                                                    id="add-favorite-btn",
                                                                    color="warning",
                                                                    size="sm",
                                                                    disabled=True,
                                                                    className="w-100 add-favorite-button",
                                                                    title="Clique para salvar a seleção nos Favoritos",
                                                                ),
                                                            ],
                                                            vertical=True,
                                                            className="w-100 mb-2",
                                                        ),
                                                        dbc.ButtonGroup(
                                                            [
                                                                dbc.Button(
                                                                    [
                                                                        html.I(
                                                                            className="bi bi-calculator me-2"
                                                                        ),
                                                                        "Calcular ETo",
                                                                    ],
                                                                    id="calculate-eto-btn",
                                                                    color="success",
                                                                    size="sm",
                                                                    disabled=True,
                                                                    className="w-100",
                                                                    href="/eto-calculator",
                                                                ),
                                                            ],
                                                            vertical=True,
                                                            className="w-100 mb-3",
                                                        ),
                                                        html.Hr(),
                                                        # Coordenadas selecionadas (fixo)
                                                        html.Div(
                                                            id="selected-coords-display",
                                                            className="mb-2 small text-muted",
                                                        ),
                                                    ],
                                                ),
                                                html.Hr(className="my-2"),
                                                # Título da lista (fixo)
                                                html.H6(
                                                    "Lista",
                                                    className="mb-2",
                                                ),
                                                # Container scrollável APENAS para a lista
                                                html.Div(
                                                    [
                                                        html.Div(
                                                            id="favorites-list-container",
                                                            style={
                                                                "minHeight": "100px",
                                                                "maxHeight": "280px",
                                                                "overflowY": "auto",
                                                                "overflowX": "hidden",
                                                            },
                                                        ),
                                                        dbc.Alert(
                                                            [
                                                                "Lista vazia. ",
                                                                "Clique no mapa para selecionar.",
                                                            ],
                                                            color="info",
                                                            id="empty-favorites-alert",
                                                            className="mt-2 mb-0 small",
                                                        ),
                                                    ],
                                                    className="mb-2",
                                                ),
                                                # Botão Limpar (sempre fixo no final)
                                                html.Div(
                                                    [
                                                        html.Hr(
                                                            className="my-2"
                                                        ),
                                                        html.Div(
                                                            create_clear_favorites_button(),
                                                            className="d-flex justify-content-center",
                                                        ),
                                                    ],
                                                ),
                                            ],
                                            style={
                                                "display": "flex",
                                                "flexDirection": "column",
                                                "height": "100%",
                                            },
                                        ),
                                    ],
                                    className="shadow-sm",
                                    style={
                                        "position": "sticky",
                                        "top": "20px",
                                        "height": "680px",  # Mesma altura do card do mapa (600px + padding)
                                        "overflowY": "auto",  # Scroll na sidebar se necessário
                                    },
                                )
                            ],
                            md=4,
                            className="mb-4",
                        ),
                    ]
                ),
                # Stores específicos da home
                dcc.Store(
                    id="favorites-store",
                    storage_type="local",
                    data=[],
                ),
                dcc.Store(id="home-favorites-count", data=0),
                dcc.Store(id="selected-location-data", data=None),
                dcc.Store(id="map-click-data", data=None),
                # Toast para notificações
                html.Div(
                    id="toast-container",
                    style={
                        "position": "fixed",
                        "top": "80px",
                        "right": "20px",
                        "zIndex": 9999,
                        "minWidth": "300px",
                    },
                ),
            ],
            fluid=False,  # Container com margens laterais
            className="pb-2 px-2",  # Padding bottom e lateral
        )
    ],
)


logger.info("✅ Página inicial carregada com sucesso")
