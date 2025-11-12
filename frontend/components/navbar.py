"""
Navbar profissional para ETO Calculator - Estilo C4AI.
Inclui logo, links internos + botão de tradução PT/EN.
"""

import dash_bootstrap_components as dbc
from dash import html


def create_navbar():
    """
    Cria navbar responsiva no estilo C4AI com botão de tradução.
    Returns:
        dbc.Navbar: Navbar completa.
    """
    navbar = dbc.Navbar(
        [
            dbc.Container(
                [
                    # Brand com texto destacado (sem logo)
                    html.Div(
                        [
                            html.A(
                                html.Div(
                                    [
                                        html.Div(
                                            "EVAonline",
                                            style={
                                                "fontSize": "1.8rem",  # Aumentado de 1.5rem
                                                "fontWeight": "700",
                                                "color": "#00695c",
                                                "lineHeight": "1.2",
                                                "marginBottom": "0",
                                            },
                                        ),
                                        html.Div(
                                            "Online tool for reference EVApotranspiration estimation",
                                            style={
                                                "fontSize": "0.8rem",  # Aumentado de 0.75rem
                                                "fontWeight": "400",
                                                "color": "#666666",
                                                "lineHeight": "1.2",
                                                "marginTop": "2px",
                                            },
                                        ),
                                    ],
                                    style={
                                        "display": "flex",
                                        "flexDirection": "column",
                                    },
                                ),
                                href="/",
                                style={
                                    "textDecoration": "none",
                                },
                            ),
                        ],
                        className="navbar-brand",
                    ),
                    # Toggle para mobile (collapse)
                    dbc.NavbarToggler(id="navbar-toggler"),
                    # Links principais + Botão de tradução (direita)
                    dbc.Collapse(
                        dbc.Nav(
                            [
                                # Links internos (pages/) na ordem correta
                                dbc.NavItem(
                                    dbc.NavLink(
                                        "HOME",
                                        href="/",
                                        id="nav-home",
                                        className="nav-link-custom",
                                        style={
                                            "fontWeight": "500",
                                            "fontSize": "0.95rem",
                                            "color": "#424242",
                                            "textTransform": "uppercase",
                                            "letterSpacing": "0.5px",
                                        },
                                    )
                                ),
                                dbc.NavItem(
                                    dbc.NavLink(
                                        "CALCULAR ETO",
                                        href="/eto-calculator",
                                        id="nav-eto",
                                        className="nav-link-custom",
                                        style={
                                            "fontWeight": "500",
                                            "fontSize": "0.95rem",
                                            "color": "#424242",
                                            "textTransform": "uppercase",
                                            "letterSpacing": "0.5px",
                                        },
                                    )
                                ),
                                dbc.NavItem(
                                    dbc.NavLink(
                                        "DOCUMENTAÇÃO",
                                        href="/documentation",
                                        id="nav-documentation",
                                        className="nav-link-custom",
                                        style={
                                            "fontWeight": "500",
                                            "fontSize": "0.95rem",
                                            "color": "#424242",
                                            "textTransform": "uppercase",
                                            "letterSpacing": "0.5px",
                                        },
                                    )
                                ),
                                dbc.NavItem(
                                    dbc.NavLink(
                                        "SOBRE",
                                        href="/about",
                                        id="nav-about",
                                        className="nav-link-custom",
                                        style={
                                            "fontWeight": "500",
                                            "fontSize": "0.95rem",
                                            "color": "#424242",
                                            "textTransform": "uppercase",
                                            "letterSpacing": "0.5px",
                                        },
                                    )
                                ),
                                dbc.NavItem(
                                    dbc.NavLink(
                                        [
                                            html.Img(
                                                src="/assets/images/github.svg",
                                                alt="GitHub",
                                                height="20",
                                                className="me-2",
                                                style={
                                                    "filter": "invert(26%) sepia(0%) saturate(0%) hue-rotate(180deg) brightness(95%) contrast(88%)"
                                                },
                                            ),
                                            "GITHUB",
                                        ],
                                        href="https://github.com/angelacunhasoares/EVAonline_SoftwareX",
                                        target="_blank",
                                        className="nav-link-custom",
                                        style={
                                            "fontWeight": "500",
                                            "fontSize": "0.95rem",
                                            "color": "#424242",
                                            "textTransform": "uppercase",
                                            "letterSpacing": "0.5px",
                                            "display": "flex",
                                            "alignItems": "center",
                                        },
                                    )
                                ),
                                # Botão de Tradução (estilo C4AI - verde teal com largura fixa)
                                dbc.NavItem(
                                    dbc.Button(
                                        html.Span(
                                            id="language-label",
                                            children="ENGLISH",
                                        ),
                                        id="language-toggle",
                                        color="primary",
                                        className="ms-3",
                                        style={
                                            "backgroundColor": "#00695c",
                                            "borderColor": "#00695c",
                                            "fontWeight": "600",
                                            "fontSize": "0.9rem",
                                            "padding": "8px 20px",
                                            "textTransform": "uppercase",
                                            "letterSpacing": "0.5px",
                                            "borderRadius": "4px",
                                            "minWidth": "130px",  # Largura fixa
                                            "textAlign": "center",
                                        },
                                    ),
                                    className="d-flex align-items-center",
                                ),
                            ],
                            className="ms-auto",
                            navbar=True,
                        ),
                        id="navbar-collapse",
                        navbar=True,
                    ),
                ],
                fluid=False,  # Container com margens laterais iguais ao conteúdo
            ),
        ],
        color="light",  # Fundo claro como C4AI
        light=True,
        className="navbar-expand-lg shadow-sm",
        style={
            "backgroundColor": "#ffffff",
            "borderBottom": "1px solid #e0e0e0",
            "padding": "0.5rem 0",
        },
    )

    return navbar
