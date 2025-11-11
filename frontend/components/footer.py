"""
Componente de footer (rodapé) para o ETO Calculator - Versão com Colunas.
Colunas: Desenvolvedores | Logos Parceiros | Links Importantes.
"""

import logging
from datetime import datetime
from functools import lru_cache
from typing import Dict, List

import dash_bootstrap_components as dbc
from dash import html

logger = logging.getLogger(__name__)


class FooterManager:
    """Gerencia dados do footer com cache."""

    def __init__(self):
        self._current_year = datetime.now().year

    @property
    def current_year(self) -> int:
        return self._current_year

    @lru_cache(maxsize=1)
    def get_developer_data(self) -> List[Dict]:
        """Desenvolvedores com emails."""
        return [
            {
                "name": "Angela Cristina Cunha Soares",
                "email": "angelacunhasoares@usp.br",
                "institution": "ESALQ/USP",
                "role": "Desenvolvedora Principal",
            },
            {
                "name": "Patricia A. A. Marques",
                "email": "paamarques@usp.br",
                "institution": "ESALQ/USP",
                "role": "Pesquisadora",
            },
            {
                "name": "Carlos D. Maciel",
                "email": "carlos.maciel@unesp.br",
                "institution": "UNESP",
                "role": "Coordenador",
            },
        ]

    @lru_cache(maxsize=1)
    def get_partner_data(self) -> Dict[str, str]:
        """Parceiros com URLs para logos."""
        return {
            "esalq": "https://www.esalq.usp.br/",
            "usp": "https://www.usp.br/",
            "fapesp": "https://fapesp.br/",
            "ibm": "https://www.ibm.com/br-pt",
            "c4ai": "https://c4ai.inova.usp.br/",
            "leb": "http://www.leb.esalq.usp.br/",
        }

    @lru_cache(maxsize=1)
    def get_logo_extensions(self) -> Dict[str, str]:
        """Extensões dos arquivos de logo (padrão: .png)."""
        return {
            "leb": ".jpg",  # LEB usa .jpg
            # Todos os outros usam .png por padrão
        }

    def get_logo_path(self, partner: str) -> str:
        """Retorna o caminho completo do logo com a extensão correta."""
        extension = self.get_logo_extensions().get(partner, ".png")
        return f"/assets/images/logo_{partner}{extension}"

    def get_email_link(self, email: str) -> str:
        """Link mailto simples."""
        return f"mailto:{email}"


# Instância global
footer_manager = FooterManager()


def create_footer(lang: str = "pt") -> html.Footer:
    """
    Cria footer com 3 colunas responsivas.
    Args:
        lang: 'pt' ou 'en'.
    Returns:
        html.Footer: Footer columnar.
    """
    logger.debug("🔄 Criando footer com colunas")
    try:
        texts = _get_footer_texts(lang)

        return html.Footer(
            dbc.Container(
                [
                    # Linha principal: Sobre + Tecnologias (seu atual, adaptado)
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.H5(
                                        "ETO Calculator",
                                        className="fw-bold mb-3 text-white",
                                    ),
                                    html.P(
                                        "Cálculo de ETo com dados climáticos e mapa interativo.",
                                        className="text-white-50 small",
                                    ),
                                ],
                                md=6,
                            ),
                            dbc.Col(
                                [
                                    html.H5(
                                        "Tecnologias",
                                        className="fw-bold mb-3 text-white",
                                    ),
                                    html.P(
                                        "Dash • Plotly • FastAPI • Python • Bootstrap",
                                        className="text-white-50 small",
                                    ),
                                ],
                                md=6,
                            ),
                        ],
                        className="py-3",
                    ),
                    # Nova Linha: 3 Colunas Principais
                    dbc.Row(
                        [
                            # Coluna 1: Desenvolvedores
                            dbc.Col(
                                [
                                    html.H6(
                                        texts["developers"],
                                        className="mb-3 text-white-50",
                                    ),
                                    html.Ul(
                                        [
                                            html.Li(
                                                [
                                                    html.Strong(dev["name"]),
                                                    html.Br(),
                                                    html.Small(
                                                        dev["institution"]
                                                        + " • "
                                                        + dev["role"],
                                                        className="text-white-50",
                                                    ),
                                                    html.Br(),
                                                    html.A(
                                                        dev["email"],
                                                        href=footer_manager.get_email_link(
                                                            dev["email"]
                                                        ),
                                                        className="text-white-50 small d-block",
                                                        target="_blank",
                                                    ),
                                                ],
                                                className="mb-3 small",
                                            )
                                            for dev in footer_manager.get_developer_data()
                                        ],
                                        className="list-unstyled",
                                    ),
                                ],
                                md=4,
                            ),
                            # Coluna 2: Logos Parceiros
                            dbc.Col(
                                [
                                    html.H6(
                                        texts["partners"],
                                        className="mb-3 text-white-50 text-center",
                                    ),
                                    html.Div(
                                        [
                                            html.A(
                                                html.Img(
                                                    src=footer_manager.get_logo_path(
                                                        partner
                                                    ),
                                                    alt=f"Logo {partner.upper()}",
                                                    style={
                                                        "height": "50px",
                                                        "maxWidth": "120px",
                                                        "margin": "8px auto",
                                                        "display": "block",
                                                        "padding": "8px",
                                                        "background": "white",
                                                        "borderRadius": "8px",
                                                        "opacity": "0.9",
                                                        "transition": "all 0.3s",
                                                        "objectFit": "contain",
                                                    },
                                                    className="logo-partner",
                                                ),
                                                href=url,
                                                target="_blank",
                                                rel="noopener noreferrer",
                                                title=f"Visitar {partner.upper()}",
                                                style={
                                                    "textDecoration": "none",
                                                },
                                            )
                                            for partner, url in footer_manager.get_partner_data().items()
                                        ],
                                        className="d-flex justify-content-center flex-wrap align-items-center gap-2",
                                        style={
                                            "maxWidth": "400px",
                                            "margin": "0 auto",
                                        },
                                    ),
                                ],
                                md=4,
                                className="text-center",
                            ),
                            # Coluna 3: Links Importantes
                            dbc.Col(
                                [
                                    html.H6(
                                        texts["links"],
                                        className="mb-3 text-white-50",
                                    ),
                                    html.Ul(
                                        [
                                            html.Li(
                                                html.A(
                                                    "GitHub Repo",
                                                    href="https://github.com/angelacunhasoares/EVAonline_SoftwareX",
                                                    target="_blank",
                                                    className="text-white-50 d-block small mb-2",
                                                    rel="noopener noreferrer",
                                                ),
                                                className="mb-2",
                                            ),
                                            html.Li(
                                                html.A(
                                                    "Licença do Software",
                                                    href="https://github.com/angelacunhasoares/EVAonline_SoftwareX?tab=License-1-ov-file",
                                                    target="_blank",
                                                    className="text-white-50 d-block small mb-2",
                                                    rel="noopener noreferrer",
                                                ),
                                                className="mb-2",
                                            ),
                                            html.Li(
                                                html.A(
                                                    "Documentação",
                                                    href="/documentation",  # Linka para sua página documentation.py
                                                    className="text-white-50 d-block small",
                                                ),
                                                className="mb-2",
                                            ),
                                        ],
                                        className="list-unstyled",
                                    ),
                                ],
                                md=4,
                            ),
                        ],
                        className="mb-4",
                    ),
                    # Linha Copyright (simples, abaixo das colunas)
                    html.Hr(className="my-2 bg-white-50"),
                    dbc.Row(
                        [
                            dbc.Col(
                                html.P(
                                    [
                                        f"© {footer_manager.current_year} ETO Calculator. ",
                                        "Todos os direitos reservados. ",
                                        "Desenvolvido com ",
                                        html.I(
                                            className="bi bi-heart-fill text-danger mx-1"
                                        ),
                                        " pela ESALQ/USP.",
                                    ],
                                    className="text-center mb-0 small text-white-50",
                                ),
                                width=12,
                            ),
                        ]
                    ),
                ],
                fluid=True,
            ),
            className="bg-dark text-white mt-auto py-3",
            style={"marginTop": "auto"},
        )
    except Exception as e:
        logger.error(f"❌ Erro ao criar footer: {e}")
        return _create_fallback_footer()


# Funções Auxiliares (simplificadas)
def _get_footer_texts(lang: str) -> Dict:
    """Textos i18n."""
    texts = {
        "pt": {
            "developers": "Desenvolvedores",
            "partners": "Parceiros",
            "links": "Links Importantes",
        },
        "en": {
            "developers": "Developers",
            "partners": "Partners",
            "links": "Important Links",
        },
    }
    return texts.get(lang, texts["pt"])


def _create_fallback_footer():
    """Fallback simples."""
    return html.Footer(
        html.Div(
            html.P(
                "© 2024 ETO Calculator - Desenvolvido com ❤️ pela ESALQ/USP",
                className="text-center text-muted py-3 mb-0 small",
            ),
            className="bg-light border-top",
        )
    )


# Footer minimalista (mantido do seu original)
def create_simple_footer(lang: str = "pt") -> html.Footer:
    """Versão minimalista."""
    texts = _get_footer_texts(lang)
    return html.Footer(
        dbc.Container(
            html.Div(
                [
                    html.P(
                        [
                            f"© {footer_manager.current_year} ETO Calculator | ",
                            html.A(
                                "Documentação",
                                href="/documentation",
                                className="text-muted",
                            ),
                            " | ",
                            html.A(
                                "Sobre", href="/about", className="text-muted"
                            ),
                            " | ",
                            html.A(
                                "ESALQ/USP",
                                href="https://www.esalq.usp.br/",
                                target="_blank",
                                className="text-muted",
                            ),
                        ],
                        className="text-center mb-0 small",
                    ),
                ],
                className="py-3",
            ),
            fluid=True,
        ),
        className="bg-light border-top",
    )
