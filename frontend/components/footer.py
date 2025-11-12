"""
Componente de footer (rodapé) profissional para o ETO Calculator - Versão com 4 Colunas.
Colunas: Logo | Desenvolvedores | Parceiros | Links Importantes.
Inspirado em footers acadêmicos clean e responsivos.
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
                "name": "Angela S. M. C. Soares",
                "email": "angelasilviane@alumni.usp.br",
                "institution": "ESALQ/USP",
            },
            {
                "name": "Patricia A. A. Marques",
                "email": "paamarques@usp.br",
                "institution": "ESALQ/USP",
            },
            {
                "name": "Carlos D. Maciel",
                "email": "carlos.maciel@unesp.br",
                "institution": "UNESP",
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
        """Extensões dos arquivos de logo (padrão: .svg)."""
        return {
            # Todos os logos agora são SVG
            "esalq": ".svg",
            "usp": ".svg",
            "fapesp": ".svg",
            "ibm": ".svg",
            "leb": ".svg",
        }

    def get_logo_path(self, partner: str) -> str:
        """Retorna o caminho completo do logo com a extensão correta."""
        extension = self.get_logo_extensions().get(partner, ".svg")
        return f"/assets/images/logo_{partner}{extension}"

    def get_email_link(self, email: str) -> str:
        """Link mailto simples."""
        return f"mailto:{email}"


# Instância global
footer_manager = FooterManager()


def create_footer(lang: str = "pt") -> html.Footer:
    """
    Cria footer profissional com 4 colunas responsivas.
    Args:
        lang: 'pt' ou 'en'.
    Returns:
        html.Footer: Footer columnar profissional.
    """
    logger.debug("🔄 Criando footer profissional com 3 colunas")
    try:
        texts = _get_footer_texts(lang)

        return html.Footer(
            [
                # Linha divisória sutil acima do footer (estilo C4AI)
                html.Hr(
                    className="m-0",
                    style={
                        "borderTop": "1px solid #dee2e6",
                        "opacity": "0.5",
                    },
                ),
                dbc.Container(
                    [
                        # ===== Linha Única: 3 Colunas =====
                        dbc.Row(
                            [
                                # Coluna 1: Desenvolvedores
                                dbc.Col(
                                    [
                                        html.H6(
                                            texts["developers"],
                                            className="mb-3 fw-bold text-center",
                                            style={"color": "#2c3e50"},
                                        ),
                                        html.Ul(
                                            [
                                                html.Li(
                                                    [
                                                        html.Strong(
                                                            dev["name"],
                                                            className="d-block",
                                                        ),
                                                        html.Span(
                                                            f"{dev['institution']}",
                                                            className="text-muted small d-block mb-1",
                                                        ),
                                                        html.A(
                                                            dev["email"],
                                                            href=footer_manager.get_email_link(
                                                                dev["email"]
                                                            ),
                                                            className="text-muted small",
                                                            style={
                                                                "textDecoration": "none",
                                                                "fontSize": "0.875rem",
                                                            },
                                                        ),
                                                    ],
                                                    className="mb-3 list-unstyled",
                                                )
                                                for dev in footer_manager.get_developer_data()
                                            ],
                                            className="list-unstyled",
                                        ),
                                    ],
                                    md=4,
                                    className="mb-4 text-center",
                                ),
                                # Coluna 2: Parceiros (logos maiores)
                                dbc.Col(
                                    [
                                        html.H6(
                                            texts["partners"],
                                            className="mb-3 fw-bold text-center",
                                            style={"color": "#2c3e50"},
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
                                                            "height": "75px",  # Aumentado de 65px
                                                            "maxWidth": "160px",  # Aumentado de 140px
                                                            "margin": "8px",
                                                            "display": "block",
                                                            "objectFit": "contain",
                                                            "opacity": "0.9",
                                                            "transition": "opacity 0.3s ease",
                                                        },
                                                        className="logo-partner",
                                                    ),
                                                    href=url,
                                                    target="_blank",
                                                    rel="noopener noreferrer",
                                                    title=f"Visitar {partner.upper()}",
                                                    style={
                                                        "textDecoration": "none",
                                                        ":hover .logo-partner": {
                                                            "opacity": "1.0"
                                                        },
                                                    },
                                                )
                                                for partner, url in footer_manager.get_partner_data().items()
                                            ],
                                            className="d-flex justify-content-center flex-wrap align-items-center",
                                        ),
                                    ],
                                    md=4,
                                    className="mb-4 text-center",
                                ),
                                # Coluna 3: Links Importantes (horizontal em uma linha)
                                dbc.Col(
                                    [
                                        html.H6(
                                            texts["links"],
                                            className="mb-3 fw-bold text-center",
                                            style={"color": "#2c3e50"},
                                        ),
                                        html.Div(
                                            [
                                                html.A(
                                                    [
                                                        html.Img(
                                                            src="/assets/images/github.svg",
                                                            alt="GitHub",
                                                            style={
                                                                "height": "40px",
                                                                "width": "40px",
                                                                "objectFit": "contain",
                                                            },
                                                            className="github-icon",
                                                        ),
                                                        html.Span(
                                                            "GitHub",
                                                            className="d-block small mt-1",
                                                            style={
                                                                "color": "#6c757d"
                                                            },
                                                        ),
                                                    ],
                                                    href=(
                                                        "https://github.com/"
                                                        "angelacunhasoares/"
                                                        "EVAonline_SoftwareX"
                                                    ),
                                                    target="_blank",
                                                    rel="noopener noreferrer",
                                                    title="Repositório GitHub",
                                                    style={
                                                        "textDecoration": "none",
                                                        "display": "flex",
                                                        "flexDirection": "column",
                                                        "alignItems": "center",
                                                        "margin": "0 10px",
                                                    },
                                                ),
                                                html.A(
                                                    [
                                                        html.I(
                                                            className=(
                                                                "bi bi-file-earmark-text"
                                                            ),
                                                            style={
                                                                "fontSize": "40px",
                                                                "color": "#6c757d",
                                                            },
                                                        ),
                                                        html.Span(
                                                            "Licença",
                                                            className="d-block small mt-1",
                                                            style={
                                                                "color": "#6c757d"
                                                            },
                                                        ),
                                                    ],
                                                    href=(
                                                        "https://github.com/"
                                                        "angelacunhasoares/"
                                                        "EVAonline_SoftwareX?"
                                                        "tab=License-1-ov-file"
                                                    ),
                                                    target="_blank",
                                                    rel="noopener noreferrer",
                                                    title="Licença MIT",
                                                    style={
                                                        "textDecoration": "none",
                                                        "display": "flex",
                                                        "flexDirection": "column",
                                                        "alignItems": "center",
                                                        "margin": "0 10px",
                                                    },
                                                    className="license-link",
                                                ),
                                                html.A(
                                                    [
                                                        html.I(
                                                            className=(
                                                                "bi bi-book"
                                                            ),
                                                            style={
                                                                "fontSize": "40px",
                                                                "color": "#6c757d",
                                                            },
                                                        ),
                                                        html.Span(
                                                            "Documentação",
                                                            className="d-block small mt-1",
                                                            style={
                                                                "color": "#6c757d"
                                                            },
                                                        ),
                                                    ],
                                                    href="/documentation",
                                                    title="Documentação",
                                                    style={
                                                        "textDecoration": "none",
                                                        "display": "flex",
                                                        "flexDirection": "column",
                                                        "alignItems": "center",
                                                        "margin": "0 10px",
                                                    },
                                                    className="docs-link",
                                                ),
                                            ],
                                            className=(
                                                "d-flex justify-content-center "
                                                "align-items-center flex-wrap"
                                            ),
                                        ),
                                    ],
                                    md=4,
                                    className="mb-4 text-center",
                                ),
                            ],
                            className="py-4 justify-content-center",
                        ),
                        # Linha de Copyright
                        html.Hr(
                            className="my-2", style={"borderColor": "#dee2e6"}
                        ),
                        dbc.Row(
                            [
                                dbc.Col(
                                    html.P(
                                        [
                                            f"Copyright ©{footer_manager.current_year} ",
                                            html.Strong("EVAonline"),
                                            ". Open-source sob licença ",
                                            html.A(
                                                "AGPLv3",
                                                href="https://github.com/angelassilviane/EVAONLINE/blob/main/LICENSE",
                                                target="_blank",
                                                rel="noopener noreferrer",
                                                className="text-muted",
                                                style={
                                                    "textDecoration": "underline"
                                                },
                                            ),
                                            ".",
                                        ],
                                        className="text-center mb-0 small text-muted",
                                    ),
                                    width=12,
                                ),
                            ],
                            className="mt-2",
                        ),
                    ],
                    fluid=False,  # Usar container fixo
                    style={
                        "paddingLeft": "40px",
                        "paddingRight": "40px",
                        "maxWidth": "1400px",
                    },
                ),
            ],
            className="bg-white",
            style={
                "marginTop": "10px",  # Margem reduzida
                "paddingTop": "0px",
                "paddingBottom": "15px",
                "overflowX": "hidden",
            },
        )
    except Exception as e:
        logger.error(f"❌ Erro ao criar footer: {e}")
        return _create_fallback_footer()


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
                "© 2025 ETO Calculator",
                className="text-center text-muted py-3 mb-0 small",
            ),
            className="bg-white border-top",
        )
    )


# Versão minimalista mantida para compatibilidade
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
            style={
                "paddingLeft": "40px",
                "paddingRight": "40px",
                "maxWidth": "100%",
            },
        ),
        className="bg-white border-top",
    )
