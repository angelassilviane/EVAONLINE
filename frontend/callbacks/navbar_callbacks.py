"""
Callbacks para funcionalidades da navbar.
Inclui tradução PT/EN e destaque do link ativo.
"""

import logging

from dash import Input, Output, callback, State
from dash.exceptions import PreventUpdate

logger = logging.getLogger(__name__)


@callback(
    Output("language-label", "children"),
    Output("language-toggle", "style"),
    Output("language-store", "data"),
    Input("language-toggle", "n_clicks"),
    State("language-store", "data"),
    prevent_initial_call=True,
)
def toggle_language(n_clicks, current_language):
    """
    Alterna entre Português e Inglês ao clicar no botão.

    Args:
        n_clicks: Número de cliques no botão
        current_language: Idioma atual armazenado ("en" ou "pt")

    Returns:
        tuple: (novo_label, novo_estilo, novo_idioma_code)
    """
    if not n_clicks:
        raise PreventUpdate

    # Alterna o idioma
    if current_language == "en":
        new_language_code = "pt"
        new_label = "PORTUGUÊS"
        logger.info("✅ Idioma alterado para: Português")
    else:
        new_language_code = "en"
        new_label = "ENGLISH"
        logger.info("✅ Idioma alterado para: Inglês")

    # Estilo do botão (mantém o verde teal C4AI com largura fixa)
    button_style = {
        "backgroundColor": "#00695c",
        "borderColor": "#00695c",
        "fontWeight": "600",
        "fontSize": "0.9rem",
        "padding": "8px 20px",
        "textTransform": "uppercase",
        "letterSpacing": "0.5px",
        "borderRadius": "4px",
        "minWidth": "130px",  # Largura fixa para evitar mudança de tamanho
        "textAlign": "center",
    }

    return new_label, button_style, new_language_code


# Callback para destacar o link ativo (opcional - futuro)
@callback(
    [
        Output("nav-home", "style"),
        Output("nav-about", "style"),
        Output("nav-documentation", "style"),
        Output("nav-eto", "style"),
    ],
    Input("url", "pathname"),
    prevent_initial_call=True,
)
def highlight_active_link(pathname):
    """
    Destaca o link ativo na navbar baseado na URL atual.

    Args:
        pathname: URL atual da página

    Returns:
        tuple: Estilos para cada link (ativo ou padrão)
    """
    # Estilo base para links
    base_style = {
        "fontWeight": "500",
        "fontSize": "0.95rem",
        "color": "#424242",
        "textTransform": "uppercase",
        "letterSpacing": "0.5px",
    }

    # Estilo para link ativo (verde teal + negrito)
    active_style = {
        **base_style,
        "color": "#00695c",
        "fontWeight": "700",
        "borderBottom": "2px solid #00695c",
    }

    # Define qual link está ativo
    home_style = active_style if pathname == "/" else base_style
    about_style = active_style if pathname == "/about" else base_style
    docs_style = active_style if pathname == "/documentation" else base_style
    eto_style = active_style if pathname == "/eto-calculator" else base_style

    return home_style, about_style, docs_style, eto_style


logger.info("✅ Callbacks da navbar registrados com sucesso")
