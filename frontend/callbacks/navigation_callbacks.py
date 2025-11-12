"""
CORRIGIDO
Callbacks para navegação entre páginas e controle de roteamento.

Features:
- Navegação entre Home, ETo, Documentação e Sobre
- Redirecionamento para página ETo com localização
- Controle de estado da navbar
- Integração com sistema de localização
"""

import logging

from dash import callback_context, html
from dash.dependencies import ALL, Input, Output, State

from ..pages.home import home_layout
from ..pages.dash_eto import eto_layout
from ..pages.about import about_layout
from ..pages.documentation import documentation_layout

logger = logging.getLogger(__name__)


def register_navigation_callbacks(app):
    """
    Registra todos os callbacks relacionados à navegação
    """

    # Navigation callback - Roteamento básico
    @app.callback(
        Output("page-content", "children"), [Input("url", "pathname")]
    )
    def display_page(pathname):
        """
        Controla a exibição das páginas baseado na URL.
        """
        logger.info(f"🧭 Navegando para: {pathname}")
        pages = {
            "/eto-calculator": eto_layout,  # ✅ Rota principal
            "/about": about_layout,
            "/documentation": documentation_layout,
        }
        return pages.get(pathname, home_layout)

    # Navigation callback - Ir para página ETo com localização
    # Navigation callback - Ir para página ETo com localização
    @app.callback(
        Output("url", "pathname"),
        [
            Input("calc-eto-button", "n_clicks"),
            Input({"type": "calc-fav-eto", "index": ALL}, "n_clicks"),
        ],
        [State("current-location", "data"), State("favorites-store", "data")],
        prevent_initial_call=True,
    )
    def navigate_to_eto(
        n_clicks, fav_clicks_list, current_location, favorites
    ):
        """
        Navega para a página ETo quando usuário clica em botões de cálculo
        """
        ctx = callback_context
        if not ctx.triggered:
            return "/"

        trigger_id = ctx.triggered[0]["prop_id"]

        # 📊 Botão "Calcular ETo" principal
        if "calc-eto-button" in trigger_id and n_clicks > 0:
            if current_location and current_location.get("lat"):
                logger.info("📍 Navegando para ETo com localização atual")
                return "/eto-calculator"
            else:
                logger.warning(
                    "❌ Tentativa de navegação sem localização selecionada"
                )
                return "/"

        # ⭐ Botão "Calcular ETo" em favoritos
        elif "calc-fav-eto" in trigger_id:
            # Encontrar qual favorito foi clicado
            try:
                fav_id = eval(trigger_id.split(".")[0])["index"]

                # Buscar dados do favorito
                favorite = next(
                    (fav for fav in favorites if fav["id"] == fav_id), None
                )
                if favorite:
                    logger.info(
                        f"⭐ Navegando para ETo com favorito: {favorite.get('location_info', 'Unknown')}"
                    )
                    return "/eto-calculator"  # ✅ Corrigido
            except Exception as e:
                logger.error(f"Erro ao navegar com favorito: {e}")

        return "/"

    # Navigation callback - Navbar links
    @app.callback(
        Output("url", "pathname", allow_duplicate=True),
        [
            Input("nav-home", "n_clicks"),
            Input("nav-eto", "n_clicks"),
            Input("nav-documentation", "n_clicks"),
            Input("nav-about", "n_clicks"),
        ],
        prevent_initial_call=True,
    )
    def handle_navbar_navigation(
        home_clicks, eto_clicks, doc_clicks, about_clicks
    ):
        """
        Manipula navegação pela navbar
        """
        ctx = callback_context
        if not ctx.triggered:
            return "/"

        trigger_id = ctx.triggered[0]["prop_id"]

        if "nav-home" in trigger_id and home_clicks:
            logger.info("🏠 Navegando para Home")
            return "/"
        elif "nav-eto" in trigger_id and eto_clicks:
            logger.info("📊 Navegando para ETo")
            return "/eto-calculator"  # ✅ Corrigido
        elif "nav-documentation" in trigger_id and doc_clicks:
            logger.info("📚 Navegando para Documentação")
            return "/documentation"
        elif "nav-about" in trigger_id and about_clicks:
            logger.info("ℹ️ Navegando para Sobre")
            return "/about"

        return "/"

    # Navigation callback - Toggle navbar em mobile
    @app.callback(
        Output("navbar-collapse", "is_open"),
        [Input("navbar-toggler", "n_clicks")],
        [State("navbar-collapse", "is_open")],
    )
    def toggle_navbar(n_clicks, is_open):
        """
        Alterna a navbar em dispositivos móveis
        """
        if n_clicks:
            logger.debug("📱 Alternando estado da navbar")
            return not is_open
        return is_open

    # Navigation callback - Atualiza links ativos na navbar
    @app.callback(
        [
            Output("nav-home", "active"),
            Output("nav-eto", "active"),
            Output("nav-documentation", "active"),
            Output("nav-about", "active"),
        ],
        [Input("url", "pathname")],
    )
    def update_navbar_active_links(pathname):
        """
        Atualiza os links ativos na navbar baseado na página atual
        """
        if pathname == "/eto-calculator":
            return False, True, False, False
        elif pathname == "/documentation":
            return False, False, True, False
        elif pathname == "/about":
            return False, False, False, True
        else:  # Home ou qualquer outra página
            return True, False, False, False

    # Navigation callback - Atualiza título da página baseado na rota
    @app.callback(Output("page-title", "children"), [Input("url", "pathname")])
    def update_page_title(pathname):
        """
        Atualiza o título da aba do navegador baseado na página atual.
        """
        titles = {
            "/": "🌦️ EVAonline: Home",
            "/eto-calculator": "🌦️ EVAonline: Calcular ETo",
            "/documentation": "🌦️ EVAonline: Documentação",
            "/about": "🌦️ EVAonline: Sobre",
        }
        return titles.get(pathname, "🌦️ EVAonline")

    # Navigation callback - Simula loading entre páginas
    @app.callback(
        Output("page-loading", "children"), [Input("url", "pathname")]
    )
    def handle_page_loading(pathname):
        """
        Simula loading entre páginas (pode ser usado para mostrar spinner)
        """
        logger.info(f"🔄 Carregando página: {pathname}")
        return html.Div()  # Pode ser extendido para mostrar loading spinner

    # Final do registro de callbacks
    logger.info("✅ Callbacks de navegação registrados com sucesso")
