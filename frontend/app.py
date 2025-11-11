"""
Aplicação Dash do ETO Calculator (integrada ao Backend FastAPI).

Exports:
- create_dash_app(): Factory para criar instância Dash
- register_all_callbacks(): Registra todos callbacks

Integração:
- Montada pelo backend/main.py como sub-aplicação
- Backend roda em http://localhost:8000
- Dash frontend em http://localhost:8000/
- API em http://localhost:8000/api/v1/...
"""

import logging
import os
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Add frontend to path for imports
frontend_path = Path(__file__).parent
if str(frontend_path) not in sys.path:
    sys.path.insert(0, str(frontend_path))

# Import components (always use absolute imports now)
from frontend.core.dash_app_config import create_dash_app
from frontend.core.base_layout import create_base_layout
from frontend.callbacks.registry import register_all_callbacks
from frontend.utils.logging_config import setup_logging

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


def initialize_dash_app(standalone=False):
    """
    Inicializa a aplicação Dash com layout e callbacks.
    Called by: backend/main.py → mount_dash()
    Args:
        standalone: Se True, configura para execução independente
    Returns:
        tuple: (app, server) - Instâncias Dash e Flask
    """
    logger.info("🔄 Inicializando Dash Frontend...")
    # Criar instância Dash + Flask server
    app, server = create_dash_app(standalone=standalone)
    # Configurar layout
    app.layout = create_base_layout()
    # Registrar callbacks
    register_all_callbacks(app)
    logger.info("✅ Dash Frontend inicializado com sucesso")
    return app, server


# Criar instância global quando módulo é importado
standalone_mode = os.getenv("EVA_FRONTEND_STANDALONE") == "1"
app, server = initialize_dash_app(standalone=standalone_mode)

if __name__ == "__main__":
    print("🚀 Iniciando EVAonline Frontend (Dash) na porta 8050...")
    print("📡 API disponível em: http://localhost:8000")
    print("🌐 Frontend disponível em: http://localhost:8050")

    # Re-inicializar com configuração standalone
    print("🔧 Re-inicializando com modo standalone...")
    app, server = initialize_dash_app(standalone=True)

    # Run the Dash server
    server.run(host="0.0.0.0", port=8050, debug=False, threaded=True)
