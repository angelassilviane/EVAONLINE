"""
Módulo de monitoramento e health checks para o banco de dados.
"""

import time
from contextlib import contextmanager
from typing import Any, Dict

from loguru import logger
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from backend.database.connection import SessionLocal, engine
from backend.database.redis_pool import get_redis_client


def check_database_connection() -> Dict[str, Any]:
    """
    Verifica a saúde da conexão com o banco PostgreSQL.

    Returns:
        Dict com status e métricas da conexão
    """
    start_time = time.time()

    try:
        with SessionLocal() as session:
            # Query simples para testar conexão
            result = session.execute(
                text("SELECT 1 as health_check")
            ).fetchone()
            response_time = time.time() - start_time

            if result and result[0] == 1:
                return {
                    "status": "healthy",
                    "response_time": round(response_time * 1000, 2),  # ms
                    "database": "postgresql",
                    "message": "Database connection is healthy",
                }
            else:
                return {
                    "status": "unhealthy",
                    "response_time": round(response_time * 1000, 2),
                    "database": "postgresql",
                    "message": "Database returned unexpected result",
                }

    except SQLAlchemyError as e:
        response_time = time.time() - start_time
        return {
            "status": "unhealthy",
            "response_time": round(response_time * 1000, 2),
            "database": "postgresql",
            "message": f"Database connection failed: {str(e)}",
        }


def check_redis_connection() -> Dict[str, Any]:
    """
    Verifica a saúde da conexão com o Redis.

    Returns:
        Dict com status e métricas da conexão
    """
    start_time = time.time()

    try:
        redis_client = get_redis_client()
        redis_client.ping()
        response_time = time.time() - start_time

        # Informações adicionais do Redis
        info = redis_client.info()
        memory_used = info.get("used_memory_human", "unknown")
        connected_clients = info.get("connected_clients", "unknown")

        return {
            "status": "healthy",
            "response_time": round(response_time * 1000, 2),
            "cache": "redis",
            "memory_used": memory_used,
            "connected_clients": connected_clients,
            "message": "Redis connection is healthy",
        }

    except Exception as e:
        response_time = time.time() - start_time
        return {
            "status": "unhealthy",
            "response_time": round(response_time * 1000, 2),
            "cache": "redis",
            "message": f"Redis connection failed: {str(e)}",
        }


def get_database_metrics() -> Dict[str, Any]:
    """
    Coleta métricas de performance do banco de dados.

    Returns:
        Dict com métricas do banco
    """
    try:
        with SessionLocal() as session:
            # Número de conexões ativas
            connections_query = text(
                """
                SELECT count(*) as active_connections
                FROM pg_stat_activity
                WHERE state = 'active'
            """
            )
            connections_result = session.execute(connections_query).fetchone()

            # Tamanho do banco
            size_query = text(
                """
                SELECT pg_size_pretty(pg_database_size(current_database()))
                as db_size
            """
            )
            size_result = session.execute(size_query).fetchone()

            # Número de tabelas
            tables_query = text(
                """
                SELECT count(*) as table_count
                FROM information_schema.tables
                WHERE table_schema = 'public'
            """
            )
            tables_result = session.execute(tables_query).fetchone()

            return {
                "active_connections": (
                    connections_result[0] if connections_result else 0
                ),
                "database_size": size_result[0] if size_result else "unknown",
                "table_count": tables_result[0] if tables_result else 0,
                "pool_size": getattr(engine.pool, "size", lambda: 0)(),
                "pool_checked_out": getattr(
                    engine.pool, "checkedout", lambda: 0
                )(),
                "pool_overflow": getattr(engine.pool, "overflow", lambda: 0)(),
            }

    except SQLAlchemyError as e:
        return {"error": f"Failed to collect database metrics: {str(e)}"}


def perform_full_health_check() -> Dict[str, Any]:
    """
    Executa verificação completa de saúde de todos os componentes.

    Returns:
        Dict com status geral e detalhes de cada componente
    """
    results = {
        "timestamp": time.time(),
        "overall_status": "healthy",
        "checks": {},
    }

    # Verificar banco de dados
    db_check = check_database_connection()
    results["checks"]["database"] = db_check

    # Verificar Redis
    redis_check = check_redis_connection()
    results["checks"]["redis"] = redis_check

    # Coletar métricas se tudo estiver saudável
    if db_check["status"] == "healthy":
        results["metrics"] = get_database_metrics()

    # Determinar status geral
    unhealthy_condition = (
        db_check["status"] == "unhealthy"
        or redis_check["status"] == "unhealthy"
    )
    if unhealthy_condition:
        results["overall_status"] = "unhealthy"

    return results


@contextmanager
def database_monitoring_context(operation_name: str):
    """
    Context manager para monitoramento de operações de banco.

    Args:
        operation_name: Nome da operação para logging
    """
    start_time = time.time()

    try:
        yield
        duration = time.time() - start_time
        # Aqui poderia enviar métricas para Prometheus/monitoring
        logger.info(
            f"Database operation '{operation_name}' "
            f"completed in {duration:.3f}s"
        )

    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            f"Database operation '{operation_name}' "
            f"failed after {duration:.3f}s: {e}"
        )
        raise
