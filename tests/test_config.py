"""
Tests para el módulo de configuración.

Valida que la configuración se carga correctamente y las validaciones funcionan.
"""

import os
import pytest
from decimal import Decimal

# Importar módulo de configuración
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import load_config, get_config, ConfigError, AppConfig


class TestConfigLoading:
    """Tests para carga de configuración."""

    def test_load_config_with_valid_env(self, monkeypatch):
        """Test que carga configuración con variables válidas."""
        # Configurar variables de entorno mínimas requeridas
        monkeypatch.setenv("PRIVATE_KEY", "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
        monkeypatch.setenv("RPC_URL", "https://polygon-rpc.com")
        monkeypatch.setenv("POLYMARKET_API_KEY", "test_api_key")
        monkeypatch.setenv("CONDITION_ID", "test_condition_123")
        monkeypatch.setenv("MARKET_IDS", "market_1,market_2")
        monkeypatch.setenv("DRY_RUN", "true")

        config = load_config()

        assert isinstance(config, AppConfig)
        assert config.wallet.private_key == "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        assert config.wallet.rpc_url == "https://polygon-rpc.com"
        assert config.polymarket.api_key == "test_api_key"
        assert config.polymarket.condition_id == "test_condition_123"
        assert config.polymarket.market_ids == ["market_1", "market_2"]
        assert config.execution.dry_run is True

    def test_missing_required_env(self, monkeypatch):
        """Test que falla cuando falta una variable requerida."""
        # Limpiar todas las variables
        for key in ["PRIVATE_KEY", "RPC_URL", "POLYMARKET_API_KEY"]:
            monkeypatch.delenv(key, raising=False)

        with pytest.raises(ConfigError) as exc_info:
            load_config()

        assert "PRIVATE_KEY" in str(exc_info.value)

    def test_default_values(self, monkeypatch):
        """Test que los valores por defecto se aplican correctamente."""
        monkeypatch.setenv("PRIVATE_KEY", "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
        monkeypatch.setenv("RPC_URL", "https://polygon-rpc.com")
        monkeypatch.setenv("POLYMARKET_API_KEY", "test_key")
        monkeypatch.setenv("CONDITION_ID", "test_id")

        config = load_config()

        # Valores por defecto esperados
        assert config.weather_feed.latitude == 40.7128  # NYC por defecto
        assert config.weather_feed.longitude == -74.0060
        assert config.trading.bet_size_usd == 100.0
        assert config.trading.min_roi_threshold == 0.08
        assert config.risk.max_consecutive_losses == 3

    def test_custom_values(self, monkeypatch):
        """Test que valores personalizados se aplican correctamente."""
        monkeypatch.setenv("PRIVATE_KEY", "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
        monkeypatch.setenv("RPC_URL", "https://custom-rpc.com")
        monkeypatch.setenv("POLYMARKET_API_KEY", "custom_key")
        monkeypatch.setenv("CONDITION_ID", "custom_id")
        monkeypatch.setenv("MARKET_IDS", "m1,m2,m3")
        monkeypatch.setenv("BET_SIZE_USD", "500")
        monkeypatch.setenv("MIN_ROI_THRESHOLD", "0.15")
        monkeypatch.setenv("WEATHER_LAT", "51.5074")  # Londres
        monkeypatch.setenv("WEATHER_LON", "-0.1278")

        config = load_config()

        assert config.trading.bet_size_usd == 500.0
        assert config.trading.min_roi_threshold == 0.15
        assert config.weather_feed.latitude == 51.5074
        assert config.weather_feed.longitude == -0.1278
        assert config.polymarket.market_ids == ["m1", "m2", "m3"]


class TestConfigSingleton:
    """Tests para el patrón singleton de configuración."""

    def test_get_config_returns_same_instance(self, monkeypatch):
        """Test que get_config() retorna la misma instancia."""
        monkeypatch.setenv("PRIVATE_KEY", "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
        monkeypatch.setenv("RPC_URL", "https://polygon-rpc.com")
        monkeypatch.setenv("POLYMARKET_API_KEY", "test_key")
        monkeypatch.setenv("CONDITION_ID", "test_id")

        # Resetear singleton
        import config
        config._config = None

        config1 = get_config()
        config2 = get_config()

        assert config1 is config2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
