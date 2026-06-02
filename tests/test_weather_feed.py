"""
Tests para FastWeatherFeed - Feed de datos climáticos.

Valida:
- Parsing de respuestas de WeatherAPI
- Validación de datos de sensor
- Cálculo de latencia
- Manejo de errores y retries

Ejecutar: pytest tests/test_weather_feed.py -v
"""

import os
import sys
import time
import pytest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.fast_weather_feed import (
    FastWeatherFeed,
    FeedMetrics,
    RetryConfig,
    SensorValidationError,
    WeatherFeedState,
    create_weather_feed,
)
from models import WeatherObservation


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def sample_weatherapi_response():
    """Respuesta típica de WeatherAPI.com."""
    return {
        "location": {
            "name": "New York",
            "region": "New York",
            "country": "United States of America",
            "lat": 40.71,
            "lon": -74.01,
            "tz_id": "America/New_York",
            "localtime_epoch": 1699999999,
            "localtime": "2024-01-15 14:00",
        },
        "current": {
            "last_updated_epoch": 1699999999,
            "last_updated": "2024-01-15 14:00",
            "temp_c": 25.5,
            "temp_f": 77.9,
            "is_day": 1,
            "condition": {
                "text": "Partly cloudy",
                "icon": "//cdn.weatherapi.com/weather/64x64/day/116.png",
                "code": 1003,
            },
            "wind_mph": 10.5,
            "wind_kph": 16.9,
            "wind_degree": 270,
            "wind_dir": "W",
            "pressure_mb": 1013.0,
            "pressure_in": 29.91,
            "precip_mm": 0.0,
            "precip_in": 0.0,
            "humidity": 60,
            "cloud": 25,
            "feelslike_c": 26.0,
            "feelslike_f": 78.8,
            "vis_km": 10.0,
            "vis_miles": 6.0,
            "uv": 5.0,
            "gust_mph": 15.0,
            "gust_kph": 24.1,
        }
    }


@pytest.fixture
def mock_config():
    """Configuración mock para tests."""
    config = MagicMock()
    config.weather_feed.api_key = "test_api_key"
    config.weather_feed.latitude = 40.7128
    config.weather_feed.longitude = -74.0060
    config.weather_feed.heartbeat_timeout_sec = 5
    config.performance.network_timeout_sec = 5
    config.performance.max_retries = 3
    config.performance.retry_delay_sec = 0.1
    config.risk.max_feed_latency_ms = 500
    return config


@pytest.fixture
def weather_feed(mock_config):
    """Crea un FastWeatherFeed para testing."""
    return FastWeatherFeed(config=mock_config, poll_interval=0.1)


# =============================================================================
# TESTS DE MODELOS
# =============================================================================

class TestFeedMetrics:
    """Tests para FeedMetrics."""

    def test_initial_metrics(self):
        """Test que las métricas inicializan correctamente."""
        metrics = FeedMetrics()

        assert metrics.observations_received == 0
        assert metrics.observations_valid == 0
        assert metrics.avg_latency_ms == 0.0
        assert metrics.min_latency_ms == float('inf')
        assert metrics.max_latency_ms == 0.0

    def test_record_latency(self):
        """Test que registra latencia correctamente."""
        metrics = FeedMetrics()

        metrics.record_latency(50.0)
        assert metrics.last_latency_ms == 50.0
        assert metrics.min_latency_ms == 50.0
        assert metrics.max_latency_ms == 50.0
        assert metrics.avg_latency_ms == 50.0

        metrics.record_latency(100.0)
        assert metrics.last_latency_ms == 100.0
        assert metrics.min_latency_ms == 50.0
        assert metrics.max_latency_ms == 100.0
        # Moving average: 50 * 0.85 + 100 * 0.15 = 57.5
        assert abs(metrics.avg_latency_ms - 57.5) < 0.1

    def test_record_retry(self):
        """Test que registra reintentos correctamente."""
        metrics = FeedMetrics()

        metrics.record_retry()
        assert metrics.consecutive_retries == 1
        assert metrics.max_consecutive_retries == 1

        metrics.record_retry()
        assert metrics.consecutive_retries == 2
        assert metrics.max_consecutive_retries == 2

        metrics.reset_retry_counter()
        assert metrics.consecutive_retries == 0
        assert metrics.max_consecutive_retries == 2  # No se resetea el máximo


class TestRetryConfig:
    """Tests para RetryConfig."""

    def test_default_config(self):
        """Test configuración por defecto."""
        config = RetryConfig()

        assert config.max_retries == 3
        assert config.base_delay_ms == 100
        assert config.max_delay_ms == 5000
        assert config.exponential_base == 2.0


# =============================================================================
# TESTS DE VALIDACIÓN
# =============================================================================

class TestSensorValidation:
    """Tests para validación de datos de sensor."""

    def test_valid_data(self, weather_feed, sample_weatherapi_response):
        """Test que datos válidos pasan validación."""
        is_valid, error_msg = weather_feed._validate_sensor_data(sample_weatherapi_response)

        assert is_valid is True
        assert error_msg is None

    def test_missing_current_field(self, weather_feed):
        """Test que falla si falta campo 'current'."""
        data = {"location": {"name": "NYC"}}

        is_valid, error_msg = weather_feed._validate_sensor_data(data)

        assert is_valid is False
        assert "current" in error_msg

    def test_missing_temperature(self, weather_feed, sample_weatherapi_response):
        """Test que falla si falta temperatura."""
        sample_weatherapi_response["current"]["temp_c"] = None

        is_valid, error_msg = weather_feed._validate_sensor_data(sample_weatherapi_response)

        assert is_valid is False
        assert "Temperatura faltante" in error_msg

    def test_non_numeric_temperature(self, weather_feed, sample_weatherapi_response):
        """Test que falla si temperatura no es numérica."""
        sample_weatherapi_response["current"]["temp_c"] = "hot"

        is_valid, error_msg = weather_feed._validate_sensor_data(sample_weatherapi_response)

        assert is_valid is False
        assert "no numérica" in error_msg

    def test_temperature_out_of_range_too_hot(self, weather_feed, sample_weatherapi_response):
        """Test que falla si temperatura es demasiado alta."""
        sample_weatherapi_response["current"]["temp_c"] = 70.0  # Más de 60°C

        with pytest.raises(SensorValidationError) as exc_info:
            weather_feed._validate_sensor_data(sample_weatherapi_response)

        assert "fuera de rango físico" in str(exc_info.value)

    def test_temperature_out_of_range_too_cold(self, weather_feed, sample_weatherapi_response):
        """Test que falla si temperatura es demasiado baja."""
        sample_weatherapi_response["current"]["temp_c"] = -100.0  # Menos de -90°C

        with pytest.raises(SensorValidationError) as exc_info:
            weather_feed._validate_sensor_data(sample_weatherapi_response)

        assert "fuera de rango físico" in str(exc_info.value)

    def test_humidity_out_of_range(self, weather_feed, sample_weatherapi_response):
        """Test que falla si humedad es inválida."""
        sample_weatherapi_response["current"]["humidity"] = 150.0

        with pytest.raises(SensorValidationError) as exc_info:
            weather_feed._validate_sensor_data(sample_weatherapi_response)

        assert "fuera de rango" in str(exc_info.value)


# =============================================================================
# TESTS DE PARSING
# =============================================================================

class TestParseObservation:
    """Tests para parsing de observaciones."""

    def test_parse_complete_response(self, weather_feed, sample_weatherapi_response):
        """Test parsing de respuesta completa."""
        received_at_ns = time.time_ns()

        observation = weather_feed._parse_observation(
            sample_weatherapi_response,
            received_at_ns
        )

        assert isinstance(observation, WeatherObservation)
        assert observation.temperature_c == 25.5
        assert observation.humidity_pct == 60
        assert observation.wind_speed_kmh == 16.9
        assert observation.pressure_hpa == 1013.0
        assert observation.precipitation_mm == 0.0
        assert observation.source == "WeatherAPI"
        assert observation.is_live is True
        assert observation.quality_score == 1.0  # Todos los campos presentes

    def test_parse_incomplete_response(self, weather_feed, sample_weatherapi_response):
        """Test parsing de respuesta incompleta."""
        # Remover algunos campos
        sample_weatherapi_response["current"]["humidity"] = None
        sample_weatherapi_response["current"]["wind_kph"] = None

        received_at_ns = time.time_ns()
        observation = weather_feed._parse_observation(
            sample_weatherapi_response,
            received_at_ns
        )

        assert observation.temperature_c == 25.5
        assert observation.humidity_pct is None
        assert observation.wind_speed_kmh is None
        assert observation.quality_score == 0.7  # Datos incompletos

    def test_provider_timestamp_extraction(self, weather_feed, sample_weatherapi_response):
        """Test extracción de timestamp del provider."""
        received_at_ns = time.time_ns()
        observation = weather_feed._parse_observation(
            sample_weatherapi_response,
            received_at_ns
        )

        # El timestamp del provider viene de last_updated_epoch (en segundos)
        expected_provider_ts = 1699999999 * 1_000_000_000
        assert observation.timestamp_ns == expected_provider_ts

        # El lag debe ser positivo (received_at > provider_timestamp)
        assert observation.received_at_ns >= observation.timestamp_ns


# =============================================================================
# TESTS DE URL BUILDING
# =============================================================================

class TestURLBuilding:
    """Tests para construcción de URLs."""

    def test_url_contains_required_params(self, weather_feed):
        """Test que la URL contiene parámetros requeridos."""
        url = weather_feed._build_request_url()

        assert "key=test_api_key" in url
        assert "q=40.7128,-74.0060" in url
        assert "aqi=no" in url
        assert "_t=" in url  # Cache-busting

    def test_url_changes_timestamp(self, weather_feed):
        """Test que el timestamp cambia entre llamadas."""
        url1 = weather_feed._build_request_url()
        time.sleep(0.001)  # Esperar 1ms
        url2 = weather_feed._build_request_url()

        # Los timestamps deben ser diferentes
        assert url1 != url2


# =============================================================================
# TESTS DE CREACIÓN
# =============================================================================

class TestFeedCreation:
    """Tests para creación del feed."""

    def test_create_with_factory_function(self, mock_config):
        """Test creación con factory function."""
        feed = create_weather_feed(config=mock_config, poll_interval=0.5)

        assert isinstance(feed, FastWeatherFeed)
        assert feed.poll_interval == 0.5
        assert feed.api_key == "test_api_key"

    def test_create_with_default_config(self):
        """Test creación con configuración por defecto."""
        # Esto fallará si no hay .env configurado
        # Skip en CI/CD
        if os.getenv("CI"):
            pytest.skip("No config in CI")

        feed = create_weather_feed()
        assert isinstance(feed, FastWeatherFeed)


# =============================================================================
# TESTS DE MÉTRICAS
# =============================================================================

class TestMetrics:
    """Tests para métricas del feed."""

    def test_metrics_summary(self, weather_feed):
        """Test resumen de métricas."""
        summary = weather_feed.get_metrics_summary()

        assert isinstance(summary, dict)
        assert "state" in summary
        assert "observations_received" in summary
        assert "avg_latency_ms" in summary
        assert summary["state"] == "STOPPED"  # Inicialmente detenido

    def test_last_observation_none_initially(self, weather_feed):
        """Test que no hay observación inicialmente."""
        assert weather_feed.last_observation is None


# =============================================================================
# TESTS DE ESTADO
# =============================================================================

class TestState:
    """Tests para estados del feed."""

    def test_initial_state(self, weather_feed):
        """Test estado inicial."""
        assert weather_feed.state == WeatherFeedState.STOPPED
        assert weather_feed.is_running is False

    def test_state_transitions(self, weather_feed):
        """Test transiciones de estado."""
        # Inicialmente STOPPED
        assert weather_feed._state == WeatherFeedState.STOPPED

        # Durante start() debería pasar a STARTING -> RUNNING
        # (no testeamos start() completo porque requiere event loop)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
