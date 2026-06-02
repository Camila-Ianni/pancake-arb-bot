#!/bin/bash

echo "========================================="
echo " Polymarket CLOB SDK Dependency Installer "
echo "========================================="

# Check Python version
PYTHON_VERSION=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")')
echo "🔍 Versión de Python detectada: $PYTHON_VERSION"

# Minimal version comparison
python -c "
import sys
if sys.version_info < (3, 9, 10):
    print('❌ ERROR: py-clob-client requiere Python >= 3.9.10.')
    sys.exit(1)
"
if [ $? -ne 0 ]; then
    echo "💡 Por favor, instala Python 3.9.10 o superior y recrea tu entorno virtual (venv)."
    exit 1
fi

echo "✅ Versión de Python compatible."

echo "📦 Instalando py_clob_client y dependencias..."
pip install py_clob_client python-dotenv

if [ $? -eq 0 ]; then
    echo "✅ py_clob_client instalado correctamente."
    echo "🚀 Puedes ejecutar la prueba con: python test_connection.py"
else
    echo "❌ ERROR: Hubo un fallo durante la instalación de pip."
    exit 1
fi
