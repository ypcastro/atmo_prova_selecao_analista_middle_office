"""conftest.py — configura sys.path para importar src/app nos testes."""
import sys
from pathlib import Path

# Garante que src/ esteja no path (para `from app.xxx import ...`)
src_path = str(Path(__file__).parent / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)
