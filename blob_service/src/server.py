import argparse
import uvicorn
from .api import app
from .persistence import BlobPersistence
from .business import BlobService


def rebind(storage: str):
    """
    Reconfigura las instancias globales de la API con un almacenamiento distinto.
    Permite cambiar la ruta del directorio de datos dinámicamente.
    """
    from . import api as api_module
    api_module._persistence = BlobPersistence(storage)
    api_module._service = BlobService(api_module._persistence)
    print(f"📁 Almacenamiento configurado en: {storage}")


def main():
    """
    Punto de entrada del servidor del Blob Service.
    Permite configuración por línea de comandos:
      - puerto (-p)
      - host (-l)
      - directorio de datos (-s)
    """
    parser = argparse.ArgumentParser(description="Blob Service Server")
    parser.add_argument("-p", "--port", type=int, default=8000, help="Puerto de escucha (por defecto 8000)")
    parser.add_argument("-l", "--listening", default="127.0.0.1", help="Dirección de escucha (por defecto localhost)")
    parser.add_argument("-s", "--storage", default="./data", help="Directorio de almacenamiento de blobs")
    args = parser.parse_args()

    # Reasigna la ruta de almacenamiento antes de iniciar el servidor
    rebind(args.storage)

    print(f"🚀 Servidor iniciando en http://{args.listening}:{args.port}")
    print("🧱 API docs: http://127.0.0.1:8000/docs\n")

    uvicorn.run(
        "src.api:app",
        host=args.listening,
        port=args.port,
        reload=True,
        log_level="info"
    )


if __name__ == "__main__":
    main()
