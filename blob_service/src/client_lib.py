import json
import requests
from typing import Optional, List

class BlobClient:
    """
    Cliente HTTP de alto nivel para el servicio BlobService.

    Este cliente encapsula todas las operaciones expuestas por la API REST:
      - Crear, listar, descargar, reemplazar y eliminar blobs
      - Gestionar permisos de lectura (lectores)
    """

    def __init__(self, base_url: str, auth_token: str):
        self.base_url = base_url.rstrip("/")
        self.headers = {"AuthToken": auth_token}

    # -------------------------------------------------------
    # 📤 Crear un nuevo blob
    # -------------------------------------------------------
    def create(self, name: str, path: str, readable_by: Optional[List[str]] = None) -> str:
        """Sube un nuevo blob al servidor."""
        try:
            with open(path, "rb") as f:
                files = {"file": (name, f, "application/octet-stream")}
                data = {"name": name}
                if readable_by:
                    data["readable_by"] = json.dumps(readable_by)

                r = requests.put(f"{self.base_url}/blob", data=data, files=files, headers=self.headers)
                r.raise_for_status()
                return r.json()["blob_id"]
        except FileNotFoundError:
            raise Exception(f"El archivo local '{path}' no existe.")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error de conexión al crear blob: {e}")

    # -------------------------------------------------------
    # 📦 Listar blobs
    # -------------------------------------------------------
    def list(self) -> List[dict]:
        """Devuelve la lista de blobs accesibles por el usuario."""
        try:
            r = requests.get(f"{self.base_url}/blob", headers=self.headers)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error al listar blobs: {e}")

    # -------------------------------------------------------
    # 📥 Descargar contenido de un blob
    # -------------------------------------------------------
    def download_text(self, blob_id: str) -> str:
        """Descarga el contenido (texto) de un blob existente."""
        try:
            r = requests.get(f"{self.base_url}/blob/{blob_id}/data", headers=self.headers)
            r.raise_for_status()
            return r.json().get("data", "")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error al descargar blob {blob_id}: {e}")

    # -------------------------------------------------------
    # ✏️ Reemplazar contenido
    # -------------------------------------------------------
    def replace(self, blob_id: str, path: str):
        """Reemplaza el contenido binario del blob."""
        try:
            with open(path, "rb") as f:
                files = {"file": (path, f, "application/octet-stream")}
                r = requests.post(f"{self.base_url}/blob/{blob_id}/data", files=files, headers=self.headers)
                r.raise_for_status()
        except FileNotFoundError:
            raise Exception(f"El archivo '{path}' no existe.")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error al reemplazar blob {blob_id}: {e}")

    # -------------------------------------------------------
    # ❌ Eliminar blob
    # -------------------------------------------------------
    def delete(self, blob_id: str):
        """Elimina un blob (solo propietario)."""
        try:
            r = requests.delete(f"{self.base_url}/blob/{blob_id}", headers=self.headers)
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error al eliminar blob {blob_id}: {e}")

    # -------------------------------------------------------
    # 👥 Gestión de lectores
    # -------------------------------------------------------
    def get_readers(self, blob_id: str) -> List[str]:
        """Obtiene la lista de usuarios con acceso de lectura."""
        try:
            r = requests.get(f"{self.base_url}/blob/{blob_id}/readable_by", headers=self.headers)
            r.raise_for_status()
            return r.json().get("readable_by", [])
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error al obtener lectores de {blob_id}: {e}")

    def add_reader(self, blob_id: str, user: str):
        """Añade un lector (usuario) a un blob existente."""
        try:
            r = requests.post(
                f"{self.base_url}/blob/{blob_id}/readable_by",
                data={"target_user": user},
                headers=self.headers,
            )
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error al añadir lector '{user}' a {blob_id}: {e}")

    def remove_reader(self, blob_id: str, user: str):
        """Elimina un lector (usuario) de un blob existente."""
        try:
            r = requests.delete(
                f"{self.base_url}/blob/{blob_id}/readable_by",
                data={"target_user": user},
                headers=self.headers,
            )
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error al eliminar lector '{user}' de {blob_id}: {e}")
