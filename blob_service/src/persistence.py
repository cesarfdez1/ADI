import os
import json
from typing import Optional, List, Tuple
from .models import BlobMeta


class BlobPersistence:
    """
    Persistencia en filesystem:
      - contenido: {storage}/{blob_id}.data
      - metadatos: {storage}/{blob_id}.json

    Cada blob tiene:
      - id: identificador único (UUID)
      - name: nombre asignado
      - owner: usuario propietario
      - readable_by: lista de usuarios con permiso de lectura
      - extra: metadatos adicionales
    """

    def __init__(self, storage: str):
        self.storage = storage
        os.makedirs(storage, exist_ok=True)

    # ----------------------------
    # Rutas internas
    # ----------------------------
    def _data(self, blob_id: str) -> str:
        return os.path.join(self.storage, f"{blob_id}.data")

    def _meta(self, blob_id: str) -> str:
        return os.path.join(self.storage, f"{blob_id}.json")

    # ----------------------------
    # Creación
    # ----------------------------
    def create(self, meta: BlobMeta, content: bytes) -> None:
        """Crea los archivos .data y .json asociados al blob."""
        with open(self._data(meta.id), "wb") as f:
            f.write(content)
        with open(self._meta(meta.id), "w", encoding="utf-8") as f:
            json.dump(meta.__dict__, f, ensure_ascii=False, indent=2)

    # ----------------------------
    # Lectura
    # ----------------------------
    def read_content(self, blob_id: str) -> Optional[bytes]:
        """Devuelve el contenido binario de un blob."""
        try:
            with open(self._data(blob_id), "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def read_meta(self, blob_id: str) -> Optional[BlobMeta]:
        """Devuelve los metadatos del blob (BlobMeta)."""
        try:
            with open(self._meta(blob_id), "r", encoding="utf-8") as f:
                data = json.load(f)
            return BlobMeta(**data)
        except FileNotFoundError:
            return None

    def read_pair(self, blob_id: str) -> Optional[Tuple[BlobMeta, bytes]]:
        """Devuelve (meta, contenido) si ambos existen."""
        meta = self.read_meta(blob_id)
        if not meta:
            return None
        content = self.read_content(blob_id)
        if content is None:
            return None
        return meta, content

    # ----------------------------
    # Actualización
    # ----------------------------
    def update_content(self, blob_id: str, new_content: bytes) -> bool:
        """Reemplaza el contenido binario de un blob existente."""
        if not os.path.exists(self._data(blob_id)):
            return False
        with open(self._data(blob_id), "wb") as f:
            f.write(new_content)
        return True

    def update_meta(self, meta: BlobMeta) -> bool:
        """Reemplaza el archivo JSON de metadatos completo."""
        if not os.path.exists(self._meta(meta.id)):
            return False
        with open(self._meta(meta.id), "w", encoding="utf-8") as f:
            json.dump(meta.__dict__, f, ensure_ascii=False, indent=2)
        return True

    def patch_meta(self, blob_id: str, **fields) -> bool:
        """
        Modifica parcialmente los metadatos de un blob.
        Ejemplo:
            patch_meta(id, name="nuevo", readable_by=["a","b"])
        """
        meta = self.read_meta(blob_id)
        if not meta:
            return False
        for k, v in fields.items():
            if hasattr(meta, k):
                setattr(meta, k, v)
        return self.update_meta(meta)

    # ----------------------------
    # Eliminación
    # ----------------------------
    def delete(self, blob_id: str) -> bool:
        """Elimina tanto el .data como el .json de un blob."""
        dp, mp = self._data(blob_id), self._meta(blob_id)
        if not os.path.exists(dp) or not os.path.exists(mp):
            return False
        os.remove(dp)
        os.remove(mp)
        return True

    # ----------------------------
    # Utilidades
    # ----------------------------
    def list_ids(self) -> List[str]:
        """Lista los IDs de todos los blobs existentes."""
        return [n[:-5] for n in os.listdir(self.storage) if n.endswith(".json")]

    def exists(self, blob_id: str) -> bool:
        """Comprueba si un blob existe en almacenamiento."""
        return os.path.exists(self._meta(blob_id)) and os.path.exists(self._data(blob_id))
