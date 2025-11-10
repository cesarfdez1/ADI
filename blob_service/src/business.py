import uuid
from typing import List, Optional
from .persistence import BlobPersistence
from .models import BlobMeta


# -------------------------------
# Excepciones específicas
# -------------------------------
class BlobNotFound(Exception):
    def __init__(self, blob_id: str):
        super().__init__(f"Blob not found: {blob_id}")


class Forbidden(Exception):
    def __init__(self, blob_id: str, user: str):
        super().__init__(f"Forbidden access to blob '{blob_id}' for user '{user}'")


# -------------------------------
# Capa de negocio
# -------------------------------
class BlobService:
    """
    Capa intermedia de negocio:
    - Controla permisos de propietario y lectores
    - Orquesta la persistencia
    - Define operaciones según el enunciado
    """

    def __init__(self, persistence: BlobPersistence):
        self.p = persistence

    # ---------------------------------------
    # CRUD principal
    # ---------------------------------------
    def create_blob(
        self,
        user: str,
        name: str,
        content: bytes,
        readable_by: Optional[List[str]] = None,
        extra=None,
    ) -> str:
        blob_id = str(uuid.uuid4())
        readers = list(set((readable_by or []) + [user]))
        meta = BlobMeta(
            id=blob_id, name=name, owner=user, readable_by=readers, extra=extra or {}
        )
        self.p.create(meta, content)
        return blob_id
    
    def update_blob(self, user: str, blob_id: str, new_content: bytes) -> None:
        meta = self.p.read_meta(blob_id)
        if not meta:
            raise BlobNotFound(blob_id)
        if user != meta.owner:
            raise Forbidden(blob_id, user)
        if not self.p.update_content(blob_id, new_content):
            raise BlobNotFound(blob_id)
        
    def read_blob(self, user: str, blob_id: str) -> bytes:
        meta = self._check_access(user, blob_id, read=True)
        data = self.p.read_content(blob_id)
        if data is None:
            raise BlobNotFound(blob_id)
        return data

    def delete_blob(self, user: str, blob_id: str) -> None:
        meta = self._check_access(user, blob_id, owner_only=True)
        if not self.p.delete(blob_id):
            raise BlobNotFound(blob_id)

    def list_blobs(self, user: str):
        """Lista solo los blobs que el usuario puede leer."""
        result = []
        for blob_id in self.p.list_ids():
            meta = self.p.read_meta(blob_id)
            if meta and user in meta.readable_by:
                result.append(
                    {"id": meta.id, "name": meta.name, "owner": meta.owner}
                )
        return result

    # ---------------------------------------
    # Modificaciones parciales (PATCH / PUT)
    # ---------------------------------------
    def modify_blob(self, user: str, blob_id: str, new_name: Optional[str] = None):
        """Permite modificar el nombre de un blob existente."""
        meta = self._check_access(user, blob_id, owner_only=True)
        if new_name:
            meta.name = new_name
            self.p.update_meta(meta)

    def replace_blob(self, user: str, blob_id: str, new_content: bytes):
        """Reemplaza el contenido del blob (solo propietario)."""
        self._check_access(user, blob_id, owner_only=True)
        if not self.p.update_content(blob_id, new_content):
            raise BlobNotFound(blob_id)

    def set_readable_by(self, user: str, blob_id: str, new_list: List[str]):
        """Reemplaza la lista completa de usuarios con acceso de lectura."""
        meta = self._check_access(user, blob_id, owner_only=True)
        if user not in new_list:
            new_list.append(user)
        meta.readable_by = list(set(new_list))
        self.p.update_meta(meta)

    # ---------------------------------------
    # Lectura de metadatos y permisos
    # ---------------------------------------
    def get_meta(self, user: str, blob_id: str) -> BlobMeta:
        return self._check_access(user, blob_id, read=True)

    def get_readable_by(self, user: str, blob_id: str) -> List[str]:
        meta = self._check_access(user, blob_id, read=True)
        return meta.readable_by

    def set_name(self, user: str, blob_id: str, new_name: str):
        meta = self._check_access(user, blob_id, owner_only=True)
        meta.name = new_name
        self.p.update_meta(meta)

    # ---------------------------------------
    # Gestión de lectores
    # ---------------------------------------
    def add_reader(self, user: str, blob_id: str, target_user: str):
        meta = self._check_access(user, blob_id, owner_only=True)
        if target_user not in meta.readable_by:
            meta.readable_by.append(target_user)
            self.p.update_meta(meta)

    def remove_reader(self, user: str, blob_id: str, target_user: str):
        meta = self._check_access(user, blob_id, owner_only=True)
        if target_user in meta.readable_by and target_user != meta.owner:
            meta.readable_by.remove(target_user)
            self.p.update_meta(meta)

    # ---------------------------------------
    # Método privado de verificación de permisos
    # ---------------------------------------
    def _check_access(
        self,
        user: str,
        blob_id: str,
        read: bool = False,
        owner_only: bool = False,
    ) -> BlobMeta:
        """Verifica existencia y permisos según operación."""
        meta = self.p.read_meta(blob_id)
        if not meta:
            raise BlobNotFound(blob_id)

        if owner_only and user != meta.owner:
            raise Forbidden(blob_id, user)

        if read and user not in meta.readable_by:
            raise Forbidden(blob_id, user)

        return meta
