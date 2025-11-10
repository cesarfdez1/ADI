from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends, Query, Body
from typing import List, Optional
from .persistence import BlobPersistence
from .business import BlobService, BlobNotFound, Forbidden
from .auth_mock import get_current_user
import json

app = FastAPI(title="Blob Service API", description="Servicio REST de blobs con control de permisos")

# -------------------------------
# Inyección de dependencias
# -------------------------------
_persistence = BlobPersistence("./data")
_service = BlobService(_persistence)

# ---------------------------------------------------------
# CREAR BLOB
# ---------------------------------------------------------
@app.put("/blob", summary="Crea un nuevo blob")
async def create_blob(
    name: str = Form(...),
    readable_by: Optional[str] = Form(None),  # lista JSON opcional
    file: UploadFile = File(...),
    user: str = Depends(get_current_user),
):
    """Crea un blob nuevo con nombre, fichero binario y lista opcional de usuarios con permiso de lectura."""
    raw = await file.read()
    readers: List[str] = []
    if readable_by:
        try:
            readers = json.loads(readable_by)
            if not isinstance(readers, list):
                raise ValueError()
        except Exception:
            raise HTTPException(400, "El parámetro 'readable_by' debe ser una lista JSON válida.")
    blob_id = _service.create_blob(user=user, name=name, content=raw, readable_by=readers)
    return {"blob_id": blob_id, "owner": user, "readable_by": readers}

# ---------------------------------------------------------
# LISTAR BLOBS
# ---------------------------------------------------------
@app.get("/blob", summary="Lista blobs accesibles por el usuario actual")
def list_blobs(user: str = Depends(get_current_user)):
    return _service.list_blobs(user)

# ---------------------------------------------------------
# METADATOS
# ---------------------------------------------------------
@app.get("/blob/{blob_id}", summary="Obtiene los metadatos de un blob")
def get_meta(blob_id: str, user: str = Depends(get_current_user)):
    try:
        m = _service.get_meta(user, blob_id)
        return {"id": m.id, "name": m.name, "owner": m.owner, "readable_by": m.readable_by}
    except BlobNotFound:
        raise HTTPException(404, "Blob no encontrado")
    except Forbidden:
        raise HTTPException(403, "Acceso denegado")

# ---------------------------------------------------------
# DESCARGAR CONTENIDO
# ---------------------------------------------------------
@app.get("/blob/{blob_id}/data", summary="Descarga el contenido del blob")
def download(blob_id: str, user: str = Depends(get_current_user)):
    try:
        content = _service.read_blob(user, blob_id)
        return {"blob_id": blob_id, "data": content.decode("utf-8", errors="ignore")}
    except BlobNotFound:
        raise HTTPException(404, "Blob no encontrado")
    except Forbidden:
        raise HTTPException(403, "Acceso denegado")

# ---------------------------------------------------------
# REEMPLAZAR CONTENIDO (POST)
# ---------------------------------------------------------
@app.post("/blob/{blob_id}/data", summary="Reemplaza el contenido de un blob existente")
async def replace_data(blob_id: str, file: UploadFile = File(...), user: str = Depends(get_current_user)):
    try:
        raw = await file.read()
        _service.update_blob(user, blob_id, raw)  # <--- usa update_blob (coincide con tests)
        return {"updated": blob_id}
    except BlobNotFound:
        raise HTTPException(404, "Blob no encontrado")
    except Forbidden:
        raise HTTPException(403, "Acceso denegado")

# ---------------------------------------------------------
# OBTENER / CAMBIAR NOMBRE
# ---------------------------------------------------------
@app.get("/blob/{blob_id}/name", summary="Obtiene el nombre de un blob")
def get_name(blob_id: str, user: str = Depends(get_current_user)):
    try:
        return {"name": _service.get_meta(user, blob_id).name}
    except BlobNotFound:
        raise HTTPException(404, "Blob no encontrado")
    except Forbidden:
        raise HTTPException(403, "Acceso denegado")

@app.patch("/blob/{blob_id}/name", summary="Cambia el nombre de un blob")
def set_name(blob_id: str, name: str = Form(...), user: str = Depends(get_current_user)):
    try:
        _service.set_name(user, blob_id, name)
        return {"updated": blob_id, "name": name}
    except BlobNotFound:
        raise HTTPException(404, "Blob no encontrado")
    except Forbidden:
        raise HTTPException(403, "Acceso denegado")

# ---------------------------------------------------------
# LECTORES
# ---------------------------------------------------------
@app.get("/blob/{blob_id}/readable_by", summary="Obtiene los usuarios con acceso de lectura")
def get_readers(blob_id: str, user: str = Depends(get_current_user)):
    try:
        return {"readable_by": _service.get_readable_by(user, blob_id)}
    except BlobNotFound:
        raise HTTPException(404, "Blob no encontrado")
    except Forbidden:
        raise HTTPException(403, "Acceso denegado")

@app.put("/blob/{blob_id}/readable_by", summary="Reemplaza la lista completa de lectores")
def set_readable_by(blob_id: str, readable_by: str = Form(...), user: str = Depends(get_current_user)):
    try:
        readers = json.loads(readable_by)
        if not isinstance(readers, list):
            raise ValueError()
        _service.set_readable_by(user, blob_id, readers)
        return {"updated": blob_id, "readable_by": readers}
    except ValueError:
        raise HTTPException(400, "El campo readable_by debe ser una lista JSON válida")
    except BlobNotFound:
        raise HTTPException(404, "Blob no encontrado")
    except Forbidden:
        raise HTTPException(403, "Acceso denegado")

@app.post("/blob/{blob_id}/readable_by", summary="Añade un lector al blob")
def add_reader(blob_id: str, target_user: str = Form(...), user: str = Depends(get_current_user)):
    try:
        _service.add_reader(user, blob_id, target_user)
        return {"added": target_user}
    except BlobNotFound:
        raise HTTPException(404, "Blob no encontrado")
    except Forbidden:
        raise HTTPException(403, "Acceso denegado")

@app.delete("/blob/{blob_id}/readable_by", summary="Elimina un lector del blob")
async def remove_reader(
    blob_id: str,
    target_user: Optional[str] = Query(None),
    user: str = Depends(get_current_user),
    body: Optional[dict] = Body(None)
):
    """
    Permite eliminar lectores, compatible con el test que envía data en DELETE.
    Admite tanto query (?target_user=...) como body {"target_user": "..."}.
    """
    # Soporte para cuando el test usa data={'target_user': 'bob'}
    if not target_user and body and "target_user" in body:
        target_user = body["target_user"]

    if not target_user:
        raise HTTPException(400, "target_user requerido")

    try:
        _service.remove_reader(user, blob_id, target_user)
        return {"removed": target_user}
    except BlobNotFound:
        raise HTTPException(404, "Not Found")
    except Forbidden:
        raise HTTPException(403, "Forbidden")

# ---------------------------------------------------------
# ELIMINAR BLOB
# ---------------------------------------------------------
@app.delete("/blob/{blob_id}", summary="Elimina un blob (solo propietario)")
def delete(blob_id: str, user: str = Depends(get_current_user)):
    try:
        _service.delete_blob(user, blob_id)
        return {"deleted": blob_id}
    except BlobNotFound:
        raise HTTPException(404, "Blob no encontrado")
    except Forbidden:
        raise HTTPException(403, "Acceso denegado")
