import os
import shutil
import pytest
from src.persistence import BlobPersistence
from src.business import BlobService, Forbidden, BlobNotFound


@pytest.fixture(scope="module")
def svc():
    """Crea un servicio limpio antes de las pruebas y lo elimina al final."""
    if os.path.exists("bizdata"):
        shutil.rmtree("bizdata")
    os.makedirs("bizdata", exist_ok=True)
    service = BlobService(BlobPersistence("bizdata"))
    yield service
    shutil.rmtree("bizdata", ignore_errors=True)


def test_create_and_read_blob(svc):
    """Verifica la creación y lectura básica de un blob."""
    blob_id = svc.create_blob("alice", "demo.txt", b"hello", readable_by=["alice"])
    data = svc.read_blob("alice", blob_id)
    assert data == b"hello"

    meta = svc.get_meta("alice", blob_id)
    assert meta.name == "demo.txt"
    assert meta.owner == "alice"
    assert "alice" in meta.readable_by


def test_update_blob(svc):
    """Comprueba que el propietario puede actualizar contenido."""
    blob_id = svc.create_blob("alice", "file.txt", b"v1")
    svc.update_blob("alice", blob_id, b"v2")
    assert svc.read_blob("alice", blob_id) == b"v2"

    # Intento de actualización por otro usuario
    with pytest.raises(Forbidden):
        svc.update_blob("bob", blob_id, b"x")


def test_add_and_remove_reader(svc):
    """Valida la gestión de permisos de lectura."""
    blob_id = svc.create_blob("alice", "secret.txt", b"shh")

    # Bob no puede leer al inicio
    with pytest.raises(Forbidden):
        svc.read_blob("bob", blob_id)

    # Alice concede permiso a Bob
    svc.add_reader("alice", blob_id, "bob")
    readers = svc.get_readable_by("alice", blob_id)
    assert "bob" in readers

    # Bob ahora puede leer
    assert svc.read_blob("bob", blob_id) == b"shh"

    # Alice le quita permiso
    svc.remove_reader("alice", blob_id, "bob")
    readers = svc.get_readable_by("alice", blob_id)
    assert "bob" not in readers

    # Bob vuelve a perder acceso
    with pytest.raises(Forbidden):
        svc.read_blob("bob", blob_id)


def test_set_name_and_list(svc):
    """Comprueba el renombrado y listado de blobs."""
    blob_id = svc.create_blob("alice", "oldname.txt", b"data")
    svc.set_name("alice", blob_id, "newname.txt")

    meta = svc.get_meta("alice", blob_id)
    assert meta.name == "newname.txt"

    blobs = svc.list_blobs("alice")
    assert any(b["id"] == blob_id for b in blobs)


def test_delete_blob(svc):
    """Verifica que solo el propietario puede eliminar el blob."""
    blob_id = svc.create_blob("alice", "todelete.txt", b"temp")

    # Bob no puede eliminar
    with pytest.raises(Forbidden):
        svc.delete_blob("bob", blob_id)

    # Alice sí puede eliminar
    svc.delete_blob("alice", blob_id)

    # Blob ya no existe
    with pytest.raises(BlobNotFound):
        svc.read_blob("alice", blob_id)


def test_not_found_errors(svc):
    """Comprueba excepciones cuando el blob no existe."""
    fake_id = "non-existent-id"

    with pytest.raises(BlobNotFound):
        svc.read_blob("alice", fake_id)

    with pytest.raises(BlobNotFound):
        svc.get_meta("alice", fake_id)

    with pytest.raises(BlobNotFound):
        svc.update_blob("alice", fake_id, b"whatever")

    with pytest.raises(BlobNotFound):
        svc.delete_blob("alice", fake_id)

    with pytest.raises(BlobNotFound):
        svc.add_reader("alice", fake_id, "bob")
