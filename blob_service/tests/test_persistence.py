import os
import shutil
import pytest
from src.persistence import BlobPersistence
from src.models import BlobMeta


@pytest.fixture(scope="module")
def storage_path():
    """Prepara un directorio temporal de pruebas."""
    path = "testdata"
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)
    yield path
    shutil.rmtree(path, ignore_errors=True)


def test_create_and_read(storage_path):
    """Comprueba creación y lectura de datos y metadatos."""
    p = BlobPersistence(storage_path)
    meta = BlobMeta(id="abc", name="test.txt", owner="user1", readable_by=["user1"])
    p.create(meta, b"hola mundo")

    # Verifica contenido y metadatos
    content = p.read_content("abc")
    assert content == b"hola mundo"

    meta2 = p.read_meta("abc")
    assert meta2 is not None
    assert meta2.id == "abc"
    assert meta2.owner == "user1"
    assert meta2.name == "test.txt"


def test_update_content_and_meta(storage_path):
    """Verifica actualización de contenido y metadatos."""
    p = BlobPersistence(storage_path)
    meta = BlobMeta(id="def", name="a.txt", owner="alice", readable_by=["alice"])
    p.create(meta, b"v1")

    # Actualización de contenido
    assert p.update_content("def", b"v2") is True
    assert p.read_content("def") == b"v2"

    # Actualización de metadatos
    meta.name = "b.txt"
    assert p.update_meta(meta) is True
    meta2 = p.read_meta("def")
    assert meta2.name == "b.txt"


def test_read_pair_and_list(storage_path):
    """Comprueba lectura conjunta (meta+contenido) y listados."""
    p = BlobPersistence(storage_path)
    meta = BlobMeta(id="ghi", name="combo.txt", owner="user", readable_by=["user"])
    p.create(meta, b"123")

    pair = p.read_pair("ghi")
    assert pair is not None
    m, data = pair
    assert isinstance(m, BlobMeta)
    assert data == b"123"

    ids = p.list_ids()
    assert "ghi" in ids


def test_delete_success_and_failure(storage_path):
    """Comprueba eliminación de blobs y manejo de casos inexistentes."""
    p = BlobPersistence(storage_path)
    meta = BlobMeta(id="zzz", name="del.txt", owner="bob", readable_by=["bob"])
    p.create(meta, b"x")

    # Eliminación correcta
    assert p.delete("zzz") is True

    # Repetir debe fallar (ya no existe)
    assert p.delete("zzz") is False

    # Intentos de leer después deben devolver None
    assert p.read_content("zzz") is None
    assert p.read_meta("zzz") is None
    assert p.read_pair("zzz") is None


def test_update_and_read_nonexistent(storage_path):
    """Verifica comportamiento con blobs inexistentes."""
    p = BlobPersistence(storage_path)
    # Ningún archivo con id inexistente
    assert p.update_content("nonexistent", b"x") is False
    assert p.update_meta(BlobMeta(id="fake", name="x", owner="y", readable_by=[])) is False
    assert p.read_content("nonexistent") is None
    assert p.read_meta("nonexistent") is None
    assert p.read_pair("nonexistent") is None
