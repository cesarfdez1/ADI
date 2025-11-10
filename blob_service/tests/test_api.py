import os
import shutil
import pytest
from fastapi.testclient import TestClient
from src.api import app

# Creamos un cliente de test sobre la app FastAPI
client = TestClient(app)


@pytest.fixture(scope="module", autouse=True)
def setup_storage():
    """Limpia el almacenamiento antes y después de las pruebas."""
    if os.path.exists("./data"):
        shutil.rmtree("./data")
    os.makedirs("./data", exist_ok=True)
    yield
    shutil.rmtree("./data", ignore_errors=True)


def test_create_and_list_blobs():
    """Verifica la subida y listado de blobs."""
    files = {"file": ("demo.txt", b"contenido demo", "text/plain")}
    data = {"name": "demo.txt"}
    headers = {"X-User": "alice"}

    r = client.put("/blob", data=data, files=files, headers=headers)
    assert r.status_code == 200
    blob_id = r.json()["blob_id"]

    # Listar blobs accesibles
    r2 = client.get("/blob", headers=headers)
    assert r2.status_code == 200
    blobs = r2.json()
    assert any(b["id"] == blob_id for b in blobs)


def test_download_and_update_blob():
    """Verifica descarga y actualización del contenido."""
    headers = {"X-User": "alice"}

    # Crear uno nuevo
    files = {"file": ("file.txt", b"hola", "text/plain")}
    data = {"name": "file.txt"}
    r = client.put("/blob", data=data, files=files, headers=headers)
    blob_id = r.json()["blob_id"]

    # Descargar
    r2 = client.get(f"/blob/{blob_id}/data", headers=headers)
    assert r2.status_code == 200
    assert "hola" in r2.json()["data"]

    # Reemplazar
    new_file = {"file": ("file.txt", b"nuevo contenido", "text/plain")}
    r3 = client.post(f"/blob/{blob_id}/data", files=new_file, headers=headers)
    assert r3.status_code == 200

    # Comprobar nuevo contenido
    r4 = client.get(f"/blob/{blob_id}/data", headers=headers)
    assert "nuevo contenido" in r4.json()["data"]


def test_readers_management():
    """Comprueba gestión de permisos de lectura."""
    headers_alice = {"X-User": "alice"}
    headers_bob = {"X-User": "bob"}

    # Crear blob solo visible por Alice
    files = {"file": ("secret.txt", b"shh", "text/plain")}
    data = {"name": "secret.txt"}
    blob_id = client.put("/blob", data=data, files=files, headers=headers_alice).json()["blob_id"]

    # Bob no puede leer
    r1 = client.get(f"/blob/{blob_id}/data", headers=headers_bob)
    assert r1.status_code == 403

    # Alice añade a Bob
    r2 = client.post(f"/blob/{blob_id}/readable_by", data={"target_user": "bob"}, headers=headers_alice)
    assert r2.status_code == 200

    # Bob ya puede leer
    r3 = client.get(f"/blob/{blob_id}/data", headers=headers_bob)
    assert r3.status_code == 200
    assert "shh" in r3.json()["data"]

    # Alice elimina a Bob
    r4 = client.delete(f"/blob/{blob_id}/readable_by", params={"target_user": "bob"}, headers=headers_alice)
    assert r4.status_code == 200

    # Bob vuelve a estar bloqueado
    r5 = client.get(f"/blob/{blob_id}/data", headers=headers_bob)
    assert r5.status_code == 403


def test_rename_and_delete():
    """Comprueba renombrado y eliminación de blobs."""
    headers = {"X-User": "alice"}
    files = {"file": ("old.txt", b"x", "text/plain")}
    data = {"name": "old.txt"}
    blob_id = client.put("/blob", data=data, files=files, headers=headers).json()["blob_id"]

    # Renombrar
    r1 = client.patch(f"/blob/{blob_id}/name", data={"name": "new.txt"}, headers=headers)
    assert r1.status_code == 200
    r2 = client.get(f"/blob/{blob_id}/name", headers=headers)
    assert r2.status_code == 200
    assert r2.json()["name"] == "new.txt"

    # Eliminar
    r3 = client.delete(f"/blob/{blob_id}", headers=headers)
    assert r3.status_code == 200

    # Ya no debería existir
    r4 = client.get(f"/blob/{blob_id}/data", headers=headers)
    assert r4.status_code == 404


def test_error_cases():
    """Verifica manejo correcto de errores 404 y 403."""
    headers = {"X-User": "alice"}
    fake_id = "non-existent-id"

    r1 = client.get(f"/blob/{fake_id}/data", headers=headers)
    assert r1.status_code == 404

    r2 = client.delete(f"/blob/{fake_id}", headers=headers)
    assert r2.status_code == 404

    # Intentar crear sin cabecera X-User → 401
    files = {"file": ("f.txt", b"x", "text/plain")}
    data = {"name": "f.txt"}
    r3 = client.put("/blob", data=data, files=files)
    assert r3.status_code == 401
