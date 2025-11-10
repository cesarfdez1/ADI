import argparse
from .client_lib import BlobClient

def main():
    """
    Cliente CLI para interactuar con el Blob Service.
    Permite subir, listar, descargar, reemplazar y eliminar blobs,
    así como gestionar los permisos de lectura.
    """
    parser = argparse.ArgumentParser(
        prog="blob-cli",
        description="Cliente CLI para Blob Service (API REST con FastAPI)."
    )
    parser.add_argument("--url", default="http://127.0.0.1:8000", help="URL base del servicio (por defecto localhost:8000)")
    parser.add_argument("--user", default="cesar", help="Usuario o token de autenticación (AuthToken)")

    sub = parser.add_subparsers(dest="cmd", required=True)

    # --- Crear blob ---
    upload = sub.add_parser("upload", help="Sube un nuevo blob al servidor")
    upload.add_argument("name", help="Nombre lógico del blob")
    upload.add_argument("path", help="Ruta del archivo local a subir")

    # --- Listar blobs ---
    sub.add_parser("list", help="Lista blobs accesibles por el usuario")

    # --- Descargar blob ---
    download = sub.add_parser("download", help="Descarga el contenido de un blob")
    download.add_argument("blob_id", help="ID del blob a descargar")

    # --- Reemplazar contenido ---
    replace = sub.add_parser("replace", help="Reemplaza el contenido de un blob existente")
    replace.add_argument("blob_id", help="ID del blob a reemplazar")
    replace.add_argument("path", help="Ruta del nuevo archivo")

    # --- Eliminar blob ---
    delete = sub.add_parser("delete", help="Elimina un blob del servidor")
    delete.add_argument("blob_id", help="ID del blob a eliminar")

    # --- Lectores ---
    readers = sub.add_parser("readers", help="Lista los usuarios con acceso de lectura a un blob")
    readers.add_argument("blob_id", help="ID del blob")

    add_reader = sub.add_parser("add-reader", help="Añade un lector al blob")
    add_reader.add_argument("blob_id", help="ID del blob")
    add_reader.add_argument("target_user", help="Usuario que se añadirá a readable_by")

    remove_reader = sub.add_parser("remove-reader", help="Elimina un lector del blob")
    remove_reader.add_argument("blob_id", help="ID del blob")
    remove_reader.add_argument("target_user", help="Usuario que se eliminará de readable_by")

    args = parser.parse_args()
    cli = BlobClient(args.url, args.user)

    try:
        # -------------------
        # Ejecución de comandos
        # -------------------
        if args.cmd == "upload":
            blob_id = cli.create(args.name, args.path)
            print(f"✅ Blob creado correctamente: {blob_id}")

        elif args.cmd == "list":
            blobs = cli.list()
            if not blobs:
                print("ℹ️ No hay blobs disponibles.")
            else:
                for b in blobs:
                    print(f"📦 {b['id']} - {b['name']} (propietario: {b['owner']})")

        elif args.cmd == "download":
            text = cli.download_text(args.blob_id)
            print(f"📄 Contenido del blob {args.blob_id}:\n{text}")

        elif args.cmd == "replace":
            cli.replace(args.blob_id, args.path)
            print(f"✅ Contenido reemplazado: {args.blob_id}")

        elif args.cmd == "delete":
            cli.delete(args.blob_id)
            print(f"🗑️ Blob eliminado: {args.blob_id}")

        elif args.cmd == "readers":
            readers = cli.get_readers(args.blob_id)
            print(f"👥 Lectores de {args.blob_id}: {', '.join(readers)}")

        elif args.cmd == "add-reader":
            cli.add_reader(args.blob_id, args.target_user)
            print(f"✅ Añadido lector '{args.target_user}' a {args.blob_id}")

        elif args.cmd == "remove-reader":
            cli.remove_reader(args.blob_id, args.target_user)
            print(f"✅ Eliminado lector '{args.target_user}' de {args.blob_id}")

    except Exception as ex:
        print(f"❌ Error: {ex}")

if __name__ == "__main__":
    main()
