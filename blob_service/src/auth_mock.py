from fastapi import Header, HTTPException

def resolve_user(auth_token: str | None, x_user: str | None) -> str:
    """
    Determina el usuario activo a partir de las cabeceras de autenticación.

    Orden de prioridad:
      1. AuthToken (cabecera estándar)
      2. X-User (cabecera alternativa usada por el cliente CLI)

    Si no llega ninguna o está vacía → 401 Unauthorized.
    """
    user = (auth_token or x_user or "").strip()

    # Validación estricta
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized: missing user header")

    # Validación básica de formato 
    if " " in user or len(user) > 64:
        raise HTTPException(status_code=400, detail="Invalid username format")

    return user


def get_current_user(
    AuthToken: str | None = Header(default=None),
    X_User: str | None = Header(default=None)
):
    """
    Dependencia FastAPI para inyectar el usuario actual.
    Nota: FastAPI convierte automáticamente 'X-User' en 'X_User'.
    """
    return resolve_user(AuthToken, X_User)
