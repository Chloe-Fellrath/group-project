from __future__ import annotations

from typing import Dict, Optional, Any, List

import os
import urllib.parse
import http.server
import secrets
import requests
import base64
import sys
import string
import random
import threading
import webbrowser
import time
import json
import csv
import hashlib

# ---- Sélection des exports ----
spotify_ID = "c62c55975a5f467a89a13bcb6fdcb76e"  # id client spotify developer
get_liked: bool = False      # True => génère liked_tracks.csv
get_playlist: bool = True   # True => génère <nom_playlist>.csv
playlist_ID: str = "4RFQsq8DrkJmNfR5l2BwfO"  # ID Spotify de la playlist à exporter

# ---- OAuth/Spotify ----
SPOTIFY_AUTH_URL = "https://accounts.spotify.com/authorize"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_API_BASE = "https://api.spotify.com/v1"

# Renseigne ton Client ID via variable d'env (recommandé)
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", spotify_ID)
# Redirect URI à ajouter dans le dashboard Spotify
REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI", "http://127.0.0.1:8721/callback")

# Scopes : user-library-read pour Liked Songs ; playlist-read-* pour playlists privées/collaboratives
SCOPE = "user-library-read playlist-read-private playlist-read-collaborative"

PORT = int(urllib.parse.urlparse(REDIRECT_URI).port or 8721)
CALLBACK_PATH = urllib.parse.urlparse(REDIRECT_URI).path or "/callback"

# ---- Fichiers dans le même dossier que main.py ----
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_LIKED_PATH = os.path.join(BASE_DIR, "liked_tracks.csv")
TOKEN_CACHE_PATH = os.path.join(BASE_DIR, "spotify_token_cache.json")

# ---- Cache local pour total_tracks d'un album ----
_album_total_cache: Dict[str, Optional[int]] = {}


# ------------------------ Utilitaires PKCE & OAuth ------------------------
def _b64url_no_pad(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def generate_pkce_pair() -> Dict[str, str]:
    verifier = _b64url_no_pad(secrets.token_bytes(64))  # 86+ chars
    challenge = _b64url_no_pad(hashlib.sha256(verifier.encode("utf-8")).digest())
    return {"verifier": verifier, "challenge": challenge}


def random_state(n: int = 24) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choice(alphabet) for _ in range(n))


def build_auth_url(client_id: str, redirect_uri: str, scope: str, code_challenge: str, state: str) -> str:
    params = {
        "client_id": client_id,
        "response_type": "code",
        "redirect_uri": redirect_uri,
        "code_challenge_method": "S256",
        "code_challenge": code_challenge,
        "scope": scope,
        "state": state,
        "show_dialog": "false",
    }
    return f"{SPOTIFY_AUTH_URL}?{urllib.parse.urlencode(params)}"


def exchange_code_for_token(code: str, verifier: str, client_id: str, redirect_uri: str, timeout: int = 15) -> Dict[str, Any]:
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "code_verifier": verifier,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    r = requests.post(SPOTIFY_TOKEN_URL, data=data, headers=headers, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    payload["_obtained_at"] = int(time.time())
    return payload


def refresh_access_token(refresh_token: str, client_id: str, timeout: int = 15) -> Dict[str, Any]:
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    r = requests.post(SPOTIFY_TOKEN_URL, data=data, headers=headers, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    payload["_obtained_at"] = int(time.time())
    if "refresh_token" not in payload:  # Spotify peut ne pas le renvoyer à chaque refresh
        payload["refresh_token"] = refresh_token
    return payload


def load_token_cache() -> Optional[Dict[str, Any]]:
    if os.path.exists(TOKEN_CACHE_PATH):
        try:
            with open(TOKEN_CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None


def save_token_cache(tok: Dict[str, Any]) -> None:
    with open(TOKEN_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(tok, f, ensure_ascii=False, indent=2)


def is_token_valid(tok: Dict[str, Any]) -> bool:
    if not tok:
        return False
    at = tok.get("_obtained_at")
    exp = tok.get("expires_in")
    if at is None or exp is None:
        return False
    # marge de 30s pour anticiper
    return (time.time() - at) < (exp - 30)


# ------------------------ Serveur local pour capturer le "code" ------------------------
class AuthServer(http.server.HTTPServer):
    auth_code: Optional[str] = None
    auth_state: Optional[str] = None
    auth_error: Optional[str] = None


class AuthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith(CALLBACK_PATH):
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            code = (qs.get("code") or [None])[0]
            state = (qs.get("state") or [None])[0]
            error = (qs.get("error") or [None])[0]
            self.server: AuthServer
            self.server.auth_code = code
            self.server.auth_state = state
            self.server.auth_error = error

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"<html><body><h1>OK</h1><p>Authentification terminee. Vous pouvez fermer cette fenetre.</p></body></html>")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        return  # silence


def get_user_token_via_pkce(client_id: str, redirect_uri: str, scope: str, timeout_total: int = 300) -> Dict[str, Any]:
    pair = generate_pkce_pair()
    state = random_state()
    url = build_auth_url(client_id, redirect_uri, scope, pair["challenge"], state)

    # Lance serveur HTTP local
    srv = AuthServer(("127.0.0.1", PORT), AuthHandler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()

    # Ouvre navigateur
    webbrowser.open(url)

    # Attend le callback
    t0 = time.time()
    while True:
        if srv.auth_error:
            srv.shutdown()
            raise RuntimeError(f"OAuth error: {srv.auth_error}")
        if srv.auth_code and srv.auth_state == state:
            code = srv.auth_code
            srv.shutdown()
            break
        if time.time() - t0 > timeout_total:
            srv.shutdown()
            raise TimeoutError("Dépassement de temps pendant l’authentification.")
        time.sleep(0.2)

    # Echange code -> tokens
    tokens = exchange_code_for_token(code, pair["verifier"], client_id, redirect_uri)
    save_token_cache(tokens)
    return tokens


# ------------------------ Appels API génériques avec gestion 401/429 ------------------------
def auth_header(access_token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}


def api_request(method: str, path: str, access_token: str, params: Optional[Dict[str, Any]] = None, json_body: Optional[Dict[str, Any]] = None, timeout: int = 15) -> requests.Response:
    url = f"{SPOTIFY_API_BASE}{path if path.startswith('/') else '/' + path}"
    r = requests.request(method.upper(), url, headers=auth_header(access_token), params=params, json=json_body, timeout=timeout)
    return r


def api_request_with_reauth(method: str, path: str, token_cache: Dict[str, Any], params: Optional[Dict[str, Any]] = None, json_body: Optional[Dict[str, Any]] = None, timeout: int = 15) -> Dict[str, Any]:
    # 1er essai
    r = api_request(method, path, token_cache["access_token"], params, json_body, timeout)
    if r.status_code == 401 and "refresh_token" in token_cache:
        # Rafraîchit et retente 1 fois
        new_tok = refresh_access_token(token_cache["refresh_token"], CLIENT_ID)
        token_cache.update(new_tok)
        save_token_cache(token_cache)
        r = api_request(method, path, token_cache["access_token"], params, json_body, timeout)

    # 429 => attend Retry-After si présent, puis retente 1 fois
    if r.status_code == 429:
        retry_after = int(r.headers.get("Retry-After", "1"))
        time.sleep(max(retry_after, 1))
        r = api_request(method, path, token_cache["access_token"], params, json_body, timeout)

    r.raise_for_status()
    try:
        return r.json()
    except ValueError:
        return {"raw": r.text}


# ------------------------ Helpers album_total_tracks (vérif via /albums/{id}) ------------------------
def get_album_total_tracks_exact(token_cache: Dict[str, Any], album_id: Optional[str], market: Optional[str] = None) -> Optional[int]:
    """
    Retourne total_tracks depuis l'endpoint ALBUM COMPLET (cache local),
    ou None si album_id manquant.
    """
    if not album_id:
        return None
    if album_id in _album_total_cache:
        return _album_total_cache[album_id]
    album = api_request_with_reauth("GET", f"/albums/{album_id}", token_cache, params={"market": market})
    total = album.get("total_tracks")
    _album_total_cache[album_id] = total
    return total


# ------------------------ Extraction "Liked Songs" ------------------------
def get_all_liked_tracks(token_cache: Dict[str, Any], market: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    offset = 0
    limit = max(1, min(int(limit), 50))  # API: 1..50

    while True:
        params = {"limit": limit, "offset": offset}
        if market:
            params["market"] = market
        page = api_request_with_reauth("GET", "/me/tracks", token_cache, params=params)

        items = page.get("items") or []
        if not items:
            break

        for it in items:
            tr = it.get("track") or {}
            if not tr or tr.get("type") != "track":
                continue
            album = tr.get("album") or {}
            artists = tr.get("artists") or []
            artist_names = "; ".join([a.get("name", "") for a in artists if a])
            artist_ids = "; ".join([a.get("id", "") for a in artists if a])

            album_id = album.get("id")
            # Vérifie/corrige total_tracks via /albums/{id}
            album_total_tracks = get_album_total_tracks_exact(token_cache, album_id, market)
            if album_total_tracks is None:
                album_total_tracks = album.get("total_tracks")

            rows.append({
                # --- Colonnes exportées ---
                "track_id": tr.get("id"),
                "track_name": tr.get("name"),
                "track_popularity": tr.get("popularity"),
                "duration_ms": tr.get("duration_ms"),
                "artist_names": artist_names,
                "artist_ids": artist_ids,
                "album_id": album_id,
                "album_name": album.get("name"),
                "album_release_date": album.get("release_date"),
                "album_total_tracks": album_total_tracks,
            })

        offset += len(items)
        if not page.get("next"):
            break

    return rows


def write_csv(rows: List[Dict[str, Any]], csv_path: str) -> None:
    # N'écrit QUE les colonnes demandées
    fieldnames = [
        "track_id",
        "track_name",
        "track_popularity",
        "duration_ms",
        "artist_names",
        "artist_ids",
        "album_id",
        "album_name",
        "album_release_date",
        "album_total_tracks",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k) for k in fieldnames})


# ------------------------ Playlist -> CSV par ID ------------------------
def write_playlist_csv_by_id(
    token_cache: Dict[str, Any],
    playlist_id: str,
    market: Optional[str] = None,
    limit: int = 50,
) -> str:
    if not playlist_id:
        raise ValueError("playlist_id est requis.")

    # (1) Métadonnées playlist pour le nom du fichier
    pl = api_request_with_reauth("GET", f"/playlists/{playlist_id}", token_cache, params={"market": market})
    pl_name = pl.get("name") or playlist_id

    # Nom de fichier "safe"
    safe = "".join(c if (c.isalnum() or c in (" ", ".", "_", "-")) else "_" for c in pl_name).strip()
    if not safe:
        safe = playlist_id
    out_path = os.path.join(BASE_DIR, f"{safe}.csv")

    # (2) Itération paginée des items
    rows: List[Dict[str, Any]] = []
    offset = 0
    limit = max(1, min(int(limit), 50))
    while True:
        params = {
            "market": market,
            "limit": limit,
            "offset": offset,
            "additional_types": "track",
        }
        page = api_request_with_reauth("GET", f"/playlists/{playlist_id}/tracks", token_cache, params=params)
        items = page.get("items") or []
        if not items:
            break

        for it in items:
            tr = it.get("track") or {}
            if not tr or tr.get("type") != "track":
                continue
            album = tr.get("album") or {}
            artists = tr.get("artists") or []
            artist_names = "; ".join([a.get("name", "") for a in artists if a])
            artist_ids = "; ".join([a.get("id", "") for a in artists if a])

            album_id = album.get("id")
            # Vérifie/corrige total_tracks via /albums/{id}
            album_total_tracks = get_album_total_tracks_exact(token_cache, album_id, market)
            if album_total_tracks is None:
                album_total_tracks = album.get("total_tracks")

            rows.append({
                # --- Colonnes exportées ---
                "track_id": tr.get("id"),
                "track_name": tr.get("name"),
                "track_popularity": tr.get("popularity"),
                "duration_ms": tr.get("duration_ms"),
                "artist_names": artist_names,
                "artist_ids": artist_ids,
                "album_id": album_id,
                "album_name": album.get("name"),
                "album_release_date": album.get("release_date"),
                "album_total_tracks": album_total_tracks,
            })

        offset += len(items)
        if not page.get("next"):
            break

    write_csv(rows, out_path)
    return out_path


# ------------------------ Token helper (créer/rafraîchir seulement si nécessaire) ------------------------
def ensure_user_token() -> Dict[str, Any]:
    if CLIENT_ID.startswith("XXX_"):
        print("Erreur: configure la variable d'environnement SPOTIFY_CLIENT_ID avec ton Client ID.", file=sys.stderr)
        sys.exit(1)

    tok = load_token_cache()
    # Si cache absent ou invalide -> tente refresh si possible, sinon PKCE
    if not tok or not is_token_valid(tok):
        if tok and "refresh_token" in tok:
            try:
                tok = refresh_access_token(tok["refresh_token"], CLIENT_ID)
                save_token_cache(tok)
            except Exception:
                tok = None
        if not tok:
            print("Ouverture du navigateur pour autoriser l'accès (scopes: %s)..." % SCOPE)
            tok = get_user_token_via_pkce(CLIENT_ID, REDIRECT_URI, SCOPE)
            save_token_cache(tok)
    return tok


# ------------------------ Main ------------------------
def main():
    if not (get_liked or get_playlist):
        print("Rien à faire : get_liked=False et get_playlist=False.")
        return

    # Crée/rafraîchit le cache OAuth UNIQUEMENT si nécessaire
    tok = ensure_user_token()

    # Playlist uniquement ?
    if get_playlist and not get_liked:
        if not playlist_ID.strip():
            print("Erreur: get_playlist=True mais playlist_ID est vide.", file=sys.stderr)
            sys.exit(1)
        out_path = write_playlist_csv_by_id(tok, playlist_ID.strip(), market=None, limit=50)
        print(f"CSV playlist écrit: {out_path}")
        return

    # Liked uniquement ?
    if get_liked and not get_playlist:
        rows = get_all_liked_tracks(tok, market=None, limit=50)
        write_csv(rows, CSV_LIKED_PATH)
        print(f"CSV liked écrit: {CSV_LIKED_PATH}")
        return

    # Les deux
    if get_liked and get_playlist:
        rows = get_all_liked_tracks(tok, market=None, limit=50)
        write_csv(rows, CSV_LIKED_PATH)
        print(f"CSV liked écrit: {CSV_LIKED_PATH}")

        if not playlist_ID.strip():
            print("Avertissement: get_playlist=True mais playlist_ID est vide. Skip playlist.", file=sys.stderr)
        else:
            out_path = write_playlist_csv_by_id(tok, playlist_ID.strip(), market=None, limit=50)
            print(f"CSV playlist écrit: {out_path}")


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        try:
            j = e.response.json()
        except Exception:
            j = {"error": e.response.text}
        print(f"[HTTPError] {e.response.status_code} {j}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"[Erreur] {e}", file=sys.stderr)
        sys.exit(1)