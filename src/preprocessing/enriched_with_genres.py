from __future__ import annotations
from typing import Dict, Any, Optional, List
import base64
import hashlib
import http.server
import json
import os
import random
import secrets
import string
import sys
import threading
import time
import urllib.parse
import webbrowser
import csv
import pandas as pd

import requests  # pip install requests

# ===================== CONFIG =====================
spotify_ID = "c62c55975a5f467a89a13bcb6fdcb76e"  # Mets ton CLIENT_ID ici si tu n'utilises pas la variable d'env
INPUT_CSV = "all_playlists_combined.csv"                    # CSV source à enrichir
OUTPUT_CSV = "all_playlists_combined.csv-with_genres.csv"       # CSV de sortie

# Noms de colonnes attendues dans le CSV d'entrée
ARTIST_IDS_FIELD = "artist_ids"
ALBUM_ID_FIELD   = "album_id"
GENRE_FIELD      = "genre"       # sera ajouté/écrasé

# Parsing des artist_ids en entrée et des genres en sortie
SEP_IN  = ";"     # séparateur entre plusieurs artist_ids dans le CSV source
SEP_OUT = "; "    # séparateur pour la liste finale des genres

# ===================== Spotify OAuth / API =====================
SPOTIFY_AUTH_URL = "https://accounts.spotify.com/authorize"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_API_BASE = "https://api.spotify.com/v1"

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", spotify_ID)
REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI", "http://127.0.0.1:8721/callback")

# Pas de scopes requis pour /artists et /albums (publics)
SCOPE = ""

PORT = int(urllib.parse.urlparse(REDIRECT_URI).port or 8721)
CALLBACK_PATH = urllib.parse.urlparse(REDIRECT_URI).path or "/callback"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TOKEN_CACHE_PATH = os.path.join(BASE_DIR, "spotify_token_cache.json")

# ===================== Caches =====================
_artist_genres_cache: Dict[str, List[str]] = {}   # artist_id -> [genres]
_album_artists_cache: Dict[str, List[str]] = {}   # album_id  -> [artist_ids]

# ===================== PKCE & OAuth =====================
def _b64url_no_pad(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")

def generate_pkce_pair() -> Dict[str, str]:
    verifier = _b64url_no_pad(secrets.token_bytes(64))
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
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token, "client_id": client_id}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    r = requests.post(SPOTIFY_TOKEN_URL, data=data, headers=headers, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    payload["_obtained_at"] = int(time.time())
    if "refresh_token" not in payload:
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
    return (time.time() - at) < (exp - 30)

class AuthServer(http.server.HTTPServer):
    auth_code: Optional[str] = None
    auth_state: Optional[str] = None
    auth_error: Optional[str] = None

class AuthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith(CALLBACK_PATH):
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            self.server: AuthServer
            self.server.auth_code = (qs.get("code") or [None])[0]
            self.server.auth_state = (qs.get("state") or [None])[0]
            self.server.auth_error = (qs.get("error") or [None])[0]

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"<html><body><h1>OK</h1><p>Authentification terminee. Vous pouvez fermer cette fenetre.</p></body></html>")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *_):
        return

def get_user_token_via_pkce(client_id: str, redirect_uri: str, scope: str, timeout_total: int = 300) -> Dict[str, Any]:
    pair = generate_pkce_pair()
    state = random_state()
    url = build_auth_url(client_id, redirect_uri, scope, pair["challenge"], state)

    srv = AuthServer(("127.0.0.1", PORT), AuthHandler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    webbrowser.open(url)

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

    tokens = exchange_code_for_token(code, pair["verifier"], client_id, redirect_uri)
    save_token_cache(tokens)
    return tokens

# ===================== API helpers =====================
def auth_header(access_token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

def api_request(method: str, path: str, access_token: str,
                params: Optional[Dict[str, Any]] = None,
                json_body: Optional[Dict[str, Any]] = None,
                timeout: int = 15) -> requests.Response:
    url = f"{SPOTIFY_API_BASE}{path if path.startswith('/') else '/' + path}"
    r = requests.request(method.upper(), url, headers=auth_header(access_token),
                         params=params, json=json_body, timeout=timeout)
    return r

def api_request_with_reauth(method: str, path: str, token_cache: Dict[str, Any],
                            params: Optional[Dict[str, Any]] = None,
                            json_body: Optional[Dict[str, Any]] = None,
                            timeout: int = 15) -> Dict[str, Any]:
    r = api_request(method, path, token_cache["access_token"], params, json_body, timeout)
    if r.status_code == 401 and "refresh_token" in token_cache:
        new_tok = refresh_access_token(token_cache["refresh_token"], CLIENT_ID)
        token_cache.update(new_tok)
        save_token_cache(token_cache)
        r = api_request(method, path, token_cache["access_token"], params, json_body, timeout)

    if r.status_code == 429:
        retry_after = int(r.headers.get("Retry-After", "1"))
        time.sleep(max(retry_after, 1))
        r = api_request(method, path, token_cache["access_token"], params, json_body, timeout)

    r.raise_for_status()
    try:
        return r.json()
    except ValueError:
        return {"raw": r.text}

# ===================== Genres & Albums helpers =====================
def _unique(seq: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in seq:
        x = (x or "").strip()
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out

def get_album_artist_ids(token_cache: Dict[str, Any], album_id: Optional[str]) -> List[str]:
    if not album_id:
        return []
    if album_id in _album_artists_cache:
        return _album_artists_cache[album_id]
    album = api_request_with_reauth("GET", f"/albums/{album_id}", token_cache)
    ids = [a.get("id") for a in (album.get("artists") or []) if a and a.get("id")]
    _album_artists_cache[album_id] = _unique(ids)
    return _album_artists_cache[album_id]

def get_genres_for_artist_ids(token_cache: Dict[str, Any], artist_ids: List[str]) -> List[str]:
    """Union des genres de tous les artistes (batch /artists + fallback /artists/{id}), avec cache."""
    ids = _unique(artist_ids)
    if not ids:
        return []

    missing = [a for a in ids if a not in _artist_genres_cache]
    while missing:
        chunk = missing[:50]
        missing = missing[50:]
        params = {"ids": ",".join(chunk)}
        data = api_request_with_reauth("GET", "/artists", token_cache, params=params)
        returned_ids = set()
        for art in (data.get("artists") or []):
            aid = art.get("id")
            if aid:
                _artist_genres_cache[aid] = art.get("genres") or []
                returned_ids.add(aid)
        # fallback individuel si l'API batch ne renvoie pas l'artiste
        for aid in [x for x in chunk if x not in returned_ids]:
            try:
                a = api_request_with_reauth("GET", f"/artists/{aid}", token_cache)
                _artist_genres_cache[aid] = a.get("genres") or []
            except Exception:
                _artist_genres_cache[aid] = []

    genres = set()
    for aid in ids:
        for g in _artist_genres_cache.get(aid, []):
            if g:
                genres.add(g)
    return sorted(genres)

# ===================== Enrichissement CSV =====================
def enrich_csv_with_genres(
    input_csv_path: str,
    output_csv_path: str,
    token_cache: Dict[str, Any],
    artist_ids_field: str = ARTIST_IDS_FIELD,
    album_id_field: str = ALBUM_ID_FIELD,
    genre_field: str = GENRE_FIELD,
    sep_in: str = SEP_IN,
    sep_out: str = SEP_OUT
) -> str:
    if not os.path.isabs(input_csv_path):
        input_csv_path = os.path.join(BASE_DIR, input_csv_path)
    if not os.path.isabs(output_csv_path):
        output_csv_path = os.path.join(BASE_DIR, output_csv_path)

    if not os.path.exists(input_csv_path):
        raise FileNotFoundError(f"Fichier introuvable: {input_csv_path}")

    # Lecture du CSV source
    with open(input_csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        in_fields = reader.fieldnames or []

    # Pré-collecte de tous les artist_ids pour précharger le cache via batch
    all_artist_ids: List[str] = []
    for row in rows:
        raw = (row.get(artist_ids_field) or "")
        parts = [p.strip() for p in raw.replace(",", sep_in).split(sep_in) if p.strip()]
        all_artist_ids.extend(parts)

        album_id = (row.get(album_id_field) or "").strip()
        if album_id:
            all_artist_ids.extend(get_album_artist_ids(token_cache, album_id))

    # Préchargement (remplit _artist_genres_cache)
    get_genres_for_artist_ids(token_cache, _unique(all_artist_ids))

    # Champs de sortie (on ajoute/écrase 'genre' en fin de ligne)
    out_fields = list(in_fields)
    if genre_field not in out_fields:
        out_fields.append(genre_field)

    with open(output_csv_path, "w", encoding="utf-8", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=out_fields)
        writer.writeheader()

        for row in rows:
            raw = (row.get(artist_ids_field) or "")
            ids = [p.strip() for p in raw.replace(",", sep_in).split(sep_in) if p.strip()]

            album_id = (row.get(album_id_field) or "").strip()
            if album_id:
                ids.extend(get_album_artist_ids(token_cache, album_id))

            genres = get_genres_for_artist_ids(token_cache, _unique(ids))
            row[genre_field] = sep_out.join(genres)
            writer.writerow(row)

# ===================== Token helper =====================
def ensure_user_token() -> Dict[str, Any]:
    if not CLIENT_ID or CLIENT_ID == "xx" or CLIENT_ID.startswith("XXX_"):
        print("Erreur: configure SPOTIFY_CLIENT_ID (CLIENT_ID).", file=sys.stderr)
        sys.exit(1)

    tok = load_token_cache()
    if not tok or not is_token_valid(tok):
        if tok and "refresh_token" in tok:
            try:
                tok = refresh_access_token(tok["refresh_token"], CLIENT_ID)
                save_token_cache(tok)
            except Exception:
                tok = None
        if not tok:
            print("Ouverture du navigateur pour autoriser l'accès (aucun scope requis)...")
            tok = get_user_token_via_pkce(CLIENT_ID, REDIRECT_URI, SCOPE)
            save_token_cache(tok)
    return tok

# ===================== Main =====================
def main():
    tok = ensure_user_token()
    out = enrich_csv_with_genres(INPUT_CSV, OUTPUT_CSV, tok)
    print(f"CSV enrichi écrit: {out}")

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