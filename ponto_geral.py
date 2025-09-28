# -*- coding: utf-8 -*-
"""
Importa arquivo (Excel/CSV) do Drive para 'bd_geral' (sempre limpando antes):
- Lê nome em config!C2, busca na pasta PASTA_ID
- Converte p/ Google Sheets temporário com nome __TMP_IMPORTADOR__<arquivo>
  (usa um único temporário: se existir com o mesmo nome, apaga e recria)
- Lê 1ª aba com UNFORMATTED_VALUE + SERIAL_NUMBER e escreve RAW (sem apóstrofo)
- Converte APENAS as colunas: L, P, Q, R, Z, AA, AC, AD, AE, AF, AG, AJ, AL, AM, AN → número
- Lotes grandes (BATCH=5000) + retry/backoff p/ evitar 429
- Garante exclusão do temporário ao final (mesmo se der erro)
- Logs detalhados no CMD

AJUSTE: após concluir a importação, o timestamp gravado em config!A2 desta
planilha é replicado para todas as planilhas listadas em config!I2:I, na célula
Resumo_MENSAL!J2 de cada uma.
"""

import random
import re
import time
from datetime import datetime
from typing import Any, List

import pytz
import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ======== CONFIG ========
CAMINHO_CRED = "credenciais.json"

SPREADSHEET_ID_DEST = "1jcGbthzmQcdl8VHaTZcKgeo5cB9m_h8E6V4VE7zZfZU"
PASTA_ID = "1fDcVXWg1YJ3xlAer0JmOD59XtryiWR1N"

ABA_CONFIG = "config"
ABA_DESTINO = "bd_geral"

BATCH = 5000
RATE_LIMIT_SLEEP = 0.4
MAX_TRIES = 6

TZ = pytz.timezone("America/Sao_Paulo")


def log(msg: str):
    print(f"[{datetime.now(TZ).strftime('%d/%m/%Y %H:%M:%S')}] {msg}")


def auth_clients():
    log("🔐 Autenticando APIs (Drive/Sheets)…")
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    creds = Credentials.from_service_account_file(CAMINHO_CRED, scopes=scopes)
    gc = gspread.authorize(creds)
    drive = build("drive", "v3", credentials=creds)
    sheets = build("sheets", "v4", credentials=creds)
    log("✅ Autenticação OK.")
    return gc, drive, sheets


def open_ws(gc: gspread.Client, spreadsheet_id: str, title: str) -> gspread.Worksheet:
    sh = gc.open_by_key(spreadsheet_id)
    try:
        return sh.worksheet(title)
    except WorksheetNotFound:
        raise RuntimeError(f"❌ Aba '{title}' não encontrada no destino.")


def read_cell(ws: gspread.Worksheet, a1: str) -> str:
    v = ws.acell(a1).value
    return (v or "").strip()


def write_cell(ws: gspread.Worksheet, a1: str, value: Any):
    ws.update_acell(a1, value)


def a1_from_rc(row: int, col: int) -> str:
    letters = ""
    c = col
    while c:
        c, r = divmod(c - 1, 26)
        letters = chr(r + 65) + letters
    return f"{letters}{row}"


def range_a1(row: int, col: int, nrows: int, ncols: int) -> str:
    return f"{a1_from_rc(row, col)}:{a1_from_rc(row + nrows - 1, col + ncols - 1)}"


def ensure_size(ws: gspread.Worksheet, need_last_row: int, need_last_col: int):
    rows = max(ws.row_count, need_last_row)
    cols = max(ws.col_count, need_last_col)
    if rows != ws.row_count or cols != ws.col_count:
        log(f"🧱 Redimensionando grade do destino para {rows} linhas x {cols} colunas…")
        ws.resize(rows=rows, cols=cols)


def find_in_folder_by_name(drive, pasta_id: str, nome: str) -> str:
    # escapa apóstrofo para a query do Drive
    safe = nome.replace("'", "\\'")
    query = "name = '" + safe + "' and '" + pasta_id + "' in parents and trashed = false"
    log(f"🔎 Procurando '{nome}' na pasta {pasta_id}…")
    resp = drive.files().list(
        q=query,
        fields="files(id,name)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        corpora="allDrives",
    ).execute()
    files = resp.get("files", [])
    if not files:
        raise RuntimeError(f"❌ Arquivo '{nome}' não encontrado na pasta.")
    file_id = files[0]["id"]
    log(f"📄 Arquivo encontrado: id={file_id}")
    return file_id


def trash_file(drive, file_id: str):
    if not file_id:
        return
    try:
        log(f"🗑️ Enviando temporário {file_id} para a lixeira…")
        drive.files().update(fileId=file_id, body={"trashed": True}, supportsAllDrives=True).execute()
        log("✅ Temporário movido para a lixeira.")
    except HttpError as e:
        log(f"⚠️ Não consegui lixar o temporário ({e}).")


def find_existing_temp_and_trash(drive, temp_name: str):
    """Apaga todos os temporários com esse nome, se existirem. (sem f-string com backslash)"""
    safe_name = temp_name.replace("'", "\\'")
    query = "name = '" + safe_name + "' and trashed = false"
    resp = drive.files().list(
        q=query,
        fields="files(id,name)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        corpora="allDrives",
    ).execute()
    files = resp.get("files", [])
    if files:
        log(f"🧹 Encontrado(s) temporário(s) anterior(es): {len(files)} — enviando à lixeira…")
        for f in files:
            trash_file(drive, f["id"])


def create_temp_sheet(drive, src_file_id: str, temp_name: str) -> str:
    """Apaga temporários homônimos e cria um novo."""
    find_existing_temp_and_trash(drive, temp_name)
    log("🧪 Convertendo arquivo para Google Sheets temporário…")
    body = {"name": temp_name, "mimeType": "application/vnd.google-apps.spreadsheet"}
    nf = drive.files().copy(fileId=src_file_id, body=body, supportsAllDrives=True).execute()
    temp_id = nf["id"]
    log(f"✅ Temporário criado: {temp_id}")
    return temp_id


def values_get_unformatted(sheets_api, spreadsheet_id: str, sheet_title: str) -> List[List[Any]]:
    """Lê valores não formatados e datas/horas como serial (sem apóstrofo)."""
    log("📖 Lendo dados do temporário (UNFORMATTED_VALUE + SERIAL_NUMBER)…")
    res = sheets_api.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=sheet_title,
        valueRenderOption="UNFORMATTED_VALUE",
        dateTimeRenderOption="SERIAL_NUMBER"
    ).execute()
    values = res.get("values", [])
    log(f"📦 Linhas lidas: {len(values)}")
    return values


def values_update_raw_with_retry(sheets_api, dest_spreadsheet_id: str, a1_range: str, values: List[List[Any]]):
    """update RAW com retry/backoff p/ 429/500/503"""
    attempt = 0
    while True:
        try:
            return sheets_api.spreadsheets().values().update(
                spreadsheetId=dest_spreadsheet_id,
                range=a1_range,
                valueInputOption="RAW",
                body={"values": values}
            ).execute()
        except HttpError as e:
            status = getattr(e, "resp", None).status if getattr(e, "resp", None) else None
            if status in (429, 500, 503) and attempt < MAX_TRIES - 1:
                delay = (2 ** attempt) + random.uniform(0.0, 0.5)
                log(f"⏳ {status} rate-limit/erro transitório — retry {attempt+1}/{MAX_TRIES} em {delay:.1f}s…")
                time.sleep(delay)
                attempt += 1
                continue
            raise


# ---------- COERÇÃO SOMENTE NAS COLUNAS PEDIDAS ----------
TARGET_COLS_LETTERS = ["L","P","Q","R","Z","AA","AC","AD","AE","AF","AG","AJ","AL","AM","AN"]

def col_letter_to_index(letter: str) -> int:
    s = letter.strip().upper()
    val = 0
    for ch in s:
        if 'A' <= ch <= 'Z':
            val = val * 26 + (ord(ch) - 64)
    return val if val > 0 else 1

TARGET_COLS = {col_letter_to_index(c) for c in TARGET_COLS_LETTERS}

def to_number_if_possible(v: Any):
    """Remove apóstrofo à esquerda e tenta parse numérico (pt-BR/US)."""
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        return v
    s = str(v).strip()
    if s == "":
        return ""
    if s.startswith("'"):
        s = s[1:].strip()
    s_br = s.replace(" ", "").replace("R$", "")
    if any(c in s_br for c in ",."):
        if "," in s_br and (s_br.rfind(",") > s_br.rfind(".")):
            try:
                f = float(s_br.replace(".", "").replace(",", "."))
                return int(f) if f.is_integer() else f
            except:
                pass
    try:
        f = float(s.replace(" ", "").replace("R$", ""))
        return int(f) if f.is_integer() else f
    except:
        try:
            return int(s)
        except:
            return s

def coerce_columns_to_number(block: List[List[Any]]) -> List[List[Any]]:
    out = []
    for row in block:
        if not row:
            out.append(row); continue
        new_row = []
        for idx, val in enumerate(row, start=1):
            new_row.append(to_number_if_possible(val) if idx in TARGET_COLS else val)
        out.append(new_row)
    return out
# ---------------------------------------------------------


# ======== NOVOS HELPERS PARA O AJUSTE ========
def normalize_sheet_id(s: str) -> str:
    """Aceita ID puro ou URL do Sheets e retorna o ID."""
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", s or "")
    return m.group(1) if m else (s or "").strip()

def read_destinations_from_config(ws_config: gspread.Worksheet, col_letter: str = "I", start_row: int = 2) -> List[str]:
    """Lê config!I2:I e retorna IDs/URLs não vazios."""
    vals = ws_config.get(f"{col_letter}{start_row}:{col_letter}")
    out: List[str] = []
    for row in vals:
        if row and str(row[0]).strip():
            out.append(str(row[0]).strip())
    return out

def write_timestamp_to_resumo_j2(sheets_api, spreadsheet_id: str, when_str: str):
    """Escreve o timestamp em Resumo_MENSAL!J2 com retry/backoff."""
    target_id = normalize_sheet_id(spreadsheet_id)
    values_update_raw_with_retry(sheets_api, target_id, "Resumo_MENSAL!J2", [[when_str]])
# ==============================================


def importar_excel_para_bd_geral():
    log("🚀 Iniciando importação…")
    gc, drive, sheets_api = auth_clients()

    log("📂 Abrindo abas de destino…")
    ws_config = open_ws(gc, SPREADSHEET_ID_DEST, ABA_CONFIG)
    ws_dest = open_ws(gc, SPREADSHEET_ID_DEST, ABA_DESTINO)

    log("🧭 Lendo parâmetros em config…")
    nome_arquivo = read_cell(ws_config, "C2")
    if not nome_arquivo:
        raise RuntimeError("❌ 'config!C2' vazio. Informe o nome do arquivo (com extensão).")
    log(f"📝 Nome do arquivo a importar: {nome_arquivo}")

    temp_id = ""
    temp_name = f"__TMP_IMPORTADOR__{nome_arquivo}"
    try:
        # Sempre converte usando um ÚNICO temporário (apaga anteriores com o mesmo nome)
        src_file_id = find_in_folder_by_name(drive, PASTA_ID, nome_arquivo)
        temp_id = create_temp_sheet(drive, src_file_id, temp_name)

        log("🧹 Limpando aba 'bd_geral'…")
        ws_dest.clear()
        write_cell(ws_config, "B2", "📥 Arquivo convertido. Iniciando importação…")

        log("🔗 Abrindo temporário e coletando dados…")
        sh_temp = gc.open_by_key(temp_id)
        first_ws = sh_temp.get_worksheet(0)
        sheet_title = first_ws.title
        dados = values_get_unformatted(sheets_api, temp_id, sheet_title)
        if not dados:
            write_cell(ws_config, "B2", "⚠️ Aba convertida está vazia.")
            log("⛔ Nada para importar. Encerrando.")
            return

        total_rows = len(dados)
        total_cols = max(len(r) for r in dados)
        log(f"📊 Tamanho do dataset: {total_rows} linhas x {total_cols} colunas (máx).")

        linha_destino = 1
        i = 0
        bloco_idx = 0

        while i < total_rows:
            fim = min(i + BATCH, total_rows)
            bloco = dados[i:fim]
            bloco_idx += 1

            bloco_padded = [row + [""] * (total_cols - len(row)) for row in bloco]
            bloco_fixed = coerce_columns_to_number(bloco_padded)

            ensure_size(ws_dest, need_last_row=linha_destino + len(bloco_fixed) - 1, need_last_col=total_cols)

            rng = range_a1(linha_destino, 1, len(bloco_fixed), total_cols)
            log(f"📥 Lote {bloco_idx}: colando {len(bloco_fixed)} linhas no intervalo {rng}…")
            values_update_raw_with_retry(sheets_api, SPREADSHEET_ID_DEST, rng, bloco_fixed)

            linha_destino += len(bloco_fixed)
            i = fim
            time.sleep(RATE_LIMIT_SLEEP)

        log("🧾 Finalizando (registrando timestamp em config)…")
        agora = datetime.now(TZ).strftime("%d/%m/%Y %H:%M:%S")
        write_cell(ws_config, "A2", agora)
        write_cell(ws_config, "B2", f"Concluído em {agora}")

        # ===== NOVO: replicar timestamp em todas as planilhas listadas em config!I2:I =====
        log("↗️ Replicando timestamp em Resumo_MENSAL!J2 das planilhas listadas em config!I…")
        destinos = read_destinations_from_config(ws_config, col_letter="I", start_row=2)
        if not destinos:
            log("⚠️ Nenhum destino encontrado em config!I2:I (nada a replicar).")
        else:
            ok, fail = 0, 0
            for dst in destinos:
                try:
                    write_timestamp_to_resumo_j2(sheets_api, dst, agora)
                    ok += 1
                except Exception as e:
                    fail += 1
                    log(f"❌ Falha ao escrever timestamp em '{dst}': {e}")
            log(f"✅ Replicação concluída — sucesso: {ok}, falhas: {fail}")

    finally:
        # Garantia de limpeza do temporário mesmo em caso de erro
        trash_file(drive, temp_id)


if __name__ == "__main__":
    try:
        importar_excel_para_bd_geral()
    except Exception as e:
        log(f"❌ Erro ao importar Excel: {e}")
