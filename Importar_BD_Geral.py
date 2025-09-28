# -*- coding: utf-8 -*-
# Importar_BD_Geral_fast.py — versão “à prova de 429/503”
#
# Requisitos:
#   pip install gspread google-auth gspread-formatting
# Credenciais:
#   credenciais.json na mesma pasta.

import re
import time
import random
import unicodedata
from datetime import datetime, timezone, timedelta

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, WorksheetNotFound

try:
    from gspread_formatting import format_cell_range, CellFormat, NumberFormat
    HAS_FMT = True
except Exception:
    HAS_FMT = False

# ==========================
# CONFIG
# ==========================
CAMINHO_CRED = "credenciais.json"

ID_FONTE = "1jcGbthzmQcdl8VHaTZcKgeo5cB9m_h8E6V4VE7zZfZU"
ABA_FONTE_DADOS = "bd_geral"
ABA_CONFIG_FONTE = "config"
COL_DESTINOS = "I"
LINHA_INICIO_DESTINOS = 2

ABA_DESTINO_DADOS = "bd"
ABA_DESTINO_RESUMO = "Resumo_MENSAL"
ABA_DESTINO_CONFIG = "bd_config"

RANGE_FILTROS = "F2:F"
CEL_RESUMO_TIMESTAMP_H = "H2"
B_UNICOS_START_ROW = 7
RESUMO_UNICOS_COL = "C"   # únicos agora em Resumo_MENSAL!C7:C

# Performance/robustez
TZ_SAO_PAULO = timezone(timedelta(hours=-3))

# Tentativas e pausas
MAX_RETRIES = 10                 # retries por chamada (get, batch_update, etc.)
DEST_RETRIES = 6                 # retries por destino
DEST_ROUNDS  = 5                 # rodadas extras p/ pendentes

BASE_SLEEP = 1.2                 # base do backoff exponencial
SLEEP_BETWEEN_BATCHES = 6.0      # pausa entre micro-batches de escrita (seg)
SLEEP_BETWEEN_DESTINOS = 2.0     # pausa entre destinos (seg)
RATE_LIMIT_COOLDOWN_BASE = 15.0  # cooldown extra em 429 (multiplica por tentativa)

CHUNK = 1000                     # linhas por bloco de dados (cada bloco vira 1 range)
MAX_CELLS_PER_BATCH = 49000      # células por micro-batch (mantém sob limites)

# Formatação (opcional)
APLICAR_FORMATACAO = False
SLEEP_FMT = 0.1

# Colunas por TIPO (letras 1-indexadas)
TIME_COLS = {"F","G","H","I","J","K","N","O","S","T","U","X","AH","AI","AK"}
NUMBER_COLS = {"L","M","P","Q","R","V","Z","AA","AB","AC","AD","AE","AF","AG","AJ","AL","AM","AN"}

FORCAR_TEXTO_HEADERS = {
    "CEP","CNPJ","CPF","MATRICULA","MATRÍCULA","N°","Nº",
    "NUMERO NOTA","NÚMERO NOTA","NUMERO DA NOTA","NÚMERO DA NOTA",
    "CODIGO","CÓDIGO","ID","OS","TICKET","PROTOCOLO"
}

# ==========================
# Helpers c/ retry
# ==========================
def auth_gspread():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    cred = Credentials.from_service_account_file(CAMINHO_CRED, scopes=scopes)
    return gspread.authorize(cred)

def get_http_status(err: Exception) -> int | None:
    if isinstance(err, APIError):
        try:
            # gspread >= 6 expõe .response
            return getattr(err, "response", None).status_code  # type: ignore[attr-defined]
        except Exception:
            pass
    # fallback: extrair do texto "[429]" etc.
    m = re.search(r"\[(\d{3})\]", str(err))
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None

def is_transient_error(err: Exception) -> bool:
    code = get_http_status(err)
    msg  = str(err).lower()
    if code in {429, 500, 502, 503, 504}:
        return True
    # alguns provedores retornam msg despadronizada
    if "quota exceeded" in msg or "service is currently unavailable" in msg:
        return True
    return False

def retry_sleep(i, extra: float = 0.0):
    time.sleep(BASE_SLEEP * (2 ** (i - 1)) + random.uniform(0, 0.6) + extra)

def with_retry(fn, *args, **kwargs):
    for i in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            transient = is_transient_error(e)
            code = get_http_status(e)
            if not transient or i == MAX_RETRIES:
                raise
            # 429: espera mais
            extra = 0.0
            if code == 429:
                extra = RATE_LIMIT_COOLDOWN_BASE * i
                print(f"   • Rate limit (429). Cooldown {extra:.1f}s antes do retry {i}/{MAX_RETRIES}…")
            else:
                print(f"   • Falha transitória ({code}). Retry {i}/{MAX_RETRIES}…")
            retry_sleep(i, extra=extra)

def safe_open_spreadsheet(gc, spreadsheet_id_or_url):
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", spreadsheet_id_or_url)
    ssid = m.group(1) if m else spreadsheet_id_or_url.strip()
    for i in range(1, MAX_RETRIES + 1):
        try:
            return gc.open_by_key(ssid), ssid
        except Exception as e:
            if i == MAX_RETRIES or not is_transient_error(e):
                raise
            retry_sleep(i)

def safe_get_worksheet(spreadsheet, title, create_if_missing=False, rows=1000, cols=60):
    for i in range(1, MAX_RETRIES + 1):
        try:
            return spreadsheet.worksheet(title)
        except WorksheetNotFound:
            if create_if_missing:
                return spreadsheet.add_worksheet(title, rows=rows, cols=cols)
            raise
        except Exception as e:
            if i == MAX_RETRIES or not is_transient_error(e):
                raise
            retry_sleep(i)

def try_batch_clear(ws, rngs):
    try:
        with_retry(ws.batch_clear, rngs)
        return True
    except Exception:
        return False

def values_batch_update(spreadsheet, data, value_input_option="RAW"):
    body = {"valueInputOption": value_input_option, "data": data}
    return with_retry(spreadsheet.values_batch_update, body)

def read_all_values(ws):
    return with_retry(ws.get_all_values)

def get_range_with_retry(ws, a1_range: str):
    return with_retry(ws.get, a1_range)

def clean_cell(x):
    if x is None: return ""
    s = str(x)
    s = re.sub(r"[\u200b\u200c\u200d\uFEFF]", "", s)
    return s.strip()

def normalize_for_match(s: str) -> str:
    if s is None: return ""
    s2 = str(s).strip().lower()
    s2 = "".join(c for c in unicodedata.normalize("NFD", s2) if unicodedata.category(c) != "Mn")
    s2 = re.sub(r"\s+", " ", s2)
    s2 = re.sub(r"\s*-\s*", " - ", s2)
    s2 = re.sub(r"\s+", " ", s2).strip()
    return s2

def letter_to_index(letter: str) -> int:
    letter = letter.upper().strip()
    n = 0
    for ch in letter: n = n*26 + (ord(ch) - ord('A') + 1)
    return n - 1

def a1_last_col_letter(ncols: int) -> str:
    return gspread.utils.rowcol_to_a1(1, ncols).split("1")[0]

def a1(ws, rng: str) -> str:
    return f"'{ws.title}'!{rng}"

# ======== Converters (sem apóstrofo) ========
TZ = TZ_SAO_PAULO

def serial_from_datetime(dt: datetime) -> float:
    base = datetime(1899, 12, 30, tzinfo=TZ)
    delta = dt - base
    return delta.days + (delta.seconds + delta.microseconds / 1e6) / 86400.0

def to_date_serial_keep(v):
    if v is None or v == "": return ""
    if isinstance(v, (int, float)):
        try: return float(v)
        except Exception: return v
    s = str(v).strip()
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})(?:\s+\d{2}:\d{2}(?::\d{2})?)?$", s)
    if m:
        d, mth, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        dt = datetime(y, mth, d, tzinfo=TZ)
        return serial_from_datetime(dt)
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=TZ)
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return serial_from_datetime(dt)
    except Exception:
        try: return float(s)
        except Exception: return s

def to_time_serial_keep(v):
    if v is None or v == "": return ""
    if isinstance(v, (int, float)):
        try: return float(v)
        except Exception: return v
    s = str(v).strip()
    m = re.match(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$", s)
    if m:
        hh = int(m.group(1)); mm = int(m.group(2)); ss = int(m.group(3) or 0)
        return (hh*3600 + mm*60 + ss) / 86400.0
    m = re.match(r"^\d{2}/\d{2}/\d{4}\s+(\d{1,2}):(\d{2})(?::(\d{2}))?$", s)
    if m:
        hh = int(m.group(1)); mm = int(m.group(2)); ss = int(m.group(3) or 0)
        return (hh*3600 + mm*60 + ss) / 86400.0
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=TZ)
        return (dt.hour*3600 + dt.minute*60 + dt.second) / 86400.0
    except Exception:
        try: return float(s)
        except Exception: return s

def is_zero_left_string(s: str) -> bool:
    return bool(re.match(r"^0\d+$", s.strip()))

def parse_number_brazil(x):
    if x is None: return ""
    if isinstance(x, (int, float)): return float(x)
    s = str(x).strip()
    if re.search(r"[A-Za-z]", s): return s
    if is_zero_left_string(s): return s
    s2 = s.replace(".", "").replace(",", ".")
    s2 = re.sub(r"[^0-9\.\-]", "", s2)
    if re.fullmatch(r"-?\d+(\.\d+)?", s2):
        try: return float(s2)
        except Exception: return s
    return s

# ======== Micro-batching ========
def count_cells_in_entry(entry):
    rng = entry["range"].split("!", 1)[1]
    if ":" not in rng:
        return 1
    a1_start, a1_end = rng.split(":")
    r1, c1 = gspread.utils.a1_to_rowcol(a1_start)
    r2, c2 = gspread.utils.a1_to_rowcol(a1_end)
    return (r2 - r1 + 1) * (c2 - c1 + 1)

def chunk_data_batch(entries, max_cells=MAX_CELLS_PER_BATCH):
    chunk, cells = [], 0
    for e in entries:
        e_cells = count_cells_in_entry(e)
        if cells + e_cells > max_cells and chunk:
            yield chunk
            chunk, cells = [], 0
        chunk.append(e); cells += e_cells
    if chunk: yield chunk

# ==========================
# Pipeline — com retry por DESTINO e RODADAS
# ==========================
def process_destino(gc, ss_fonte, headers, corpo_raw, d_original_list, d_normalized_list, ncols, dest):
    """Processa 1 destino. Retorna True se concluiu, False se falha não-transitória."""
    # Abertura do destino e abas
    ss_dest, ssid = safe_open_spreadsheet(gc, dest)
    ws_resumo     = safe_get_worksheet(ss_dest, ABA_DESTINO_RESUMO)
    ws_bd_config  = safe_get_worksheet(ss_dest, ABA_DESTINO_CONFIG, create_if_missing=True)
    ws_bd_dest    = safe_get_worksheet(ss_dest, ABA_DESTINO_DADOS,  create_if_missing=True)

    # Colunas presentes (evita acessar fora do header)
    time_cols_present = sorted([c for c in TIME_COLS if letter_to_index(c) < ncols], key=lambda x: letter_to_index(x))
    num_cols_present  = sorted([c for c in NUMBER_COLS if letter_to_index(c) < ncols], key=lambda x: letter_to_index(x))

    print(f"🎯 Destino: {dest}")

    # ===== Lê filtros (retry robusto) =====
    try:
        filtros_vals = get_range_with_retry(ws_bd_config, RANGE_FILTROS)
    except Exception as e:
        print(f"❌ Erro lendo '{ABA_DESTINO_CONFIG}!{RANGE_FILTROS}' em {ssid}: {e}")
        if is_transient_error(e):
            raise  # deixa o chamador reprocessar o destino
        return False

    filtros_norm = []
    for row in filtros_vals:
        if not row: continue
        v = clean_cell(row[0])
        if v != "":
            filtros_norm.append(normalize_for_match(v))
    filtros_set = set(f for f in filtros_norm if f)

    if not filtros_set:
        print(f"⚠️ Sem filtros em '{ABA_DESTINO_CONFIG}!{RANGE_FILTROS}'. Pulando destino.")
        return True  # não é erro

    # ===== Filtra pela coluna D (contém) =====
    linhas_filtradas = []
    termos_encontrados_orig = set()
    for i, row in enumerate(corpo_raw):
        if len(row) < ncols:
            row = row + [""] * (ncols - len(row))
        val_d_norm = d_normalized_list[i]
        if not val_d_norm:
            continue
        if any(f in val_d_norm for f in filtros_set):
            linhas_filtradas.append(row[:ncols])
            termos_encontrados_orig.add(d_original_list[i])

    total = len(linhas_filtradas)
    print(f"   • Filtros: {sorted(filtros_set)}")
    print(f"   • Linhas filtradas: {total}")

    # ===== Converte tipos (sem apóstrofo) =====
    conv_rows = []
    for r in linhas_filtradas:
        r2 = r[:]
        r2[0] = to_date_serial_keep(r2[0])  # A(data)
        for col_letter in time_cols_present:
            j = letter_to_index(col_letter)
            if 0 <= j < ncols:
                r2[j] = to_time_serial_keep(r2[j])
        for col_letter in num_cols_present:
            j = letter_to_index(col_letter)
            if 0 <= j < ncols:
                h = clean_cell(headers[j]).upper()
                if h not in FORCAR_TEXTO_HEADERS:
                    r2[j] = parse_number_brazil(r2[j])
        conv_rows.append(r2)

    # ===== Escrita em lote =====
    try:
        try:
            with_retry(ws_bd_dest.clear)
        except Exception:
            pass

        last_col_letter = a1_last_col_letter(ncols)
        data_batch = []

        # Cabeçalho
        data_batch.append({"range": a1(ws_bd_dest, "A1"), "values": [headers]})

        # Dados
        if total > 0:
            row_cursor = 2
            start = 0
            while start < total:
                chunk = conv_rows[start:start + CHUNK]
                rng = a1(ws_bd_dest, f"A{row_cursor}:{last_col_letter}{row_cursor + len(chunk) - 1}")
                data_batch.append({"range": rng, "values": chunk})
                start += CHUNK
                row_cursor += len(chunk)
        else:
            print("   • Sem linhas para colar (somente cabeçalho).")

        # ===== Resumo_MENSAL C7:C — únicos da coluna B =====
        col_b = [clean_cell(r[1]) for r in linhas_filtradas if len(r) > 1]
        unicos_b = sorted(set([v for v in col_b if v != ""]), key=lambda x: x.lower())
        max_clear_b = max(len(unicos_b), 1)
        clear_end_b = B_UNICOS_START_ROW + max_clear_b + 200

        # Limpa C7:C
        clear_rng_resumo = a1(ws_resumo, f"{RESUMO_UNICOS_COL}{B_UNICOS_START_ROW}:{RESUMO_UNICOS_COL}{clear_end_b}")
        if not try_batch_clear(ws_resumo, [f"{RESUMO_UNICOS_COL}{B_UNICOS_START_ROW}:{RESUMO_UNICOS_COL}"]):
            data_batch.append({
                "range": clear_rng_resumo,
                "values": [[""] for _ in range(clear_end_b - B_UNICOS_START_ROW + 1)]
            })

        # Escreve os únicos em C7:C
        if unicos_b:
            data_batch.append({
                "range": a1(ws_resumo, f"{RESUMO_UNICOS_COL}{B_UNICOS_START_ROW}:{RESUMO_UNICOS_COL}{B_UNICOS_START_ROW + len(unicos_b) - 1}"),
                "values": [[u] for u in unicos_b]
            })

        # ===== bd_config A2:A — únicos (originais) da coluna D =====
        unicos_d_orig = sorted([v for v in termos_encontrados_orig if v], key=lambda x: x.casefold())
        clear_end_a = 2 + max(len(unicos_d_orig), 1) + 500
        if not try_batch_clear(ws_bd_config, ["A2:A"]):
            data_batch.append({
                "range": a1(ws_bd_config, f"A2:A{clear_end_a}"),
                "values": [[""] for _ in range(clear_end_a - 1)]
            })
        if unicos_d_orig:
            data_batch.append({
                "range": a1(ws_bd_config, f"A2:A{1 + len(unicos_d_orig)}"),
                "values": [[u] for u in unicos_d_orig]
            })

        # Timestamp somente em H2
        stamp = datetime.now(TZ_SAO_PAULO).strftime("%d/%m/%Y %H:%M:%S")
        data_batch.append({"range": a1(ws_resumo, CEL_RESUMO_TIMESTAMP_H), "values": [[stamp]]})

        # Envia em micro-batches (com pausa entre eles)
        sent_batches = 0
        for part in chunk_data_batch(data_batch, max_cells=MAX_CELLS_PER_BATCH):
            values_batch_update(ss_dest, part, value_input_option="RAW")
            sent_batches += 1
            total_cells = sum(count_cells_in_entry(x) for x in part)
            print(f"   • Lote {sent_batches} enviado ({total_cells} células).")
            # Pausa curta para não estourar write/min
            time.sleep(SLEEP_BETWEEN_BATCHES)
        if sent_batches == 0:
            values_batch_update(ss_dest, data_batch, value_input_option="RAW")
            print("   • Lote único enviado.")

        # Formatação (opcional)
        if HAS_FMT and APLICAR_FORMATACAO and total > 0:
            total_rows = max(total + 1, 2)
            fmt_date = CellFormat(numberFormat=NumberFormat(type="DATE", pattern="dd/mm/yyyy"))
            format_cell_range(ws_bd_dest, f"A2:A{total_rows}", fmt_date); time.sleep(SLEEP_FMT)
            if time_cols_present:
                fmt_time = CellFormat(numberFormat=NumberFormat(type="TIME", pattern="hh:mm:ss"))
                for col_letter in time_cols_present:
                    format_cell_range(ws_bd_dest, f"{col_letter}2:{col_letter}{total_rows}", fmt_time); time.sleep(SLEEP_FMT)
            if num_cols_present:
                fmt_num = CellFormat(numberFormat=NumberFormat(type="NUMBER", pattern="0.############"))
                for col_letter in num_cols_present:
                    format_cell_range(ws_bd_dest, f"{col_letter}2:{col_letter}{total_rows}", fmt_num); time.sleep(SLEEP_FMT)

    except Exception as e:
        if is_transient_error(e):
            # sinaliza para o chamador tentar de novo o destino inteiro
            raise
        print(f"❌ Falha não-transitória no destino {ssid}: {e}")
        return False

    print("✅ Destino concluído.")
    return True

def main():
    print("🔐 Autenticando…")
    gc = auth_gspread()
    print("✅ Autenticado.")

    print("📂 Abrindo planilha FONTE…")
    ss_fonte, _ = safe_open_spreadsheet(gc, ID_FONTE)

    print(f"📄 Lendo aba '{ABA_FONTE_DADOS}'…")
    ws_bd_fonte = safe_get_worksheet(ss_fonte, ABA_FONTE_DADOS)
    dados = read_all_values(ws_bd_fonte)
    if not dados or len(dados) < 2:
        print("⚠️ 'bd_geral' vazio.")
        return

    headers = [clean_cell(c) for c in dados[0]]
    corpo_raw = [list(map(clean_cell, row)) for row in dados[1:]]
    ncols = len(headers)

    # Preparos coluna D
    d_original_list = []
    d_normalized_list = []
    for row in corpo_raw:
        val_d_orig = clean_cell(row[3]) if len(row) > 3 else ""
        d_original_list.append(val_d_orig)
        d_normalized_list.append(normalize_for_match(val_d_orig))

    # Destinos
    print("📋 Lendo destinos em 'config' (coluna I)…")
    ws_config_fonte = safe_get_worksheet(ss_fonte, ABA_CONFIG_FONTE)
    vals = get_range_with_retry(ws_config_fonte, f"{COL_DESTINOS}{LINHA_INICIO_DESTINOS}:{COL_DESTINOS}")
    destinos = [row[0].strip() for row in vals if row and row[0].strip()]
    if not destinos:
        print("⚠️ Nenhum destino em 'config'.")
        return

    print(f"🧭 {len(destinos)} destino(s) . Iniciando…\n")

    pendentes = []
    for dest in destinos:
        print("—" * 72)
        sucesso = False
        for attempt in range(1, DEST_RETRIES + 1):
            try:
                sucesso = process_destino(gc, ss_fonte, headers, corpo_raw, d_original_list, d_normalized_list, ncols, dest)
                break
            except Exception as e:
                if is_transient_error(e) and attempt < DEST_RETRIES:
                    code = get_http_status(e)
                    print(f"   • Falha transitória destino (HTTP {code}). Tentativa {attempt}/{DEST_RETRIES}.")
                    # cooldown extra em 429 para dar margem na cota por minuto
                    extra = RATE_LIMIT_COOLDOWN_BASE * attempt if code == 429 else 0.0
                    retry_sleep(attempt, extra=extra)
                    continue
                print(f"❌ Falha ao processar destino após {attempt} tentativa(s): {e}")
                break
        if not sucesso:
            pendentes.append(dest)
        # pausa entre destinos para baixar write/min
        time.sleep(SLEEP_BETWEEN_DESTINOS)

    round_idx = 1
    while pendentes and round_idx < DEST_ROUNDS:
        print("\n🔁 Rodada extra para pendentes…")
        restantes = []
        for dest in pendentes:
            print("—" * 72)
            sucesso = False
            for attempt in range(1, DEST_RETRIES + 1):
                try:
                    sucesso = process_destino(gc, ss_fonte, headers, corpo_raw, d_original_list, d_normalized_list, ncols, dest)
                    break
                except Exception as e:
                    if is_transient_error(e) and attempt < DEST_RETRIES:
                        code = get_http_status(e)
                        print(f"   • Falha transitória destino (HTTP {code}). Tentativa {attempt}/{DEST_RETRIES}.")
                        extra = RATE_LIMIT_COOLDOWN_BASE * attempt if code == 429 else 0.0
                        retry_sleep(attempt, extra=extra)
                        continue
                    print(f"❌ Falha ao processar destino após {attempt} tentativa(s): {e}")
                    break
            if not sucesso:
                restantes.append(dest)
            time.sleep(SLEEP_BETWEEN_DESTINOS)
        pendentes = restantes
        round_idx += 1

    if pendentes:
        print("\n⚠️ Alguns destinos ainda falharam após tentativas adicionais:")
        for d in pendentes: print("   -", d)
    else:
        print("\n🎉 Processo finalizado para todos os destinos com sucesso.")

if __name__ == "__main__":
    main()
