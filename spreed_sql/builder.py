"""
SpreedSQL Builder - Ejecuta un esquema contra la API de Google Sheets.
Equivale a psycopg2.execute(open('schema.sql')) pero para spreadsheets.
"""
import gspread
from google.oauth2.service_account import Credentials
from contextlib import suppress
import string
from typing import Optional, Dict, Any
from .models import Spreadsheet, Tab, DataType, EditMode


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _get_gc(
    credentials_path: Optional[str] = None,
    credentials_dict: Optional[dict] = None,
    gc: Optional[gspread.Client] = None,
) -> gspread.Client:
    """
    Autentica y retorna un cliente gspread.
    Soporta 3 modos:
      1. credentials_path: ruta al JSON de service account
      2. credentials_dict: dict del JSON de service account (ej: desde env var)
      3. gc: cliente gspread pre-autenticado (ej: OAuth externo)
    """
    if gc:
        return gc
    if credentials_dict:
        creds = Credentials.from_service_account_info(credentials_dict, scopes=SCOPES)
        return gspread.authorize(creds)
    path = credentials_path or "credentials.json"
    creds = Credentials.from_service_account_file(path, scopes=SCOPES)
    return gspread.authorize(creds)


def _col_letter(index: int) -> str:
    """Convierte indice 0-based a letra de columna (0=A, 1=B, ... 25=Z, 26=AA)."""
    result = ""
    while index >= 0:
        result = chr(index % 26 + 65) + result
        index = index // 26 - 1
    return result


def _hex_to_rgb(hex_color: str) -> dict:
    color = (hex_color or "").strip().lstrip("#")
    if len(color) != 6 or any(ch not in string.hexdigits for ch in color):
        return {"red": 0.9, "green": 0.9, "blue": 0.95}
    return {
        "red": int(color[0:2], 16) / 255,
        "green": int(color[2:4], 16) / 255,
        "blue": int(color[4:6], 16) / 255,
    }


def _resolve_ref_column_letter(spreadsheet_obj, ref_tab_name: str, ref_col_name: str) -> str:
    if not spreadsheet_obj:
        return "A"
    tabs = getattr(spreadsheet_obj, "tabs", None)
    if not isinstance(tabs, list):
        return "A"
    ref_tab = next((item for item in tabs if getattr(item, "name", None) == ref_tab_name), None)
    if not ref_tab:
        return "A"
    with suppress(ValueError, TypeError):
        ref_col_idx = ref_tab.get_column_index(ref_col_name) - 1
        return _col_letter(ref_col_idx)
    return "A"


def _parse_enum_reference(values_ref: str) -> Optional[tuple]:
    if not isinstance(values_ref, str):
        return None
    raw = values_ref.strip()
    if not raw:
        return None
    if "." in raw:
        left, right = raw.split(".", 1)
        if left.strip() and right.strip():
            return left.strip(), right.strip()
    return None


def _apply_column_formatting(worksheet, tab: Tab):
    """Aplica formato a los headers (negrita, colores, anchos)."""
    num_cols = len(tab.columns)
    end_col = _col_letter(num_cols - 1)
    
    # Headers en negrita
    header_bg = {"red": 0.9, "green": 0.9, "blue": 0.95}
    text_format: Dict[str, Any] = {"bold": True}

    if tab.is_native_table and tab.table_theme:
        header_bg = _hex_to_rgb(tab.table_theme.header_bg)
        text_format["foregroundColor"] = _hex_to_rgb(tab.table_theme.header_text)

    worksheet.format(
        f"A1:{end_col}1",
        {
            "textFormat": text_format,
            "backgroundColor": header_bg,
        },
    )
    
    # Congelar filas/columnas
    worksheet.freeze(rows=tab.freeze_rows, cols=tab.freeze_cols)


def _apply_data_validations(worksheet, tab: Tab, spreadsheet_obj=None):
    """Aplica validaciones usando batch_update JSON crudo (Google Sheets API v4)."""
    requests = []
    max_rows = max(2, int(tab.table_rows or 1000))

    for col_idx, col in enumerate(tab.columns):
        range_obj = {
            "sheetId": worksheet.id,
            "startRowIndex": 1,
            "endRowIndex": max_rows,
            "startColumnIndex": col_idx,
            "endColumnIndex": col_idx + 1,
        }

        # ForeignKey: dropdown con valores de otra pestaña (ONE_OF_RANGE)
        if col.foreign_key:
            fk = col.foreign_key
            fk_col_letter = _resolve_ref_column_letter(spreadsheet_obj, fk.tab, fk.column)
            fk_ref = f"'{fk.tab}'!{fk_col_letter}2:{fk_col_letter}"
            requests.append(
                {
                    "setDataValidation": {
                        "range": range_obj,
                        "rule": {
                            "condition": {
                                "type": "ONE_OF_RANGE",
                                "values": [{"userEnteredValue": fk_ref}],
                            },
                            "showCustomUi": True,
                            "strict": not fk.allow_blank,
                        },
                    }
                }
            )
            continue

        # ENUM: lista fija o referencia dinámica "Tab.Columna"
        if col.dtype == DataType.ENUM and col.values:
            enum_ref = _parse_enum_reference(col.values) if isinstance(col.values, str) else None
            if enum_ref:
                ref_tab_name, ref_col_name = enum_ref
                ref_col_letter = _resolve_ref_column_letter(spreadsheet_obj, ref_tab_name, ref_col_name)
                enum_ref_value = f"'{ref_tab_name}'!{ref_col_letter}2:{ref_col_letter}"
                condition = {
                    "type": "ONE_OF_RANGE",
                    "values": [{"userEnteredValue": enum_ref_value}],
                }
            else:
                values_list = col.values if isinstance(col.values, list) else [str(col.values)]
                condition = {
                    "type": "ONE_OF_LIST",
                    "values": [{"userEnteredValue": str(v)} for v in values_list],
                }

            requests.append(
                {
                    "setDataValidation": {
                        "range": range_obj,
                        "rule": {
                            "condition": condition,
                            "showCustomUi": True,
                            "strict": not col.enum_allow_custom,
                        },
                    }
                }
            )

    if requests:
        with suppress(gspread.exceptions.GSpreadException, ValueError, TypeError):
            worksheet.spreadsheet.batch_update({"requests": requests})


def _apply_native_table_features(worksheet, tab: Tab):
    if not tab.is_native_table:
        return None

    table_name = tab.table_name or f"Tabla_{tab.name.replace(' ', '_')}"
    row_end = max(2, int(tab.table_rows or 1000))
    col_count = len(tab.columns) + (len(tab.computed_columns) if tab.computed_columns else 0)
    col_end = max(1, col_count)

    requests = []
    if tab.filter is None or tab.filter.enabled:
        requests.append(
            {
                "setBasicFilter": {
                    "filter": {
                        "range": {
                            "sheetId": worksheet.id,
                            "startRowIndex": 0,
                            "endRowIndex": row_end,
                            "startColumnIndex": 0,
                            "endColumnIndex": col_end,
                        }
                    }
                }
            }
        )

    banded_range = {
        "range": {
            "sheetId": worksheet.id,
            "startRowIndex": 0,
            "endRowIndex": row_end,
            "startColumnIndex": 0,
            "endColumnIndex": col_end,
        },
        "headerColor": _hex_to_rgb(tab.table_theme.header_bg) if tab.table_theme else {"red": 0.87, "green": 0.90, "blue": 0.98},
        "firstBandColor": _hex_to_rgb(tab.table_theme.stripe_bg) if tab.table_theme else {"red": 0.96, "green": 0.97, "blue": 0.99},
        "secondBandColor": {"red": 1, "green": 1, "blue": 1},
    }
    requests.append({"addBanding": {"bandedRange": banded_range}})
    requests.append(
        {
            "addNamedRange": {
                "namedRange": {
                    "name": table_name,
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 0,
                        "endRowIndex": row_end,
                        "startColumnIndex": 0,
                        "endColumnIndex": col_end,
                    },
                }
            }
        }
    )

    with suppress(gspread.exceptions.GSpreadException, ValueError, TypeError):
        worksheet.spreadsheet.batch_update({"requests": requests})

    return {
        "table_name": table_name,
        "sheet_name": tab.name,
        "sheet_id": worksheet.id,
        "range_a1": f"A1:{_col_letter(col_end - 1)}{row_end}",
        "row_end": row_end,
        "col_end": col_end,
    }


def _apply_computed_columns(worksheet, tab: Tab):
    """Inserta ARRAYFORMULAs para columnas computadas."""
    if not tab.computed_columns:
        return
    
    # Primero, agregar headers de columnas computadas
    base_col_count = len(tab.columns)
    for idx, comp in enumerate(tab.computed_columns):
        col_letter = _col_letter(base_col_count + idx)
        # Header
        worksheet.update_acell(f"{col_letter}1", comp.name)
        # ARRAYFORMULA en fila 2
        worksheet.update_acell(f"{col_letter}2", f"=ARRAYFORMULA({comp.formula})")


def _apply_auto_filter(worksheet, tab: Tab):
    """Activa el filtro automatico en los headers."""
    if tab.filter and tab.filter.enabled:
        with suppress(gspread.exceptions.GSpreadException, ValueError, TypeError):
            worksheet.set_basic_filter()


def _apply_formulas(worksheet, tab: Tab):
    """Aplica formulas de columnas (auto_formula) a la fila 2."""
    for col_idx, col in enumerate(tab.columns):
        if col.auto and col.auto_formula:
            col_letter = _col_letter(col_idx)
            worksheet.update_acell(f"{col_letter}2", col.auto_formula)


def _hide_columns(worksheet, tab: Tab):
    """Oculta columnas marcadas como hidden."""
    for col_idx, col in enumerate(tab.columns):
        if col.hidden:
            body = {
                "requests": [{
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": worksheet.id,
                            "dimension": "COLUMNS",
                            "startIndex": col_idx,
                            "endIndex": col_idx + 1,
                        },
                        "properties": {"hiddenByUser": True},
                        "fields": "hiddenByUser",
                    }
                }]
            }
            with suppress(gspread.exceptions.GSpreadException, ValueError, TypeError):
                worksheet.spreadsheet.batch_update(body)


def _protect_columns(worksheet, tab: Tab):
    """
    Protege:
    1. SIEMPRE la fila de headers (row 1) - sin headers el sistema se rompe
    2. Columnas con edit_mode=READONLY -> warning al editar
    3. Columnas con edit_mode=LOCKED -> bloqueo total
    """
    requests = []
    
    # 1. SIEMPRE proteger fila de headers
    requests.append({
        "addProtectedRange": {
            "protectedRange": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,  # Solo fila 1 (headers)
                },
                "description": f"SpreedSQL: Headers de {tab.name} (no editar)",
                "warningOnly": False,  # Hard lock en headers
            }
        }
    })
    
    # 2. Proteger columnas segun edit_mode
    for col_idx, col in enumerate(tab.columns):
        if col.edit_mode == EditMode.READONLY:
            requests.append({
                "addProtectedRange": {
                    "protectedRange": {
                        "range": {
                            "sheetId": worksheet.id,
                            "startColumnIndex": col_idx,
                            "endColumnIndex": col_idx + 1,
                            "startRowIndex": 1,
                        },
                        "description": f"SpreedSQL: {col.name} (readonly)",
                        "warningOnly": True,
                    }
                }
            })
        elif col.edit_mode == EditMode.LOCKED:
            requests.append({
                "addProtectedRange": {
                    "protectedRange": {
                        "range": {
                            "sheetId": worksheet.id,
                            "startColumnIndex": col_idx,
                            "endColumnIndex": col_idx + 1,
                            "startRowIndex": 1,
                        },
                        "description": f"SpreedSQL: {col.name} (locked)",
                        "warningOnly": False,
                    }
                }
            })
    
    if requests:
        with suppress(gspread.exceptions.GSpreadException, ValueError, TypeError):
            worksheet.spreadsheet.batch_update({"requests": requests})


def _build_tab(spreadsheet: gspread.Spreadsheet, tab: Tab, is_first: bool = False, schema_obj=None):
    """Construye una pestaña completa: headers, validaciones, formulas, filtros."""
    if is_first:
        worksheet = spreadsheet.get_worksheet(0)
        worksheet.update_title(tab.name)
    else:
        total_cols = len(tab.columns)
        if tab.computed_columns:
            total_cols += len(tab.computed_columns)
        worksheet = spreadsheet.add_worksheet(
            title=tab.name, rows=1000, cols=max(total_cols, 10)
        )
    
    # 1. Escribir headers
    worksheet.update(values=[tab.headers], range_name="A1")
    
    # 2. Formato de headers
    _apply_column_formatting(worksheet, tab)
    
    # 3. Validaciones de datos (dropdowns, FKs, etc.)
    _apply_data_validations(worksheet, tab, spreadsheet_obj=schema_obj)
    
    # 4. Formulas automaticas
    _apply_formulas(worksheet, tab)
    
    # 5. Columnas computadas (ARRAYFORMULA)
    _apply_computed_columns(worksheet, tab)
    
    table_meta = _apply_native_table_features(worksheet, tab)

    # 6. Auto-filtro
    _apply_auto_filter(worksheet, tab)
    
    # 7. Ocultar columnas
    _hide_columns(worksheet, tab)
    
    # 8. Proteger columnas readonly/locked
    _protect_columns(worksheet, tab)
    
    return worksheet, table_meta


def _build_pivot_tab(spreadsheet: gspread.Spreadsheet, pivot, schema):
    """
    Crea una pestaña con una tabla dinamica simulada via QUERY().
    Google Sheets API v4 soporta pivots nativos, pero QUERY() es mas portable.
    """
    source_tab = schema.get_tab(pivot.source_tab)
    
    worksheet = spreadsheet.add_worksheet(
        title=pivot.name, rows=100, cols=20
    )
    
    # Construir QUERY formula
    row_cols = ", ".join(
        [_col_letter(source_tab.get_column_index(f.source_column) - 1) for f in pivot.rows]
    )
    
    value_parts = []
    for v in pivot.values:
        col_letter = _col_letter(source_tab.get_column_index(v.source_column) - 1)
        func = v.summarize or "COUNT"
        value_parts.append(f"{func}({col_letter})")
    
    select_clause = f"{row_cols}, {', '.join(value_parts)}"
    group_clause = f"GROUP BY {row_cols}"
    
    where_clause = ""
    if pivot.filter_column and pivot.filter_values:
        filter_col = _col_letter(source_tab.get_column_index(pivot.filter_column) - 1)
        vals = "' OR ".join([f"{filter_col} = '{v}" for v in pivot.filter_values])
        where_clause = f"WHERE {vals}'"
    
    query = f"SELECT {select_clause} {where_clause} {group_clause}"
    query_formula = f'=QUERY(\'{pivot.source_tab}\'!A:Z, "{query}", 1)'
    
    worksheet.update_acell("A1", query_formula)
    
    return worksheet


def execute_schema(
    schema: Spreadsheet,
    admin_email: str,
    credentials_path: Optional[str] = None,
    credentials_dict: Optional[dict] = None,
    gc: Optional[gspread.Client] = None,
    clinic_id: Optional[str] = None,
    return_metadata: bool = False,
    **name_vars,
) -> tuple:
    """
    Ejecuta un esquema SpreedSQL contra Google Sheets API.
    Crea el spreadsheet completo con todas las pestañas, validaciones,
    formulas, filtros, protecciones y tablas dinamicas.
    
    Auth (usar UNO):
      - credentials_path: ruta al JSON de service account
      - credentials_dict: dict del JSON (ej: json.loads(os.getenv('GSHEETS_CREDS')))
      - gc: cliente gspread pre-autenticado
    
        Retorna:
            - default: (sheet_id, sheet_url, apps_script_code)
            - con return_metadata=True: (sheet_id, sheet_url, apps_script_code, metadata)
    """
    client = _get_gc(credentials_path=credentials_path, credentials_dict=credentials_dict, gc=gc)
    
    # Resolver nombre
    resolved_name = schema.resolve_name(**name_vars)
    sh = client.create(resolved_name)
    sh.share(admin_email, perm_type="user", role="writer")
    
    table_metadata = {"tables": []}

    # Construir todas las pestañas
    for idx, tab in enumerate(schema.tabs):
        _, table_meta = _build_tab(sh, tab, is_first=(idx == 0), schema_obj=schema)
        if table_meta:
            table_metadata["tables"].append(table_meta)
    
    # Construir tablas dinamicas
    for pivot in schema.pivot_tables:
        _build_pivot_tab(sh, pivot, schema)
    
    # Generar Apps Script con clinic_id real si se proporciona
    apps_script = schema.gen_apps_script(clinic_id=clinic_id or "CLINIC_ID_PLACEHOLDER")
    
    if return_metadata:
        return sh.id, sh.url, apps_script, table_metadata
    return sh.id, sh.url, apps_script


def execute_schema_on_existing(
    schema: 'Spreadsheet',
    sheet_id: str,
    credentials_path: Optional[str] = None,
    credentials_dict: Optional[dict] = None,
    gc: Optional[gspread.Client] = None,
    clinic_id: Optional[str] = None,
    return_metadata: bool = False,
) -> tuple:
    """
    Escribe un esquema SpreedSQL sobre un spreadsheet existente.
    Borra las hojas previas y recrea todo el schema.
    Util cuando la cuota de creacion de Drive esta llena.
    
        Retorna:
            - default: (sheet_id, sheet_url, apps_script_code)
            - con return_metadata=True: (sheet_id, sheet_url, apps_script_code, metadata)
    """
    client = _get_gc(credentials_path=credentials_path, credentials_dict=credentials_dict, gc=gc)
    sh = client.open_by_key(sheet_id)
    
    # Borrar todas las hojas existentes (excepto la primera, que se reutiliza)
    existing_worksheets = sh.worksheets()
    for ws in existing_worksheets[1:]:
        sh.del_worksheet(ws)
    
    table_metadata = {"tables": []}

    # Construir todas las pestañas
    for idx, tab in enumerate(schema.tabs):
        _, table_meta = _build_tab(sh, tab, is_first=(idx == 0), schema_obj=schema)
        if table_meta:
            table_metadata["tables"].append(table_meta)
    
    # Construir tablas dinamicas
    for pivot in schema.pivot_tables:
        _build_pivot_tab(sh, pivot, schema)
    
    # Generar Apps Script
    apps_script = schema.gen_apps_script(clinic_id=clinic_id or "CLINIC_ID_PLACEHOLDER")
    
    if return_metadata:
        return sh.id, sh.url, apps_script, table_metadata
    return sh.id, sh.url, apps_script


# ============================================================
#  LECTURA - SpreedSQL tambien lee, no solo construye
# ============================================================

def read_tab(
    schema: Spreadsheet,
    tab_name: str,
    sheet_id: str,
    credentials_path: Optional[str] = None,
    credentials_dict: Optional[dict] = None,
    gc: Optional[gspread.Client] = None,
) -> list:
    """
    Lee los datos de una pestaña usando el schema para validar headers.
    Retorna lista de dicts (como get_all_records pero validado contra schema).
    
    Uso:
        citas = read_tab(clinic_schema, "Citas", sheet_id="1ABC...")
        for cita in citas:
            print(cita["Doctor"], cita["Estado"])
    """
    client = _get_gc(credentials_path=credentials_path, credentials_dict=credentials_dict, gc=gc)
    sh = client.open_by_key(sheet_id)
    
    tab = schema.get_tab(tab_name)
    
    try:
        ws = sh.worksheet(tab_name)
    except Exception as exc:
        raise ValueError(f"Pestaña '{tab_name}' no encontrada en el spreadsheet.") from exc
    
    # Validar headers
    actual_headers = ws.row_values(1)
    expected = tab.headers
    missing = [h for h in expected if h not in actual_headers]
    if missing:
        raise ValueError(f"Headers faltantes en '{tab_name}': {missing}")
    
    return ws.get_all_records()


def read_all(
    schema: Spreadsheet,
    sheet_id: str,
    credentials_path: Optional[str] = None,
    credentials_dict: Optional[dict] = None,
    gc: Optional[gspread.Client] = None,
) -> dict:
    """
    Lee todas las pestañas definidas en el schema.
    Retorna dict {tab_name: [records]}.
    
    Uso:
        data = read_all(clinic_schema, sheet_id="1ABC...")
        for cita in data["Citas"]: ...
        for doc in data["Info_Doctores"]: ...
    """
    client = _get_gc(credentials_path=credentials_path, credentials_dict=credentials_dict, gc=gc)
    sh = client.open_by_key(sheet_id)
    
    result = {}
    for tab in schema.tabs:
        try:
            ws = sh.worksheet(tab.name)
            result[tab.name] = ws.get_all_records()
        except gspread.exceptions.WorksheetNotFound:
            result[tab.name] = []
    
    return result
