"""
SpreedSQL Builder - Ejecuta un esquema contra la API de Google Sheets.
Equivale a psycopg2.execute(open('schema.sql')) pero para spreadsheets.
"""
import gspread
from google.oauth2.service_account import Credentials
from typing import Optional, Union
from .models import Spreadsheet, Tab, Column, DataType, EditMode


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _get_gc(
    credentials_path: str = None,
    credentials_dict: dict = None,
    gc: gspread.Client = None,
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


def _apply_column_formatting(worksheet, tab: Tab):
    """Aplica formato a los headers (negrita, colores, anchos)."""
    num_cols = len(tab.columns)
    end_col = _col_letter(num_cols - 1)
    
    # Headers en negrita
    worksheet.format(f"A1:{end_col}1", {
        "textFormat": {"bold": True},
        "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.95},
    })
    
    # Congelar filas/columnas
    worksheet.freeze(rows=tab.freeze_rows, cols=tab.freeze_cols)


def _apply_data_validations(worksheet, tab: Tab, spreadsheet_obj=None):
    """Aplica reglas de validacion de datos (dropdowns ENUM, ForeignKeys, etc.)."""
    for col_idx, col in enumerate(tab.columns):
        col_letter = _col_letter(col_idx)
        cell_range = f"{col_letter}2:{col_letter}1000"
        
        # ForeignKey: dropdown con valores de otra pestaña
        if col.foreign_key:
            fk = col.foreign_key
            # Resolver la columna referenciada para obtener su letra
            if spreadsheet_obj:
                try:
                    ref_tab = None
                    for t in spreadsheet_obj.tabs if hasattr(spreadsheet_obj, 'tabs') else []:
                        if t.name == fk.tab:
                            ref_tab = t
                            break
                    
                    if ref_tab:
                        fk_col_idx = ref_tab.get_column_index(fk.column) - 1
                        fk_col_letter = _col_letter(fk_col_idx)
                    else:
                        fk_col_letter = "A"  # Fallback
                except Exception:
                    fk_col_letter = "A"
            else:
                fk_col_letter = "A"
            
            # Crear regla de validacion con rango de la pestaña referenciada
            fk_range = f"'{fk.tab}'!{fk_col_letter}2:{fk_col_letter}"
            try:
                rule = gspread.cell.DataValidationRule(
                    gspread.cell.BooleanCondition("ONE_OF_RANGE", [fk_range]),
                    showCustomUi=True,
                    strict=not fk.allow_blank,
                )
                worksheet.set_data_validation(cell_range, rule)
            except Exception:
                pass
        
        # ENUM: dropdown con valores fijos
        elif col.dtype == DataType.ENUM and col.values:
            try:
                rule = gspread.cell.DataValidationRule(
                    gspread.cell.BooleanCondition("ONE_OF_LIST", col.values),
                    showCustomUi=True,
                    strict=True,
                )
                worksheet.set_data_validation(cell_range, rule)
            except Exception:
                pass


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
        try:
            worksheet.set_basic_filter()
        except Exception:
            pass


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
            try:
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
                worksheet.spreadsheet.batch_update(body)
            except Exception:
                pass


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
        try:
            worksheet.spreadsheet.batch_update({"requests": requests})
        except Exception:
            pass


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
            title=tab.name, rows="1000", cols=str(max(total_cols, 10))
        )
    
    # 1. Escribir headers
    worksheet.update("A1", [tab.headers])
    
    # 2. Formato de headers
    _apply_column_formatting(worksheet, tab)
    
    # 3. Validaciones de datos (dropdowns, FKs, etc.)
    _apply_data_validations(worksheet, tab, spreadsheet_obj=schema_obj)
    
    # 4. Formulas automaticas
    _apply_formulas(worksheet, tab)
    
    # 5. Columnas computadas (ARRAYFORMULA)
    _apply_computed_columns(worksheet, tab)
    
    # 6. Auto-filtro
    _apply_auto_filter(worksheet, tab)
    
    # 7. Ocultar columnas
    _hide_columns(worksheet, tab)
    
    # 8. Proteger columnas readonly/locked
    _protect_columns(worksheet, tab)
    
    return worksheet


def _build_pivot_tab(spreadsheet: gspread.Spreadsheet, pivot, schema):
    """
    Crea una pestaña con una tabla dinamica simulada via QUERY().
    Google Sheets API v4 soporta pivots nativos, pero QUERY() es mas portable.
    """
    source_tab = schema.get_tab(pivot.source_tab)
    
    worksheet = spreadsheet.add_worksheet(
        title=pivot.name, rows="100", cols="20"
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
    credentials_path: str = None,
    credentials_dict: dict = None,
    gc: gspread.Client = None,
    clinic_id: str = None,
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
    
    Retorna: (sheet_id, sheet_url, apps_script_code)
    """
    client = _get_gc(credentials_path=credentials_path, credentials_dict=credentials_dict, gc=gc)
    
    # Resolver nombre
    resolved_name = schema.resolve_name(**name_vars)
    sh = client.create(resolved_name)
    sh.share(admin_email, perm_type="user", role="writer")
    
    # Construir todas las pestañas
    for idx, tab in enumerate(schema.tabs):
        _build_tab(sh, tab, is_first=(idx == 0), schema_obj=schema)
    
    # Construir tablas dinamicas
    for pivot in schema.pivot_tables:
        _build_pivot_tab(sh, pivot, schema)
    
    # Generar Apps Script con clinic_id real si se proporciona
    apps_script = schema.gen_apps_script(clinic_id=clinic_id or "CLINIC_ID_PLACEHOLDER")
    
    return sh.id, sh.url, apps_script


# ============================================================
#  LECTURA - SpreedSQL tambien lee, no solo construye
# ============================================================

def read_tab(
    schema: Spreadsheet,
    tab_name: str,
    sheet_id: str,
    credentials_path: str = None,
    credentials_dict: dict = None,
    gc: gspread.Client = None,
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
    except gspread.exceptions.WorksheetNotFound:
        raise ValueError(f"Pestaña '{tab_name}' no encontrada en el spreadsheet.")
    
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
    credentials_path: str = None,
    credentials_dict: dict = None,
    gc: gspread.Client = None,
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
