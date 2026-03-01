"""
SpreedSQL Validator - Valida que un spreadsheet existente cumpla con el esquema.
Equivale a un linter de migraciones: detecta tabs faltantes, columnas renombradas, etc.
"""
import gspread
from google.oauth2.service_account import Credentials
from typing import List, Dict
from .models import Spreadsheet, Tab

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class ValidationError:
    """Un error de validacion individual."""
    def __init__(self, level: str, tab: str, message: str):
        self.level = level  # "ERROR" | "WARNING"
        self.tab = tab
        self.message = message

    def __repr__(self):
        return f"[{self.level}] {self.tab}: {self.message}"


class ValidationResult:
    """Resultado de la validacion completa."""
    def __init__(self):
        self.errors: List[ValidationError] = []
    
    @property
    def is_valid(self) -> bool:
        return not any(e.level == "ERROR" for e in self.errors)
    
    @property
    def warnings(self) -> List[ValidationError]:
        return [e for e in self.errors if e.level == "WARNING"]
    
    def add_error(self, tab: str, message: str):
        self.errors.append(ValidationError("ERROR", tab, message))
    
    def add_warning(self, tab: str, message: str):
        self.errors.append(ValidationError("WARNING", tab, message))
    
    def summary(self) -> str:
        if self.is_valid and not self.warnings:
            return "✅ Schema valido. Todas las pestañas y columnas coinciden."
        
        lines = []
        for e in self.errors:
            icon = "❌" if e.level == "ERROR" else "⚠️"
            lines.append(f"{icon} [{e.tab}] {e.message}")
        
        status = "❌ INVALIDO" if not self.is_valid else "⚠️ VALIDO CON ADVERTENCIAS"
        lines.insert(0, f"{status} ({len(self.errors)} problema(s) encontrado(s))")
        return "\n".join(lines)


def validate_schema(
    schema: Spreadsheet,
    sheet_id: str,
    credentials_path: str = "credentials.json",
) -> ValidationResult:
    """
    Valida un spreadsheet existente contra el esquema SpreedSQL.
    Detecta: tabs faltantes, columnas faltantes, columnas extra, orden incorrecto.
    
    Uso:
        result = validate_schema(clinic_schema, "1ABC...xyz")
        print(result.summary())
        if not result.is_valid:
            raise Exception("Schema mismatch!")
    """
    creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
    gc = gspread.authorize(creds)
    
    result = ValidationResult()
    
    try:
        sh = gc.open_by_key(sheet_id)
    except gspread.exceptions.SpreadsheetNotFound:
        result.add_error("GLOBAL", f"Spreadsheet con ID '{sheet_id}' no encontrado.")
        return result
    
    existing_tabs = {ws.title: ws for ws in sh.worksheets()}
    
    # Validar cada tab definida en el schema
    for tab in schema.tabs:
        if tab.name not in existing_tabs:
            result.add_error(tab.name, f"Pestaña '{tab.name}' no existe en el spreadsheet.")
            continue
        
        ws = existing_tabs[tab.name]
        
        # Leer headers existentes
        try:
            existing_headers = ws.row_values(1)
        except Exception:
            result.add_error(tab.name, "No se pudo leer la fila de headers.")
            continue
        
        expected_headers = tab.headers
        
        # Computed columns tambien deben tener header
        if tab.computed_columns:
            expected_headers += [cc.name for cc in tab.computed_columns]
        
        # Verificar headers faltantes
        for expected in expected_headers:
            if expected not in existing_headers:
                result.add_error(tab.name, f"Columna '{expected}' faltante.")
        
        # Verificar headers extra (que no estan en el schema)
        all_expected = set(expected_headers)
        for existing in existing_headers:
            if existing and existing not in all_expected:
                result.add_warning(tab.name, f"Columna extra '{existing}' no definida en el schema.")
        
        # Verificar orden de columnas
        for idx, expected_name in enumerate(tab.headers):
            if idx < len(existing_headers):
                if existing_headers[idx] != expected_name:
                    result.add_warning(
                        tab.name,
                        f"Columna en posición {idx+1}: esperada '{expected_name}', encontrada '{existing_headers[idx]}'."
                    )
    
    # Verificar tabs extra en el spreadsheet
    schema_tab_names = set(t.name for t in schema.tabs)
    pivot_names = set(p.name for p in schema.pivot_tables)
    all_schema_names = schema_tab_names | pivot_names
    
    for existing_name in existing_tabs:
        if existing_name not in all_schema_names:
            result.add_warning(existing_name, f"Pestaña '{existing_name}' existe pero no está definida en el schema.")
    
    return result
