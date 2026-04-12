"""
SpreedSQL - Modelos declarativos para esquemas de Google Sheets.
Equivalente a los modelos de un ORM (SQLAlchemy) pero para hojas de calculo.
"""
from enum import Enum
from typing import List, Optional, Dict, Any, Union, cast
from pydantic import BaseModel, Field


class DataType(str, Enum):
    """Tipos de datos soportados por columnas de un spreadsheet."""
    TEXT = "TEXT"
    NUMBER = "NUMBER"
    INTEGER = "INTEGER"
    CURRENCY = "CURRENCY"         # Formateado como moneda (S/ 150.00)
    DATE = "DATE"                 # YYYY-MM-DD
    DATETIME = "DATETIME"         # YYYY-MM-DD HH:MM:SS
    TIME = "TIME"                 # HH:MM
    BOOLEAN = "BOOLEAN"           # TRUE/FALSE o checkbox
    ENUM = "ENUM"                 # Lista desplegable con valores fijos
    URL = "URL"
    EMAIL = "EMAIL"
    PHONE = "PHONE"               # Formato telefono peruano (+51 xxx)
    UUID = "UUID"                 # Para IDs de sincronizacion con DB


class EditMode(str, Enum):
    """Nivel de proteccion del contenido de una columna (headers siempre protegidos)."""
    OPEN = "open"           # Cualquier editor puede modificar la celda
    READONLY = "readonly"   # Warning al editar (recepcionista puede override)
    LOCKED = "locked"       # Bloqueada: solo bot/API/formulas pueden escribir


class FormatRule(BaseModel):
    """Regla de formato condicional para una columna."""
    condition: str                # Ej: "equals", "contains", "greater_than"
    value: str                    # Valor de comparacion
    bg_color: Optional[str] = None  # Color de fondo hex (#FF0000)
    text_color: Optional[str] = None  # Color de texto hex


class TableTheme(BaseModel):
    """Tema visual para una tabla nativa de Google Sheets."""
    header_bg: str = "#DDE6F9"
    header_text: str = "#1F1F1F"
    stripe_bg: str = "#F6F8FC"


class Formula(BaseModel):
    """
    Formula de Google Sheets aplicable a una columna.
    Se aplica automaticamente a cada fila nueva.
    Soporta funciones nativas de Sheets: SUM, VLOOKUP, IF, QUERY, etc.
    """
    expression: str               # Formula (ej: "=SUM(B2:B)", "=IF(F2=\"Cancelado\",TRUE,FALSE)")
    array_formula: bool = False   # Si es ARRAYFORMULA que se pone solo en fila 2
    description: Optional[str] = None


class Filter(BaseModel):
    """Filtro automatico aplicado a una pestaña."""
    enabled: bool = True          # Activar auto-filtro en los headers
    default_column: Optional[str] = None  # Columna con filtro activo por defecto
    default_values: Optional[List[str]] = None  # Valores visibles por defecto


class SortSpec(BaseModel):
    """Especificacion de ordenamiento por defecto."""
    column: str                   # Nombre de la columna
    ascending: bool = True        # Ascendente o descendente


class PivotField(BaseModel):
    """Campo dentro de una tabla dinamica."""
    source_column: str            # Columna de origen en la pestaña fuente
    summarize: Optional[str] = None  # Funcion de resumen: SUM, COUNT, AVERAGE, MAX, MIN, COUNTA


class PivotTable(BaseModel):
    """
    Definicion de tabla dinamica (pivot table) que se genera en una pestaña separada.
    Equivale a un CREATE VIEW ... GROUP BY en SQL.
    """
    name: str                     # Nombre de la pestaña donde se renderiza el pivot
    source_tab: str               # Pestaña de datos fuente
    rows: List[PivotField]        # Campos de fila (GROUP BY)
    columns: Optional[List[PivotField]] = None  # Campos de columna (pivot horizontal)
    values: List[PivotField]      # Campos de valor (SUM, COUNT, etc.)
    filter_column: Optional[str] = None  # Columna de filtro opcional
    filter_values: Optional[List[str]] = None  # Valores del filtro


class ComputedColumn(BaseModel):
    """
    Columna calculada: no la llena el usuario, sino una formula automatica.
    Se inserta como ARRAYFORMULA en la fila 2 de la columna correspondiente.
    """
    name: str                     # Header de la columna
    formula: str                  # ARRAYFORMULA expression (sin el = inicial)
    dtype: DataType = DataType.TEXT
    description: Optional[str] = None


class ForeignKey(BaseModel):
    """
    Referencia a otra pestaña + columna, como un FK en SQL.
    Se implementa como data validation con rango dinámico =Tab!Col2:Col.
    
    Ejemplo:
        Column("Doctor", DataType.TEXT, foreign_key=ForeignKey(tab="Info_Doctores", column="Nombre"))
        # -> crea un dropdown en Google Sheets con los valores de Info_Doctores!A2:A
    """
    tab: str                      # Nombre de la pestaña referenciada
    column: str                   # Nombre de la columna referenciada
    allow_blank: bool = True      # Permitir valores vacíos (ON DELETE SET NULL)


class Column(BaseModel):
    """
    Definicion de una columna en una pestaña del spreadsheet.
    Equivale a una columna en un CREATE TABLE de SQL.
    """
    model_config = {"populate_by_name": True}

    name: str                                   # Nombre del header (ej: "WhatsApp")
    dtype: DataType = DataType.TEXT              # Tipo de dato
    required: bool = False                      # Si el valor es obligatorio
    hidden: bool = False                        # Columna oculta (para IDs internos)
    default: Optional[str] = None               # Valor por defecto
    values: Optional[Union[List[str], str]] = None  # ENUM: lista fija o referencia "Tab.Columna"
    enum_allow_custom: bool = False             # Si True, permite escribir fuera del dropdown
    auto: bool = False                          # Auto-rellenar (timestamps, IDs)
    auto_formula: Optional[str] = None          # Formula de Google Sheets si auto=True
    width: Optional[int] = None                 # Ancho en pixeles (None = auto)
    description: Optional[str] = None           # Nota/tooltip para el header
    format_rules: Optional[List[FormatRule]] = None  # Formato condicional
    foreign_key: Optional[ForeignKey] = None    # FK: referencia a otra pestaña/columna
    edit_mode: EditMode = EditMode.OPEN         # Proteccion: open | readonly | locked

    def __init__(self, name: Optional[str] = None, dtype: DataType = DataType.TEXT, **kwargs):
        super().__init__(name=name, dtype=dtype, **kwargs)

    def to_validation_dict(self) -> Optional[Dict[str, Any]]:
        """Genera la regla de validacion de datos de Google Sheets."""
        if self.dtype == DataType.ENUM and isinstance(self.values, list) and self.values:
            values = [str(v) for v in cast(List[str], self.values or [])]
            return {
                "type": "ONE_OF_LIST",
                "values": [{"userEnteredValue": v} for v in values],
                "strict": not self.enum_allow_custom,
                "showCustomUi": True
            }
        elif self.dtype == DataType.BOOLEAN:
            return {"type": "BOOLEAN"}
        elif self.dtype == DataType.NUMBER or self.dtype == DataType.INTEGER:
            return {"type": "NUMBER_BETWEEN", "values": [{"userEnteredValue": "-999999"}, {"userEnteredValue": "999999"}]}
        elif self.dtype == DataType.EMAIL:
            return {"type": "TEXT_IS_VALID_EMAIL"}
        elif self.dtype == DataType.URL:
            return {"type": "TEXT_IS_VALID_URL"}
        return None


class TriggerType(str, Enum):
    """Tipos de trigger soportados."""
    ON_EDIT = "on_edit"           # Se dispara al editar una columna específica
    ON_CHANGE = "on_change"       # Se dispara al cambiar cualquier celda de la pestaña
    ON_SCHEDULE = "on_schedule"   # Se dispara por un cron/timer (Google Triggers)
    ON_FORM_SUBMIT = "on_form_submit"  # Se dispara al recibir respuesta de Google Forms
    ON_THRESHOLD = "on_threshold" # Se dispara cuando un valor supera un umbral


class Trigger(BaseModel):
    """
    Trigger genérico que se activa bajo ciertas condiciones.
    Genera automáticamente el código de Google Apps Script.
    """
    type: TriggerType                               # Tipo de trigger
    webhook_url: str                                # URL del webhook destino
    event_name: str = "sheet.event"                 # Nombre del evento en el payload
    column: Optional[str] = None                    # Columna que dispara (ON_EDIT, ON_THRESHOLD)
    include_columns: Optional[List[str]] = None     # Columnas extra a incluir en el payload
    # ON_SCHEDULE fields
    cron_expression: Optional[str] = None           # Ej: "0 9 * * 1-5" (L-V a las 9am)
    # ON_THRESHOLD fields
    threshold_value: Optional[float] = None         # Valor umbral
    threshold_direction: Optional[str] = None       # "above" o "below"
    description: Optional[str] = None               # Descripción humana del trigger


# Alias de conveniencia para crear triggers rápido
def OnEdit(column: str, webhook_url: str, event_name: str = "sheet.row.update",
          include_columns: Optional[List[str]] = None) -> Trigger:
    """Crea un trigger ON_EDIT para una columna específica."""
    return Trigger(
        type=TriggerType.ON_EDIT,
        column=column,
        webhook_url=webhook_url,
        event_name=event_name,
        include_columns=include_columns,
    )

def OnChange(webhook_url: str, event_name: str = "sheet.any.change",
            include_columns: Optional[List[str]] = None) -> Trigger:
    """Crea un trigger ON_CHANGE para cualquier edición en la pestaña."""
    return Trigger(
        type=TriggerType.ON_CHANGE,
        webhook_url=webhook_url,
        event_name=event_name,
        include_columns=include_columns,
    )

def OnSchedule(webhook_url: str, cron_expression: str,
              event_name: str = "sheet.scheduled", description: Optional[str] = None) -> Trigger:
    """Crea un trigger ON_SCHEDULE basado en cron (Time-driven Google trigger)."""
    return Trigger(
        type=TriggerType.ON_SCHEDULE,
        webhook_url=webhook_url,
        cron_expression=cron_expression,
        event_name=event_name,
        description=description,
    )

def OnFormSubmit(webhook_url: str, event_name: str = "sheet.form.submit",
               include_columns: Optional[List[str]] = None) -> Trigger:
    """Crea un trigger ON_FORM_SUBMIT para Google Forms vinculados."""
    return Trigger(
        type=TriggerType.ON_FORM_SUBMIT,
        webhook_url=webhook_url,
        event_name=event_name,
        include_columns=include_columns,
    )

def OnThreshold(column: str, webhook_url: str, threshold_value: float,
               direction: str = "above", event_name: str = "sheet.threshold",
               include_columns: Optional[List[str]] = None) -> Trigger:
    """Crea un trigger ON_THRESHOLD que se dispara cuando un valor supera/baja de un umbral."""
    return Trigger(
        type=TriggerType.ON_THRESHOLD,
        column=column,
        webhook_url=webhook_url,
        threshold_value=threshold_value,
        threshold_direction=direction,
        event_name=event_name,
        include_columns=include_columns,
    )


class Tab(BaseModel):
    """
    Definicion de una pestaña (worksheet) del spreadsheet.
    Equivale a un CREATE TABLE en SQL.
    """
    name: str                                   # Nombre de la pestaña
    columns: List[Column]                       # Lista de columnas (orden = orden en sheet)
    triggers: List[Trigger] = Field(default_factory=list)  # Triggers (onEdit, onChange, etc.)
    freeze_rows: int = 1                        # Filas congeladas (1 = solo header)
    freeze_cols: int = 0                        # Columnas congeladas
    color: Optional[str] = None                 # Color de la pestaña (hex)
    protected: bool = False                     # Proteger pestaña (solo lectura para no-owners)
    filter: Optional[Filter] = None             # Auto-filtro en headers
    sorts: Optional[List[SortSpec]] = None      # Ordenamiento por defecto
    computed_columns: Optional[List[ComputedColumn]] = None  # Columnas calculadas con ARRAYFORMULA
    is_native_table: bool = False               # Simula tabla nativa: filter + banding + named range
    table_name: Optional[str] = None            # Nombre del named range (ej: Tabla_1)
    table_rows: int = 1000                      # Alto del rango de tabla para filter/banding
    table_theme: Optional[TableTheme] = None    # Tema visual para la tabla

    @property
    def headers(self) -> List[str]:
        """Retorna la lista de nombres de columnas (headers de la fila 1)."""
        return [col.name for col in self.columns]

    @property
    def visible_headers(self) -> List[str]:
        """Headers que el usuario ve (excluye hidden)."""
        return [col.name for col in self.columns if not col.hidden]

    def get_column_index(self, col_name: str) -> int:
        """Retorna el indice 1-based de una columna por nombre."""
        for idx, col in enumerate(self.columns):
            if col.name == col_name:
                return idx + 1
        raise ValueError(f"Columna '{col_name}' no encontrada en pestaña '{self.name}'")


class Spreadsheet(BaseModel):
    """
    Raiz del esquema de un spreadsheet completo.
    Equivale a un archivo schema.sql con multiples CREATE TABLE.
    """
    name_template: str                          # Template del nombre (ej: "Gestion_Clinica_{name}")
    tabs: List[Tab] = Field(default_factory=list)
    pivot_tables: List[PivotTable] = Field(default_factory=list)  # Tablas dinamicas/vistas

    def tab(
        self,
        name: str,
        *columns: Column,
        triggers: Optional[List[Trigger]] = None,
        freeze_rows: int = 1,
        freeze_cols: int = 0,
        color: Optional[str] = None,
        protected: bool = False,
        tab_filter: Optional[Filter] = None,
        sorts: Optional[List[SortSpec]] = None,
        computed_columns: Optional[List[ComputedColumn]] = None,
        is_native_table: bool = False,
        table_name: Optional[str] = None,
        table_rows: int = 1000,
        table_theme: Optional[TableTheme] = None,
        **kwargs,
    ) -> "Spreadsheet":
        """
        API fluida para agregar pestañas al esquema.
        Uso: schema.tab("Citas", Column(...), ..., triggers=[OnEdit(...), OnChange(...)])
        """
        legacy_filter = kwargs.pop("filter", None)
        if kwargs:
            unknown = ", ".join(kwargs.keys())
            raise TypeError(f"Argumentos no soportados en tab(): {unknown}")

        resolved_filter = tab_filter if tab_filter is not None else legacy_filter

        t = Tab(
            name=name,
            columns=list(columns),
            triggers=triggers or [],
            freeze_rows=freeze_rows,
            freeze_cols=freeze_cols,
            color=color,
            protected=protected,
            filter=resolved_filter,
            sorts=sorts,
            computed_columns=computed_columns,
            is_native_table=is_native_table,
            table_name=table_name,
            table_rows=table_rows,
            table_theme=table_theme,
        )
        tabs = cast(List[Tab], self.tabs)
        tabs.append(t)
        self.tabs = tabs
        return self  # Permite encadenamiento

    def get_tab(self, name: str) -> Tab:
        """Busca una pestaña por nombre."""
        for t in self.tabs:
            if t.name == name:
                return t
        raise ValueError(f"Pestaña '{name}' no encontrada en esquema '{self.name_template}'")

    def resolve_name(self, **kwargs) -> str:
        """Resuelve el template de nombre con variables."""
        return self.name_template.format(**kwargs)

    def to_dict(self) -> dict:
        """Serializa el esquema completo a dict (para guardar en JSONB)."""
        return self.model_dump()

    def gen_apps_script(self, clinic_id: str = "CLINIC_ID_PLACEHOLDER") -> str:
        """
        Genera el codigo completo de Google Apps Script con todos los 
        triggers onEdit definidos en las pestañas.
        """
        from .apps_script import generate_apps_script
        return generate_apps_script(self, clinic_id)
