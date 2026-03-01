"""
SpreedSQL Apps Script Generator - Auto-genera codigo Google Apps Script
a partir de los triggers definidos en el esquema.
Soporta: ON_EDIT, ON_CHANGE, ON_THRESHOLD, ON_SCHEDULE, ON_FORM_SUBMIT.
"""
from .models import Spreadsheet, TriggerType


def generate_apps_script(schema: Spreadsheet, clinic_id: str = "CLINIC_ID_PLACEHOLDER") -> str:
    """
    Genera el codigo completo de Google Apps Script para inyectar en el
    spreadsheet. Incluye todos los triggers de todas las pestañas.
    """
    
    # Recolectar todos los triggers de todas las pestañas
    all_triggers = []
    for tab in schema.tabs:
        for trigger in tab.triggers:
            all_triggers.append((tab, trigger))
    
    if not all_triggers:
        return "// SpreedSQL: No se definieron triggers en este esquema.\n"
    
    unique_webhooks = list(set(t[1].webhook_url for t in all_triggers))
    
    lines = []
    lines.append("// =====================================================")
    lines.append("// AUTO-GENERADO POR SPREED_SQL - NO EDITAR MANUALMENTE")
    lines.append("// =====================================================")
    lines.append("")
    lines.append(f'const CLINIC_ID = "{clinic_id}";')
    lines.append("")
    
    for idx, url in enumerate(unique_webhooks):
        var_name = "WEBHOOK_URL" if len(unique_webhooks) == 1 else f"WEBHOOK_URL_{idx}"
        lines.append(f'const {var_name} = "{url}";')
    lines.append("")
    
    # --- Helper para enviar webhooks ---
    lines.append("function _sendWebhook(url, payload) {")
    lines.append('  var options = {"method": "post", "contentType": "application/json", "payload": JSON.stringify(payload)};')
    lines.append("  try { UrlFetchApp.fetch(url, options); }")
    lines.append('  catch(e) { Logger.log("SpreedSQL webhook error: " + e); }')
    lines.append("}")
    lines.append("")
    
    # --- onEdit handler ---
    edit_triggers = [(tab, t) for tab, t in all_triggers if t.type in (TriggerType.ON_EDIT, TriggerType.ON_THRESHOLD)]
    change_triggers = [(tab, t) for tab, t in all_triggers if t.type == TriggerType.ON_CHANGE]
    schedule_triggers = [(tab, t) for tab, t in all_triggers if t.type == TriggerType.ON_SCHEDULE]
    form_triggers = [(tab, t) for tab, t in all_triggers if t.type == TriggerType.ON_FORM_SUBMIT]
    
    if edit_triggers or change_triggers:
        lines.append("function onEdit(e) {")
        lines.append("  if (!e) return;")
        lines.append("  var sheet = e.source.getActiveSheet();")
        lines.append("  var sheetName = sheet.getName();")
        lines.append("  var range = e.range;")
        lines.append("  var row = range.getRow();")
        lines.append("  var col = range.getColumn();")
        lines.append("  if (row <= 1) return;")
        lines.append("")
        
        # ON_EDIT triggers (columna específica)
        for tab, trigger in edit_triggers:
            if not trigger.column:
                continue
            col_index = tab.get_column_index(trigger.column)
            wh_var = "WEBHOOK_URL" if len(unique_webhooks) == 1 else f"WEBHOOK_URL_{unique_webhooks.index(trigger.webhook_url)}"
            
            lines.append(f'  // Trigger [{trigger.type.value}]: {tab.name}.{trigger.column}')
            lines.append(f'  if (sheetName === "{tab.name}" && col === {col_index}) {{')
            
            # Build payload
            payload_parts = [
                f'"event": "{trigger.event_name}"',
                '"clinic_id": CLINIC_ID',
                f'"sheet": "{tab.name}"',
                f'"column": "{trigger.column}"',
                '"row": row',
                '"new_value": e.value',
                '"old_value": e.oldValue || ""',
                '"timestamp": new Date().toISOString()',
            ]
            
            if trigger.include_columns:
                for inc_col in trigger.include_columns:
                    inc_idx = tab.get_column_index(inc_col)
                    payload_parts.append(f'"{inc_col.lower()}": sheet.getRange(row, {inc_idx}).getValue()')
            
            # ON_THRESHOLD: add threshold check
            if trigger.type == TriggerType.ON_THRESHOLD and trigger.threshold_value is not None:
                direction = trigger.threshold_direction or "above"
                op = ">" if direction == "above" else "<"
                lines.append(f'    var cellValue = Number(e.value);')
                lines.append(f'    if (!(cellValue {op} {trigger.threshold_value})) return;')
                payload_parts.append(f'"threshold": {trigger.threshold_value}')
                payload_parts.append(f'"direction": "{direction}"')
            
            lines.append(f"    _sendWebhook({wh_var}, {{")
            for pp in payload_parts:
                lines.append(f"      {pp},")
            lines.append("    });")
            lines.append("  }")
            lines.append("")
        
        # ON_CHANGE triggers (cualquier celda)
        for tab, trigger in change_triggers:
            wh_var = "WEBHOOK_URL" if len(unique_webhooks) == 1 else f"WEBHOOK_URL_{unique_webhooks.index(trigger.webhook_url)}"
            
            lines.append(f'  // Trigger [on_change]: {tab.name} (any cell)')
            lines.append(f'  if (sheetName === "{tab.name}") {{')
            
            payload_parts = [
                f'"event": "{trigger.event_name}"',
                '"clinic_id": CLINIC_ID',
                f'"sheet": "{tab.name}"',
                '"row": row',
                '"col": col',
                '"new_value": e.value',
                '"timestamp": new Date().toISOString()',
            ]
            
            lines.append(f"    _sendWebhook({wh_var}, {{")
            for pp in payload_parts:
                lines.append(f"      {pp},")
            lines.append("    });")
            lines.append("  }")
            lines.append("")
        
        lines.append("}")
        lines.append("")
    
    # --- onFormSubmit handler ---
    if form_triggers:
        lines.append("function onFormSubmit(e) {")
        lines.append("  if (!e) return;")
        lines.append("  var sheet = e.range.getSheet();")
        lines.append("  var sheetName = sheet.getName();")
        lines.append("  var row = e.range.getRow();")
        lines.append("")
        
        for tab, trigger in form_triggers:
            wh_var = "WEBHOOK_URL" if len(unique_webhooks) == 1 else f"WEBHOOK_URL_{unique_webhooks.index(trigger.webhook_url)}"
            
            lines.append(f'  if (sheetName === "{tab.name}") {{')
            lines.append(f"    var rowData = sheet.getRange(row, 1, 1, sheet.getLastColumn()).getValues()[0];")
            lines.append(f"    var headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];")
            lines.append(f"    var payload = {{}};")
            lines.append(f'    payload["event"] = "{trigger.event_name}";')
            lines.append(f'    payload["clinic_id"] = CLINIC_ID;')
            lines.append(f'    payload["sheet"] = "{tab.name}";')
            lines.append(f'    payload["row"] = row;')
            lines.append(f'    payload["timestamp"] = new Date().toISOString();')
            lines.append(f"    for (var i = 0; i < headers.length; i++) {{ payload[headers[i]] = rowData[i]; }}")
            lines.append(f"    _sendWebhook({wh_var}, payload);")
            lines.append("  }")
            lines.append("")
        
        lines.append("}")
        lines.append("")
    
    # --- Scheduled triggers (comment/instructions, actual setup is via GAS UI) ---
    if schedule_triggers:
        lines.append("// ===== SCHEDULED TRIGGERS =====")
        lines.append("// Estos triggers deben configurarse manualmente en GAS:")
        lines.append("// Editar > Activadores del proyecto actual > Añadir activador")
        lines.append("")
        
        for tab, trigger in schedule_triggers:
            func_name = f"scheduled_{tab.name.replace(' ', '_').lower()}"
            wh_var = "WEBHOOK_URL" if len(unique_webhooks) == 1 else f"WEBHOOK_URL_{unique_webhooks.index(trigger.webhook_url)}"
            desc = trigger.description or trigger.cron_expression or "periodic"
            
            lines.append(f"// Cron: {trigger.cron_expression or 'N/A'} | {desc}")
            lines.append(f"function {func_name}() {{")
            lines.append(f'  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("{tab.name}");')
            lines.append(f"  var data = sheet.getDataRange().getValues();")
            lines.append(f"  _sendWebhook({wh_var}, {{")
            lines.append(f'    "event": "{trigger.event_name}",')
            lines.append(f'    "clinic_id": CLINIC_ID,')
            lines.append(f'    "sheet": "{tab.name}",')
            lines.append(f'    "row_count": data.length - 1,')
            lines.append(f'    "timestamp": new Date().toISOString()')
            lines.append("  });")
            lines.append("}")
            lines.append("")
    
    return "\n".join(lines)
