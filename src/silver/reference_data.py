"""Tablas de referencia de Silver (versionadas aqui, sembradas en `silver.ref_*`).

Los valores salen de lo que realmente genera `src/datagen` (ciudades, typos, categorias, termino
con typo); un test verifica la cobertura. En produccion las reglas leen las TABLAS: cambiar un
mapeo = cambiar la fila (la siembra solo inserta lo que falta, no pisa ediciones de los expertos).
"""
from __future__ import annotations

# ref_city_mapping: clave = lower(trim(city)) -> ciudad canonica. Incluye las canonicas (identidad)
# para distinguir "ya correcta" de "ciudad desconocida".
CITY_MAPPING: dict[str, str] = {
    "lima": "Lima", "limaa": "Lima", "lmia": "Lima",
    "arequipa": "Arequipa", "arequpa": "Arequipa", "arequipaa": "Arequipa",
    "trujillo": "Trujillo", "trujilo": "Trujillo", "trujillio": "Trujillo",
    "cusco": "Cusco", "cuzco": "Cusco", "cuscco": "Cusco",
    "piura": "Piura", "pirua": "Piura",
    "chiclayo": "Chiclayo", "chiclallo": "Chiclayo", "chiclayoo": "Chiclayo",
}

# ref_category_division: clave = lower(trim(category)) -> (categoria canonica, division).
CATEGORY_DIVISION: dict[str, tuple[str, str]] = {
    # madre
    "snacks salados": ("Snacks Salados", "Alimentos"),
    "galletas": ("Galletas", "Alimentos"),
    "lacteos": ("Lacteos", "Alimentos"),
    "bebidas": ("Bebidas", "Bebidas"),
    "cuidado del hogar": ("Cuidado del Hogar", "Hogar y Cuidado Personal"),
    "cuidado personal": ("Cuidado Personal", "Hogar y Cuidado Personal"),
    # hija
    "bebidas isotonicas": ("Bebidas Isotonicas", "Bebidas"),
    "bebidas energizantes": ("Bebidas Energizantes", "Bebidas"),
    "barras proteicas": ("Barras Proteicas", "Nutricion Deportiva"),
    "snacks saludables": ("Snacks Saludables", "Alimentos"),
    "cereales y granola": ("Cereales y Granola", "Alimentos"),
}

# ref_typo_fixes: (entidad, columna, termino erroneo, termino correcto). Palabra completa.
TYPO_FIXES: list[tuple[str, str, str, str]] = [
    ("products", "product_name", "Energisante", "Energizante"),
]

# ref_company: atributos conformados por compania (market/channel de la referencia).
COMPANY_ATTRS: list[tuple[str, str, str, str]] = [
    ("madre", "madre_erp", "Peru", "Retail tradicional"),
    ("hija", "hija_export", "Peru", "Bares deportivos"),
]
