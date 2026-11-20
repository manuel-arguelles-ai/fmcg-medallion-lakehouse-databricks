"""Datos maestros base del dominio FMCG propio (consumo masivo andino) y listas de anomalias."""

# Columnas de salida por fuente (el registro de Bronze debe coincidir; ver tests).
COLUMNS = {
    "customers": ["customer_id", "customer_name", "city"],
    "products": ["product_id", "product_name", "category"],
    "prices": ["product_id", "price_month", "unit_price"],
    "orders": ["order_id", "line_no", "order_date", "customer_id", "product_id", "order_qty"],
}

CITIES = ["Lima", "Arequipa", "Trujillo", "Cusco", "Piura", "Chiclayo"]
# Typos sembrados en la hija (correccion -> variantes erroneas).
CITY_TYPOS = {
    "Lima": ["Limaa", "Lmia"],
    "Arequipa": ["Arequpa", "Arequipaa"],
    "Trujillo": ["Trujilo", "Trujillio"],
    "Cusco": ["Cuzco", "Cuscco"],
    "Piura": ["Pirua"],
    "Chiclayo": ["Chiclallo", "Chiclayoo"],
}

MADRE_CUSTOMER_PREFIX = ["Bodega", "Minimarket", "Autoservicio", "Mercado", "Distribuidora", "Tienda"]
HIJA_CUSTOMER_PREFIX = ["Sport Bar", "Bar Deportivo", "Gol Bar", "Pub Futbolero", "Arena Bar", "Cancha Bar"]
CUSTOMER_NAMES = [
    "Inti", "Quilla", "Pachamama", "Wari", "Kuntur", "Amaru", "Illari", "Sumaq", "Tayta", "Mayu",
    "Allpa", "Sami", "Yaku", "Nina", "Killa", "Apu", "Chaska", "Rumi", "Sacha", "Tika",
    "Puma", "Huaca", "Pampa", "Choclo", "Ceibo", "Molle", "Tara", "Quena", "Zampona", "Charango",
]

# (categoria, [(nombre base, [variantes])])
MADRE_PRODUCTS = {
    "Snacks Salados": [("Papas Andinas", ["Original 150g", "Limon 150g", "Picante 150g"]),
                       ("Camote Chips", ["Sal 120g", "Miel 120g"])],
    "Bebidas": [("Nectar Frutal", ["Mango 1L", "Maracuya 1L", "Durazno 1L"]),
                ("Agua Mineral", ["Sin gas 625ml", "Con gas 625ml"])],
    "Galletas": [("Galleta Quinua", ["Vainilla 6u", "Chocolate 6u"]),
                 ("Galleta Soda", ["Clasica 10u", "Integral 10u"])],
    "Cuidado del Hogar": [("Detergente Pampa", ["Floral 900g", "Limon 900g"]),
                          ("Lavavajilla Sacha", ["Limon 500ml", "Aloe 500ml"])],
    "Lacteos": [("Yogurt Kuntur", ["Fresa 1L", "Natural 1L", "Vainilla 1L"]),
                ("Leche Evaporada", ["Entera 400g", "Light 400g"])],
    "Cuidado Personal": [("Jabon Illari", ["Avena 90g", "Coco 90g"]),
                         ("Shampoo Quilla", ["Sabila 400ml", "Ortiga 400ml"])],
}
HIJA_PRODUCTS = {
    "Bebidas Isotonicas": [("Isotonica Gol", ["Limon 500ml", "Naranja 500ml", "Uva 500ml"])],
    "Bebidas Energizantes": [("Bebida Energizante Rayo", ["Original 355ml", "Zero 355ml"]),
                             ("Shot Energizante Rayo", ["Cafe 60ml", "Guarana 60ml"])],
    "Barras Proteicas": [("Barra Proteica Cancha", ["Cacao 45g", "Mani 45g", "Coco 45g"])],
    "Snacks Saludables": [("Mix Frutos Secos", ["Clasico 80g", "Tropical 80g"]),
                          ("Chips de Platano", ["Sal 70g", "Chile 70g"])],
    "Cereales y Granola": [("Granola Arena", ["Miel 300g", "Cacao 300g"])],
}

# Typo recurrente de la hija: "Energizante" -> "Energisante" en nombre/categoria.
TYPO_TERM = ("Energizante", "Energisante")
INVALID_CUSTOMER_IDS = ["N/A", "UNKNOWN", "#REF!", "sin_dato"]
NON_NUMERIC_PRICES = ["N/A", "abc", "--", "s/p"]

MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August",
          "September", "October", "November", "December"]
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
