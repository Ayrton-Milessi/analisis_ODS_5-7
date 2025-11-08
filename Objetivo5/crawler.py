# Importamos las bibliotecas
from urllib.parse import urlparse, urljoin
from collections import deque
from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
import re

# ========================================================================
# Configuracion del Crawler

# URL de inicio
START_URLS = ["https://www.f1academy.com/Racing-Series/Standings/Driver"]

# Palabras clave para filtrar las tablas
PALABRAS_CLAVE = [
    'driver', 'f1', 'academy', 'standings', 'team', 'race', 'point', '2025 F1 Academy Standings'
]

# Dominio Base
DOMINIO_BASE = 'f1academy.com'

# Retardo entre peticiones (en segundos) para ser cortés con el servidor
DELAY_SECONDS = 1

# Limite de profundidad del rastreo
MAX_DEPTH = 3 

# Exclusiones de rutas
EXCLUDE_PATHS_REGEX = r'(news|stories|contact|terms|privacy|faq|help|search|media|sponsors|partners)'

# ========================================================================
# Funciones

def normalize_and_filter_url(base_url, href, dominio_base):
    """
    Convierte un enlace relativo a absoluto y verifica si pertenece al dominio base/ruta
    """
    
    absolute_url = urljoin(base_url, href)
    parsed_url = urlparse(absolute_url)
    
    if not parsed_url.scheme:
        return None
        
    # Construir la URL limpia (sin fragmentos ni parámetros de consulta)
    normalized_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}".lower()
    
    dominio_base_lower = dominio_base.lower()

    # Filtra por Dominio Base/Ruta
    if dominio_base_lower not in normalized_url:
        return None

    # Filtrado de Archivos
    if re.search(r'\.(pdf|jpg|png|zip)$', normalized_url, re.IGNORECASE):
        return None
        
    # Filtro por excluir rutas
    if re.search(EXCLUDE_PATHS_REGEX, normalized_url):
        return None

    return normalized_url

def scrape_and_filter_page(url, palabras_clave):
    """
    Visita una URL, determina si tiene una tabla de interés (estática o dinámica), 
    y devuelve una bandera de coincidencia y los enlaces internos.
    """
    
    print(f"  🔎 Analizando contenido: {url}")
    has_matching_table = False
    internal_links = set()
    
    try:
        # Petición HTTP
        response = requests.get(url, timeout=10)
        response.raise_for_status()    
        soup = BeautifulSoup(response.text, 'html.parser')

        # Verificación de Tablas Estáticas
        try:
            # Intentamos extraer tablas estáticas y verificar palabras clave
            df_list = pd.read_html(response.text, flavor='bs4')
            print(f"  ✅ {len(df_list)} tabla(s) estática(s) encontradas en la página.")

            if df_list:
                for i, df in enumerate(df_list):
                    contenido_str = df.to_string().lower() 
                    palabras_clave_lower = [p.lower() for p in palabras_clave]
                    
                    if any(palabra in contenido_str for palabra in palabras_clave_lower):
                        print(f"    ⭐ Tabla {i+1} estática coincide con las palabras clave.")
                        has_matching_table = True
                        break # Encontramos una tabla, podemos salir del bucle
        except ValueError:
            pass

        # Verificación de Tablas Dinámicas
        if soup.find('table', id='ponchoTable'):
            print("  ⚠️ Tabla dinámica 'ponchoTable' detectada. (Marcada como interés)")
            has_matching_table = True

        # Extracción de enlaces para el rastreo
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            normalized_link = normalize_and_filter_url(url, href, DOMINIO_BASE)
            
            if normalized_link:
                internal_links.add(normalized_link)

    except requests.exceptions.RequestException as e:
        print(f"  ❌ Error de red o HTTP al acceder a {url}: {e}")
    except Exception as e:
        print(f"  ❌ Error inesperado en el procesamiento de {url}: {e}")

    print(f"  🔗 Enlaces internos válidos encontrados: {len(internal_links)}")
    
    return has_matching_table, internal_links

def main_crawler_scraper():
    """
    Función principal que ejecuta el proceso de rastreo y guarda las URLs de interés en un TXT
    """
    
    print("--- INICIO DEL RASTREO Y SCRAPING SELECTIVO ---")
    print(f"Dominio base: {DOMINIO_BASE}")
    print(f"Palabras clave: {PALABRAS_CLAVE}")
    print(f"Profundidad máxima: {MAX_DEPTH}")
    print(f"Rutas excluidas (REGEX): {EXCLUDE_PATHS_REGEX}")
    
    queue = deque([(url, 0) for url in START_URLS])
    visited = set(START_URLS) 
    urls_de_interes = []
    output_filename = "Objetivo5/urls_con_tablas.txt"

    while queue:
        current_url, current_depth = queue.popleft()
        
        if current_depth > MAX_DEPTH:
            print(f"  ⛔ Profundidad {current_depth} > Máx {MAX_DEPTH}. Saltando: {current_url}")
            continue
            
        print("\n=======================================================")
        print(f"🌐 VISITANDO: {current_url} (Profundidad: {current_depth})")
        print("=======================================================")
        
        # Obtenemos la señal de la tabla y nuevos enlaces
        has_table, new_links = scrape_and_filter_page(current_url, PALABRAS_CLAVE)
        
        # Procesamos y guardar la URL si tiene una tabla
        if has_table:
            # Nos aseguramos de guardar la URL solo una vez
            if current_url not in urls_de_interes:
                urls_de_interes.append(current_url)
                print(f"  📝 URL de interés registrada: {current_url}")
        
        # Agregamos nuevos enlaces a la cola de rastreo
        for link in new_links:
            if link not in visited:
                visited.add(link)
                # Agregamos la URL con la nueva profundidad (nivel + 1)
                queue.append((link, current_depth + 1)) 
        
        # Esperamos para cumplir con la ética de rastreo
        time.sleep(DELAY_SECONDS)

    with open(output_filename, 'w', encoding='utf-8') as f:
        f.write('\n'.join(urls_de_interes))

    print("\n--- RASTREO Y SCRAPING FINALIZADO ---")
    print(f"Páginas con tablas de interés encontradas: {len(urls_de_interes)}")
    print(f"Lista de URLs guardada en: '{output_filename}'.")
    
if __name__ == "__main__":
    main_crawler_scraper()