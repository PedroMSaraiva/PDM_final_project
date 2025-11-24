#!/usr/bin/env python3
"""
Cloud Function para baixar arquivos de Empresas da Receita Federal
Salva diretamente no Google Cloud Storage
Baixa, extrai e deleta ZIPs automaticamente
"""

import os
import re
import io
import zipfile
import json
import base64
from pathlib import Path
from urllib.parse import urljoin
from typing import List, Dict, Tuple

import requests
from bs4 import BeautifulSoup
from google.cloud import storage
import functions_framework


# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
BASE_URL = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/'
DESTINATION_BUCKET_NAME = os.environ.get('DESTINATION_BUCKET_NAME', 'seu-bucket-aqui')
BASE_PATH = os.environ.get('BASE_PATH', 'receita_federal')  # Caminho base no bucket

# Filtros de período
START_YEAR_MONTH = os.environ.get('START_YEAR_MONTH', '2020-01')
END_YEAR_MONTH = os.environ.get('END_YEAR_MONTH', '2025-12')
ALLOWED_MONTHS = os.environ.get('ALLOWED_MONTHS', '').split(',')  # Vazio = todos meses

# Configurações de download
MAX_RETRIES = 3
TIMEOUT = (30, 500)  # (connect timeout, read timeout)
CHUNK_SIZE = 1048576

# Inicializar cliente do Storage
storage_client = storage.Client()
bucket = storage_client.bucket(DESTINATION_BUCKET_NAME)


# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def make_request_with_retry(url: str, max_retries: int = MAX_RETRIES) -> requests.Response:
    """Faz requisição HTTP com retry automático"""
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                print(f'   Tentativa {attempt + 1}/{max_retries}...')
            
            response = requests.get(url, timeout=TIMEOUT, allow_redirects=True)
            response.raise_for_status()
            return response
            
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                print(f'   ⚠️  Timeout. Tentando novamente...')
            else:
                raise
                
        except requests.exceptions.RequestException:
            if attempt < max_retries - 1:
                print(f'   ⚠️  Erro de conexão. Tentando novamente...')
            else:
                raise
    
    raise Exception('Máximo de tentativas atingido')


def get_available_folders(base_url: str) -> List[str]:
    """
    Lista todas as pastas ano-mês disponíveis no servidor
    Retorna lista ordenada, filtrada pelo período e meses configurados
    """
    print(f'🔍 Buscando pastas disponíveis em: {base_url}')
    print(f'   Período: {START_YEAR_MONTH} até {END_YEAR_MONTH}')
    if ALLOWED_MONTHS and ALLOWED_MONTHS != ['']:
        print(f'   Meses filtrados: {", ".join(ALLOWED_MONTHS)}')
    
    try:
        response = make_request_with_retry(base_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        folders = []
        for link in soup.find_all('a'):
            href = link.get('href', '')
            # Padrão: YYYY-MM/
            if re.match(r'^\d{4}-\d{2}/$', href):
                folder_date = href.rstrip('/')
                
                # Filtrar pelo período
                if START_YEAR_MONTH <= folder_date <= END_YEAR_MONTH:
                    # Filtrar por meses específicos
                    if ALLOWED_MONTHS and ALLOWED_MONTHS != ['']:
                        month = folder_date.split('-')[1]
                        if month in ALLOWED_MONTHS:
                            folders.append(href)
                    else:
                        folders.append(href)
        
        folders.sort()
        
        if folders:
            print(f'✓ Encontradas {len(folders)} pastas')
        else:
            print(f'⚠️  Nenhuma pasta encontrada')
        
        return folders
        
    except Exception as e:
        print(f'❌ Erro ao listar pastas: {e}')
        return []


def get_empresas_files(folder_url: str) -> List[str]:
    """Lista todos os arquivos de Empresas de uma pasta"""
    try:
        response = make_request_with_retry(folder_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        files = []
        pattern = re.compile(r'Empresas?\d+\.zip', re.IGNORECASE)
        
        for link in soup.find_all('a'):
            href = link.get('href', '')
            if pattern.match(href):
                files.append(href)
        
        files.sort()
        return files
        
    except Exception as e:
        print(f'   ❌ Erro ao listar arquivos: {e}')
        return []


def blob_exists(blob_path: str) -> bool:
    """Verifica se um blob existe no bucket"""
    blob = bucket.blob(blob_path)
    return blob.exists()


def check_extraction_marker(folder_name: str, zip_name: str) -> bool:
    """Verifica se existe marcador de extração para um ZIP"""
    marker_path = f'{BASE_PATH}/{folder_name}/.{Path(zip_name).stem}.extracted'
    return blob_exists(marker_path)


def create_extraction_marker(folder_name: str, zip_name: str):
    """Cria marcador de extração no bucket"""
    marker_path = f'{BASE_PATH}/{folder_name}/.{Path(zip_name).stem}.extracted'
    blob = bucket.blob(marker_path)
    blob.upload_from_string('extracted', content_type='text/plain')
    print(f'   ✓ Marcador criado: {marker_path}')


def download_and_extract_to_gcs(url: str, folder_name: str, file_name: str) -> Tuple[bool, bool]:
    """
    Baixa ZIP, extrai conteúdo para GCS e deleta ZIP (em memória)
    Retorna: (download_success, extraction_success)
    """
    try:
        # Verificar se já foi extraído
        if check_extraction_marker(folder_name, file_name):
            print(f'   ⏭️  {file_name}: Já extraído, pulando...')
            return (True, True)
        
        print(f'   ⬇️  {file_name}: Baixando...')
        
        # Download do arquivo ZIP em memória
        response = requests.get(url, stream=True, timeout=TIMEOUT)
        response.raise_for_status()
        
        # Ler conteúdo do ZIP em memória
        zip_content = io.BytesIO()
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            if chunk:
                zip_content.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    percent = (downloaded / total_size) * 100
                    if downloaded % (CHUNK_SIZE * 100) == 0:  # Log a cada ~800KB
                        print(f'   {file_name}: {percent:.1f}%')
        
        print(f'   ✅ {file_name}: Download concluído ({downloaded / 1024 / 1024:.1f} MB)')
        
        # Extrair e fazer upload dos arquivos
        print(f'   📦 {file_name}: Extraindo e enviando para GCS...')
        zip_content.seek(0)
        
        with zipfile.ZipFile(zip_content, 'r') as zip_ref:
            # Testar integridade
            if zip_ref.testzip() is not None:
                print(f'   ❌ {file_name}: ZIP corrompido')
                return (True, False)
            
            # Extrair cada arquivo
            members = zip_ref.namelist()
            files_uploaded = 0
            
            for member in members:
                if not member.endswith('/'):  # Ignorar diretórios
                    # Ler arquivo do ZIP
                    file_data = zip_ref.read(member)
                    
                    # Caminho no bucket: BASE_PATH/folder_name/arquivo.csv
                    blob_path = f'{BASE_PATH}/{folder_name}/{member}'
                    blob = bucket.blob(blob_path)
                    
                    # Upload para GCS
                    blob.upload_from_string(file_data, content_type='text/csv')
                    files_uploaded += 1
                    
                    if files_uploaded % 5 == 0:
                        print(f'   ... {files_uploaded}/{len(members)} arquivos enviados')
            
            print(f'   ✅ {file_name}: {files_uploaded} arquivos extraídos para GCS')
            
            # Criar marcador de extração
            create_extraction_marker(folder_name, file_name)
        
        # ZIP é automaticamente deletado (estava em memória)
        print(f'   🗑️  {file_name}: ZIP removido da memória')
        
        return (True, True)
        
    except requests.exceptions.Timeout:
        print(f'   ❌ {file_name}: Timeout no download')
        return (False, False)
        
    except zipfile.BadZipFile:
        print(f'   ❌ {file_name}: Arquivo ZIP inválido')
        return (True, False)
        
    except Exception as e:
        print(f'   ❌ {file_name}: Erro - {str(e)[:100]}')
        return (False, False)


def process_single_file(folder_name: str, file_name: str) -> Dict[str, any]:
    """
    Processa um único arquivo ZIP
    Retorna resultado do processamento
    """
    # Normalizar folder_name
    if not folder_name.endswith('/'):
        folder_name = folder_name + '/'
    
    folder_url = urljoin(BASE_URL, folder_name)
    file_url = urljoin(folder_url, file_name)
    
    print(f'\n{"=" * 80}')
    print(f'📁 Pasta: {folder_name.rstrip("/")}')
    print(f'📦 Arquivo: {file_name}')
    print(f'{"=" * 80}')
    
    download_ok, extract_ok = download_and_extract_to_gcs(file_url, folder_name.rstrip('/'), file_name)
    
    result = {
        'folder': folder_name.rstrip('/'),
        'file': file_name,
        'download_success': download_ok,
        'extraction_success': extract_ok
    }
    
    if download_ok and extract_ok:
        print(f'\n✅ {file_name} processado com sucesso!')
    else:
        print(f'\n❌ {file_name} falhou!')
    
    return result


def list_files_in_folder(folder: str) -> List[str]:
    """
    Lista todos os arquivos de Empresas de uma pasta
    Retorna apenas os nomes dos arquivos
    """
    folder_url = urljoin(BASE_URL, folder)
    files = get_empresas_files(folder_url)
    return files


def process_folder(folder: str) -> Dict[str, int]:
    """
    Processa uma pasta específica (baixa todos as empresas)
    Retorna estatísticas
    """
    folder_url = urljoin(BASE_URL, folder)
    folder_name = folder.rstrip('/')
    
    print(f'\n{"=" * 80}')
    print(f'📁 Processando: {folder_name}')
    print(f'{"=" * 80}')
    
    # Listar arquivos
    files = get_empresas_files(folder_url)
    
    if not files:
        print(f'⚠️  Nenhum arquivo encontrado')
        return {'total': 0, 'downloaded': 0, 'extracted': 0, 'skipped': 0, 'failed': 0}
    
    print(f'📦 Encontrados {len(files)} arquivos')
    
    stats = {
        'total': len(files),
        'downloaded': 0,
        'extracted': 0,
        'skipped': 0,
        'failed': 0
    }
    
    # Processar cada arquivo
    for idx, file_name in enumerate(files, 1):
        print(f'\n[{idx}/{len(files)}] {file_name}')
        
        file_url = urljoin(folder_url, file_name)
        download_ok, extract_ok = download_and_extract_to_gcs(file_url, folder_name, file_name)
        
        if download_ok and extract_ok:
            if check_extraction_marker(folder_name, file_name):
                stats['skipped'] += 1
            else:
                stats['downloaded'] += 1
                stats['extracted'] += 1
        elif download_ok and not extract_ok:
            stats['failed'] += 1
        else:
            stats['failed'] += 1
    
    return stats


# =============================================================================
# CLOUD FUNCTION HANDLERS
# =============================================================================

@functions_framework.http
def crawler_receita_http(request):
    """
    Handler HTTP - processa todas as pastas configuradas
    """
    print('=' * 80)
    print('CRAWLER RECEITA FEDERAL - CLOUD FUNCTION')
    print('=' * 80)
    print(f'Bucket destino: {DESTINATION_BUCKET_NAME}')
    print(f'Caminho base: {BASE_PATH}')
    print()
    
    # Listar pastas disponíveis
    folders = get_available_folders(BASE_URL)
    
    if not folders:
        return {'error': 'Nenhuma pasta encontrada'}, 404
    
    print(f'\n📋 Total de pastas: {len(folders)}')
    
    # Estatísticas globais
    global_stats = {
        'folders_processed': 0,
        'total_files': 0,
        'downloaded': 0,
        'extracted': 0,
        'skipped': 0,
        'failed': 0
    }
    
    # Processar cada pasta
    for folder in folders:
        stats = process_folder(folder)
        global_stats['folders_processed'] += 1
        global_stats['total_files'] += stats['total']
        global_stats['downloaded'] += stats['downloaded']
        global_stats['extracted'] += stats['extracted']
        global_stats['skipped'] += stats['skipped']
        global_stats['failed'] += stats['failed']
    
    # Resumo final
    print('\n' + '=' * 80)
    print('RESUMO FINAL')
    print('=' * 80)
    print(f"📊 Estatísticas:")
    print(f"   Pastas processadas: {global_stats['folders_processed']}")
    print(f"   Total de arquivos:  {global_stats['total_files']}")
    print(f"   ✅ Baixados:         {global_stats['downloaded']}")
    print(f"   📦 Extraídos:        {global_stats['extracted']}")
    print(f"   ⏭️  Pulados:          {global_stats['skipped']}")
    print(f"   ❌ Falhas:           {global_stats['failed']}")
    print()
    print(f"📁 Arquivos salvos em: gs://{DESTINATION_BUCKET_NAME}/{BASE_PATH}/")
    print('✅ Processo concluído!')
    
    return global_stats, 200


@functions_framework.cloud_event
def crawler_receita_pubsub(cloud_event):
    """
    Handler Pub/Sub - processa arquivo individual, lista arquivos ou processa pasta
    
    Mensagens aceitas:
    1. {"folder": "2024-03", "file": "Empresas0.zip"} - processa arquivo específico (RECOMENDADO)
    2. {"folder": "2024-03", "list_files": true} - lista arquivos da pasta
    3. {"folder": "2024-03"} - processa todos arquivos da pasta (pode dar timeout!)
    4. {} ou {"list_folders": true} - lista todas as pastas disponíveis
    """
    try:
        message_data_str = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
        message_data = json.loads(message_data_str) if message_data_str else {}
    except Exception as e:
        print(f"Erro ao decodificar mensagem: {e}")
        message_data = {}
    
    print('=' * 80)
    print('CRAWLER RECEITA FEDERAL - CLOUD FUNCTION (Pub/Sub)')
    print('=' * 80)
    print(f'Bucket destino: {DESTINATION_BUCKET_NAME}')
    print(f'Caminho base: {BASE_PATH}')
    print()
    
    # CASO 1: Processar arquivo específico (RECOMENDADO - evita timeout)
    if 'folder' in message_data and 'file' in message_data:
        folder = message_data['folder']
        file_name = message_data['file']
        
        print(f'🎯 Modo: Processar arquivo individual')
        result = process_single_file(folder, file_name)
        
        print(f'\n{"=" * 80}')
        print('RESULTADO')
        print(f'{"=" * 80}')
        print(f"Pasta: {result['folder']}")
        print(f"Arquivo: {result['file']}")
        print(f"Download: {'✅' if result['download_success'] else '❌'}")
        print(f"Extração: {'✅' if result['extraction_success'] else '❌'}")
        print()
        
        if result['download_success'] and result['extraction_success']:
            print('✅ Processamento concluído com sucesso!')
        else:
            print('❌ Processamento falhou!')
        
        return result
    
    # CASO 2: Listar arquivos de uma pasta
    elif 'folder' in message_data and message_data.get('list_files'):
        folder = message_data['folder']
        
        print(f'📋 Modo: Listar arquivos da pasta {folder}')
        files = list_files_in_folder(folder)
        
        print(f'\n{"=" * 80}')
        print(f'ARQUIVOS ENCONTRADOS: {len(files)}')
        print(f'{"=" * 80}')
        for idx, file in enumerate(files, 1):
            print(f'  {idx}. {file}')
        print()
        
        return {'folder': folder, 'files': files, 'count': len(files)}
    
    # CASO 3: Processar pasta inteira (CUIDADO: pode dar timeout!)
    elif 'folder' in message_data:
        folder = message_data['folder']
        if not folder.endswith('/'):
            folder += '/'
        
        print(f'⚠️  Modo: Processar pasta completa (pode dar timeout em pastas grandes!)')
        print(f'📁 Pasta: {folder}')
        
        stats = process_folder(folder)
        
        print(f'\n{"=" * 80}')
        print('RESUMO')
        print(f'{"=" * 80}')
        print(f"Total: {stats['total']}")
        print(f"✅ Extraídos: {stats['extracted']}")
        print(f"⏭️  Pulados: {stats['skipped']}")
        print(f"❌ Falhas: {stats['failed']}")
        print()
        print('✅ Pasta concluída!')
        
        return stats
    
    # CASO 4: Listar pastas disponíveis
    else:
        print('📋 Modo: Listar pastas disponíveis')
        folders = get_available_folders(BASE_URL)
        
        print(f'\n{"=" * 80}')
        print(f'PASTAS DISPONÍVEIS: {len(folders)}')
        print(f'{"=" * 80}')
        for idx, folder in enumerate(folders, 1):
            print(f'  {idx}. {folder.rstrip("/")}')
        print()
        
        return {'folders': [f.rstrip('/') for f in folders], 'count': len(folders)}
    
    print('Processamento concluído.')


if __name__ == '__main__':
    # Para testes locais
    class MockRequest:
        pass
    
    crawler_receita_http(MockRequest())

