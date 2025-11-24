#!/usr/bin/env python3
"""
Cloud Function para baixar arquivos de Regime Tributário (Lucros) da Receita Federal
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
BASE_URL = 'https://arquivos.receitafederal.gov.br/dados/cnpj/regime_tributario/'
DESTINATION_BUCKET_NAME = os.environ.get('DESTINATION_BUCKET_NAME', 'seu-bucket-aqui')
BASE_PATH = os.environ.get('BASE_PATH', 'receita_federal/regime_tributario')  # Caminho base no bucket

# Arquivos de regime tributário a serem baixados
REGIME_FILES = [
    'Lucro Arbitrado.zip',
    'Lucro Presumido.zip',
    'Lucro Real.zip',
    'Imunes e Isentas.zip'
]

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


def get_available_regime_files(base_url: str) -> List[str]:
    """
    Lista todos os arquivos de regime tributário disponíveis no servidor
    Retorna lista de arquivos encontrados
    """
    print(f'🔍 Buscando arquivos de regime tributário em: {base_url}')
    
    try:
        response = make_request_with_retry(base_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        files = []
        for link in soup.find_all('a'):
            href = link.get('href', '')
            # Buscar arquivos .zip relevantes
            if href.endswith('.zip') and any(regime_name.lower() in href.lower() 
                                            for regime_name in ['lucro', 'imunes', 'isentas']):
                files.append(href)
        
        if files:
            print(f'✓ Encontrados {len(files)} arquivos de regime tributário')
            for file in files:
                print(f'   - {file}')
        else:
            print(f'⚠️  Nenhum arquivo encontrado')
        
        return files
        
    except Exception as e:
        print(f'❌ Erro ao listar arquivos: {e}')
        return []


def blob_exists(blob_path: str) -> bool:
    """Verifica se um blob existe no bucket"""
    blob = bucket.blob(blob_path)
    return blob.exists()


def check_extraction_marker(zip_name: str) -> bool:
    """Verifica se existe marcador de extração para um ZIP"""
    marker_path = f'{BASE_PATH}/.{Path(zip_name).stem}.extracted'
    return blob_exists(marker_path)


def create_extraction_marker(zip_name: str):
    """Cria marcador de extração no bucket"""
    marker_path = f'{BASE_PATH}/.{Path(zip_name).stem}.extracted'
    blob = bucket.blob(marker_path)
    blob.upload_from_string('extracted', content_type='text/plain')
    print(f'   ✓ Marcador criado: {marker_path}')


def download_and_extract_to_gcs(url: str, file_name: str) -> Tuple[bool, bool]:
    """
    Baixa ZIP, extrai conteúdo para GCS e deleta ZIP (em memória)
    Retorna: (download_success, extraction_success)
    """
    try:
        # Verificar se já foi extraído
        if check_extraction_marker(file_name):
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
                    if downloaded % (CHUNK_SIZE * 100) == 0:  # Log a cada ~100MB
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
            
            # Criar subdiretório baseado no nome do arquivo (sem extensão)
            regime_type = Path(file_name).stem
            
            for member in members:
                if not member.endswith('/'):  # Ignorar diretórios
                    # Ler arquivo do ZIP
                    file_data = zip_ref.read(member)
                    
                    # Caminho no bucket: BASE_PATH/tipo_regime/arquivo.csv
                    blob_path = f'{BASE_PATH}/{regime_type}/{member}'
                    blob = bucket.blob(blob_path)
                    
                    # Upload para GCS
                    blob.upload_from_string(file_data, content_type='text/csv')
                    files_uploaded += 1
                    
                    if files_uploaded % 5 == 0:
                        print(f'   ... {files_uploaded}/{len(members)} arquivos enviados')
            
            print(f'   ✅ {file_name}: {files_uploaded} arquivos extraídos para GCS')
            
            # Criar marcador de extração
            create_extraction_marker(file_name)
        
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


def process_single_file(file_name: str) -> Dict[str, any]:
    """
    Processa um único arquivo ZIP de regime tributário
    Retorna resultado do processamento
    """
    file_url = urljoin(BASE_URL, file_name)
    
    print(f'\n{"=" * 80}')
    print(f'📦 Arquivo: {file_name}')
    print(f'🔗 URL: {file_url}')
    print(f'{"=" * 80}')
    
    download_ok, extract_ok = download_and_extract_to_gcs(file_url, file_name)
    
    result = {
        'file': file_name,
        'download_success': download_ok,
        'extraction_success': extract_ok
    }
    
    if download_ok and extract_ok:
        print(f'\n✅ {file_name} processado com sucesso!')
    else:
        print(f'\n❌ {file_name} falhou!')
    
    return result


def process_all_files() -> Dict[str, int]:
    """
    Processa todos os arquivos de regime tributário disponíveis
    Retorna estatísticas
    """
    print(f'\n{"=" * 80}')
    print(f'📁 Processando arquivos de regime tributário')
    print(f'{"=" * 80}')
    
    # Listar arquivos disponíveis
    files = get_available_regime_files(BASE_URL)
    
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
        
        file_url = urljoin(BASE_URL, file_name)
        download_ok, extract_ok = download_and_extract_to_gcs(file_url, file_name)
        
        if download_ok and extract_ok:
            # Verificar se foi realmente processado ou apenas pulado
            if check_extraction_marker(file_name):
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
    Handler HTTP - processa todos os arquivos de regime tributário
    """
    print('=' * 80)
    print('CRAWLER REGIME TRIBUTÁRIO - RECEITA FEDERAL')
    print('=' * 80)
    print(f'Bucket destino: {DESTINATION_BUCKET_NAME}')
    print(f'Caminho base: {BASE_PATH}')
    print()
    
    # Processar todos os arquivos
    stats = process_all_files()
    
    if stats['total'] == 0:
        return {'error': 'Nenhum arquivo encontrado'}, 404
    
    # Resumo final
    print('\n' + '=' * 80)
    print('RESUMO FINAL')
    print('=' * 80)
    print(f"📊 Estatísticas:")
    print(f"   Total de arquivos:  {stats['total']}")
    print(f"   ✅ Baixados:         {stats['downloaded']}")
    print(f"   📦 Extraídos:        {stats['extracted']}")
    print(f"   ⏭️  Pulados:          {stats['skipped']}")
    print(f"   ❌ Falhas:           {stats['failed']}")
    print()
    print(f"📁 Arquivos salvos em: gs://{DESTINATION_BUCKET_NAME}/{BASE_PATH}/")
    print('✅ Processo concluído!')
    
    return stats, 200


@functions_framework.cloud_event
def crawler_receita_pubsub(cloud_event):
    """
    Handler Pub/Sub - processa arquivos de regime tributário
    
    Mensagens aceitas:
    1. {"file": "Lucro Arbitrado.zip"} - processa arquivo específico (RECOMENDADO)
    2. {"list_files": true} - lista arquivos disponíveis
    3. {} - processa todos os arquivos
    """
    try:
        message_data_str = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
        message_data = json.loads(message_data_str) if message_data_str else {}
    except Exception as e:
        print(f"Erro ao decodificar mensagem: {e}")
        message_data = {}
    
    print('=' * 80)
    print('CRAWLER REGIME TRIBUTÁRIO - RECEITA FEDERAL (Pub/Sub)')
    print('=' * 80)
    print(f'Bucket destino: {DESTINATION_BUCKET_NAME}')
    print(f'Caminho base: {BASE_PATH}')
    print()
    
    # CASO 1: Processar arquivo específico (RECOMENDADO)
    if 'file' in message_data:
        file_name = message_data['file']
        
        print(f'🎯 Modo: Processar arquivo individual')
        result = process_single_file(file_name)
        
        print(f'\n{"=" * 80}')
        print('RESULTADO')
        print(f'{"=" * 80}')
        print(f"Arquivo: {result['file']}")
        print(f"Download: {'✅' if result['download_success'] else '❌'}")
        print(f"Extração: {'✅' if result['extraction_success'] else '❌'}")
        print()
        
        if result['download_success'] and result['extraction_success']:
            print('✅ Processamento concluído com sucesso!')
        else:
            print('❌ Processamento falhou!')
        
        return result
    
    # CASO 2: Listar arquivos disponíveis
    elif message_data.get('list_files'):
        print('📋 Modo: Listar arquivos disponíveis')
        files = get_available_regime_files(BASE_URL)
        
        print(f'\n{"=" * 80}')
        print(f'ARQUIVOS DISPONÍVEIS: {len(files)}')
        print(f'{"=" * 80}')
        for idx, file in enumerate(files, 1):
            print(f'  {idx}. {file}')
        print()
        
        return {'files': files, 'count': len(files)}
    
    # CASO 3: Processar todos os arquivos
    else:
        print('🎯 Modo: Processar todos os arquivos de regime tributário')
        
        stats = process_all_files()
        
        print(f'\n{"=" * 80}')
        print('RESUMO')
        print(f'{"=" * 80}')
        print(f"Total: {stats['total']}")
        print(f"✅ Baixados: {stats['downloaded']}")
        print(f"📦 Extraídos: {stats['extracted']}")
        print(f"⏭️  Pulados: {stats['skipped']}")
        print(f"❌ Falhas: {stats['failed']}")
        print()
        print('✅ Processo concluído!')
        
        return stats
    
    print('Processamento concluído.')


if __name__ == '__main__':
    # Para testes locais
    class MockRequest:
        pass
    
    crawler_receita_http(MockRequest())

