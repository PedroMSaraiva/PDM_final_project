#!/usr/bin/env python3
"""
Script para baixar todos os arquivos de Estabelecimentos da Receita Federal
Baixa de todos os anos/meses disponíveis no servidor
"""

import os
import re
import sys
import time
import zipfile
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import wget


# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
BASE_URL = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/'
DOWNLOAD_DIR = Path(__file__).parent / 'downloads'
EXTRACTED_DIR = Path(__file__).parent / 'extracted'
MAX_RETRIES = 3
TIMEOUT = 60

# Filtro de período (formato YYYY-MM)
# NOTA: O servidor só tem dados a partir de 2023-05
START_YEAR_MONTH = '2020-01'  # Início do período desejado (tentará desde 2020)
END_YEAR_MONTH = '2025-12'    # Fim do período desejado


# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def make_dirs():
    """Cria os diretórios necessários"""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)


def make_request_with_retry(url, max_retries=MAX_RETRIES, timeout=TIMEOUT):
    """Faz requisição HTTP com retry automático"""
    retry_delay = 10
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                print(f'   Tentativa {attempt + 1}/{max_retries}...')
            
            response = requests.get(url, timeout=timeout, allow_redirects=True)
            response.raise_for_status()
            return response
            
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                print(f'   ⚠️  Timeout. Aguardando {retry_delay}s...')
                time.sleep(retry_delay)
            else:
                raise
                
        except requests.exceptions.ConnectionError:
            if attempt < max_retries - 1:
                print(f'   ⚠️  Erro de conexão. Aguardando {retry_delay}s...')
                time.sleep(retry_delay)
            else:
                raise
                
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise
    
    raise Exception('Máximo de tentativas atingido')


def get_available_folders(base_url):
    """
    Lista todas as pastas ano-mês disponíveis no servidor
    Retorna lista ordenada (mais antiga primeiro), filtrada pelo período configurado
    """
    print(f'🔍 Buscando pastas disponíveis em: {base_url}')
    print(f'   Período desejado: {START_YEAR_MONTH} até {END_YEAR_MONTH}')
    
    try:
        response = make_request_with_retry(base_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Procurar por links que parecem pastas de data (YYYY-MM/)
        folders = []
        for link in soup.find_all('a'):
            href = link.get('href', '')
            # Padrão: YYYY-MM/ (ex: 2024-05/)
            if re.match(r'^\d{4}-\d{2}/$', href):
                # Remover a barra final para comparação
                folder_date = href.rstrip('/')
                
                # Filtrar pelo período configurado
                if START_YEAR_MONTH <= folder_date <= END_YEAR_MONTH:
                    folders.append(href)
        
        # Ordenar cronologicamente (mais antiga primeiro)
        folders.sort()
        
        if folders:
            print(f'✓ Encontradas {len(folders)} pastas no período')
            print(f'  Intervalo disponível: {folders[0]} até {folders[-1]}')
        else:
            print(f'⚠️  Nenhuma pasta encontrada no período {START_YEAR_MONTH} a {END_YEAR_MONTH}!')
            print('   O servidor pode não ter dados desse período disponíveis.')
        
        return folders
        
    except Exception as e:
        print(f'❌ Erro ao listar pastas: {e}')
        return []


def get_estabelecimento_files(folder_url):
    """
    Lista todos os arquivos de Estabelecimentos de uma pasta específica
    Retorna lista de nomes de arquivos
    """
    try:
        response = make_request_with_retry(folder_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Procurar por arquivos .zip que contenham "Estabelecimento" + número
        # Padrão: Estabelecimentos0.zip, Estabelecimentos1.zip, etc.
        files = []
        pattern = re.compile(r'Estabelecimentos?\d+\.zip', re.IGNORECASE)
        
        for link in soup.find_all('a'):
            href = link.get('href', '')
            if pattern.match(href):
                files.append(href)
        
        files.sort()
        return files
        
    except Exception as e:
        print(f'   ❌ Erro ao listar arquivos: {e}')
        return []


def download_file(url, dest_path):
    """
    Baixa um arquivo se ele não existir ou estiver corrompido
    Retorna True se baixou/já existia, False se falhou
    """
    file_name = dest_path.name
    
    # Verificar se já existe e está OK
    if dest_path.exists():
        try:
            # Tentar abrir o zip para verificar integridade
            with zipfile.ZipFile(dest_path, 'r') as zip_ref:
                if zip_ref.testzip() is None:
                    print(f'   ✓ Arquivo já existe e está íntegro')
                    return True
        except zipfile.BadZipFile:
            print(f'   ⚠️  Arquivo corrompido, baixando novamente...')
            dest_path.unlink()
    
    # Baixar arquivo
    try:
        print(f'   ⬇️  Baixando...')
        
        # Usar wget para download com barra de progresso
        wget.download(url, out=str(dest_path.parent), bar=bar_progress)
        print()  # Nova linha após o progresso
        
        # Verificar integridade do arquivo baixado
        with zipfile.ZipFile(dest_path, 'r') as zip_ref:
            if zip_ref.testzip() is not None:
                print(f'   ❌ Arquivo baixado está corrompido!')
                dest_path.unlink()
                return False
        
        print(f'   ✅ Download concluído com sucesso')
        return True
        
    except Exception as e:
        print(f'   ❌ Erro no download: {str(e)[:100]}')
        if dest_path.exists():
            dest_path.unlink()
        return False


def extract_file(zip_path, extract_dir):
    """
    Extrai um arquivo zip
    Retorna True se extraiu com sucesso, False se falhou
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Verificar integridade
            if zip_ref.testzip() is not None:
                print(f'   ❌ Arquivo corrompido')
                return False
            
            # Verificar se já foi extraído
            members = zip_ref.namelist()
            already_extracted = all(
                (extract_dir / m).exists() 
                for m in members if not m.endswith('/')
            )
            
            if already_extracted:
                print(f'   ✓ Já extraído')
                return True
            
            # Extrair
            print(f'   📦 Extraindo...')
            zip_ref.extractall(extract_dir)
            print(f'   ✅ Extraído com sucesso')
            return True
            
    except Exception as e:
        print(f'   ❌ Erro na extração: {str(e)[:100]}')
        return False


def bar_progress(current, total, width=80):
    """Barra de progresso para wget"""
    progress_message = f"   {current / total * 100:.1f}% [{current:,} / {total:,}] bytes"
    sys.stdout.write("\r" + progress_message)
    sys.stdout.flush()


def format_size(bytes_size):
    """Formata tamanho em bytes para formato legível"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.1f} TB"


def format_time(seconds):
    """Formata tempo em segundos para formato legível"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}min"
    else:
        return f"{seconds/3600:.1f}h"


# =============================================================================
# FUNÇÃO PRINCIPAL
# =============================================================================

def main():
    """Função principal que coordena o download"""
    print('=' * 80)
    print('CRAWLER - ESTABELECIMENTOS RECEITA FEDERAL')
    print('=' * 80)
    print()
    
    start_time = time.time()
    
    # Criar diretórios
    make_dirs()
    
    # Listar todas as pastas disponíveis
    folders = get_available_folders(BASE_URL)
    
    if not folders:
        print('❌ Nenhuma pasta encontrada. Encerrando.')
        sys.exit(1)
    
    print()
    print(f'📋 Serão processadas {len(folders)} pastas')
    print()
    
    # Estatísticas
    total_files = 0
    downloaded_files = 0
    skipped_files = 0
    failed_files = 0
    extracted_files = 0
    
    # Processar cada pasta
    for folder_idx, folder in enumerate(folders, 1):
        folder_url = urljoin(BASE_URL, folder)
        folder_name = folder.rstrip('/')
        
        print('-' * 80)
        print(f'[{folder_idx}/{len(folders)}] Processando pasta: {folder_name}')
        print('-' * 80)
        
        # Criar subdiretório para esta pasta
        folder_download_dir = DOWNLOAD_DIR / folder_name
        folder_extract_dir = EXTRACTED_DIR / folder_name
        folder_download_dir.mkdir(parents=True, exist_ok=True)
        folder_extract_dir.mkdir(parents=True, exist_ok=True)
        
        # Listar arquivos de estabelecimentos
        files = get_estabelecimento_files(folder_url)
        
        if not files:
            print(f'⚠️  Nenhum arquivo de Estabelecimentos encontrado')
            print()
            continue
        
        print(f'📦 Encontrados {len(files)} arquivos de Estabelecimentos')
        print()
        
        # Baixar cada arquivo
        for file_idx, file_name in enumerate(files, 1):
            total_files += 1
            file_url = urljoin(folder_url, file_name)
            dest_path = folder_download_dir / file_name
            
            print(f'  [{file_idx}/{len(files)}] {file_name}')
            
            # Verificar se já existe
            if dest_path.exists():
                try:
                    with zipfile.ZipFile(dest_path, 'r') as zip_ref:
                        if zip_ref.testzip() is None:
                            print(f'   ✓ Já baixado e íntegro')
                            skipped_files += 1
                            
                            # Tentar extrair
                            if extract_file(dest_path, folder_extract_dir):
                                extracted_files += 1
                            
                            print()
                            continue
                except:
                    pass
            
            # Baixar arquivo
            if download_file(file_url, dest_path):
                downloaded_files += 1
                
                # Extrair arquivo
                if extract_file(dest_path, folder_extract_dir):
                    extracted_files += 1
            else:
                failed_files += 1
            
            print()
        
        print()
    
    # Resumo final
    elapsed_time = time.time() - start_time
    
    print('=' * 80)
    print('RESUMO FINAL')
    print('=' * 80)
    print(f'📊 Estatísticas:')
    print(f'   Total de arquivos processados: {total_files}')
    print(f'   ✅ Baixados:                   {downloaded_files}')
    print(f'   ⏭️  Pulados (já existiam):      {skipped_files}')
    print(f'   ❌ Falhas:                      {failed_files}')
    print(f'   📦 Extraídos:                   {extracted_files}')
    print()
    print(f'⏱️  Tempo total: {format_time(elapsed_time)}')
    print()
    print(f'📁 Arquivos salvos em: {DOWNLOAD_DIR}')
    print(f'📁 Extraídos em:       {EXTRACTED_DIR}')
    print()
    
    # Calcular espaço usado
    total_size = sum(
        f.stat().st_size 
        for f in DOWNLOAD_DIR.rglob('*') 
        if f.is_file()
    )
    print(f'💾 Espaço utilizado: {format_size(total_size)}')
    print()
    print('✅ Processo concluído!')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print()
        print('⚠️  Processo interrompido pelo usuário')
        sys.exit(1)
    except Exception as e:
        print()
        print(f'❌ Erro fatal: {e}')
        sys.exit(1)

