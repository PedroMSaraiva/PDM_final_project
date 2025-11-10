#!/usr/bin/env python3
"""
Script para baixar e descompactar dados da PGFN (Procuradoria-Geral da Fazenda Nacional)
Baixa dados de 2020 a 2025 (até 3º trimestre) dos três tipos:
- Não Previdenciário
- FGTS
- Previdenciário
"""

import os
import requests
import zipfile
from pathlib import Path
from typing import List, Tuple
import time
from datetime import timedelta

# Configurações
BASE_URL = "https://dadosabertos.pgfn.gov.br"
BASE_DIR = Path(__file__).parent
MAX_RETRIES = 3  # Número máximo de tentativas em caso de erro

# Tipos de dados disponíveis
DATA_TYPES = [
    "Dados_abertos_Nao_Previdenciario",
    "Dados_abertos_FGTS",
    "Dados_abertos_Previdenciario"
]

def get_downloads_list() -> List[Tuple[int, int, str]]:
    """
    Gera lista de downloads (ano, trimestre, tipo)
    2020-2024: 4 trimestres cada
    2025: apenas 3 trimestres
    """
    downloads = []
    
    # 2020 a 2024 - todos os 4 trimestres
    for year in range(2020, 2025):
        for quarter in range(1, 5):
            for data_type in DATA_TYPES:
                downloads.append((year, quarter, data_type))
    
    # 2025 - apenas 3 trimestres
    for quarter in range(1, 4):
        for data_type in DATA_TYPES:
            downloads.append((2025, quarter, data_type))
    
    return downloads

def build_url(year: int, quarter: int, data_type: str) -> str:
    """Constrói a URL de download baseada no padrão"""
    quarter_str = f"{quarter:02d}"  # Formato: 01, 02, 03, 04
    return f"{BASE_URL}/{year}_trimestre_{quarter_str}/{data_type}.zip"

def get_target_directory(year: int, quarter: int) -> Path:
    """Retorna o diretório de destino para o arquivo"""
    return BASE_DIR / str(year) / f"{quarter}trimestre"

def format_time(seconds: float) -> str:
    """Formata segundos em um formato legível"""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}min"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"

def format_eta(seconds_elapsed: float, items_done: int, items_total: int) -> str:
    """Calcula e formata o tempo estimado restante"""
    if items_done == 0:
        return "calculando..."
    
    avg_time_per_item = seconds_elapsed / items_done
    items_remaining = items_total - items_done
    seconds_remaining = avg_time_per_item * items_remaining
    
    return format_time(seconds_remaining)

def download_file(url: str, destination: Path, filename: str, retry_count: int = 0) -> bool:
    """
    Baixa um arquivo da URL e salva no destino
    Retorna True se bem-sucedido, False caso contrário
    Tenta novamente em caso de falha (até MAX_RETRIES vezes)
    """
    file_path = destination / filename
    
    # Verifica se o arquivo já existe e se é válido
    if file_path.exists():
        # Tenta verificar se o arquivo é um ZIP válido
        try:
            with zipfile.ZipFile(file_path, 'r') as zf:
                # Testa a integridade do arquivo
                if zf.testzip() is None:
                    print(f"   ✓ Arquivo já existe e é válido: {file_path.name}")
                    return True
                else:
                    print(f"   ⚠️  Arquivo corrompido detectado, removendo: {file_path.name}")
                    file_path.unlink()
        except zipfile.BadZipFile:
            print(f"   ⚠️  Arquivo ZIP corrompido detectado, removendo: {file_path.name}")
            file_path.unlink()
        except Exception:
            # Se não conseguir verificar, assume que existe e está ok
            print(f"   ✓ Arquivo já existe: {file_path.name}")
            return True
    
    try:
        retry_msg = f" (tentativa {retry_count + 1}/{MAX_RETRIES})" if retry_count > 0 else ""
        print(f"   ⬇ Baixando{retry_msg}: {url}")
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        
        # Cria o diretório se não existir
        destination.mkdir(parents=True, exist_ok=True)
        
        # Baixa o arquivo em chunks
        total_size = int(response.headers.get('content-length', 0))
        chunk_size = 8192
        downloaded = 0
        
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        percent = (downloaded / total_size) * 100
                        print(f"   Progress: {percent:.1f}%", end='\r')
        
        # Verifica integridade do arquivo baixado
        try:
            with zipfile.ZipFile(file_path, 'r') as zf:
                if zf.testzip() is not None:
                    raise zipfile.BadZipFile("Arquivo corrompido após download")
            
            print(f"   ✓ Download concluído: {file_path.name} ({downloaded / 1024 / 1024:.1f} MB)")
            return True
            
        except zipfile.BadZipFile as e:
            print(f"   ✗ Arquivo baixado está corrompido: {e}")
            file_path.unlink()
            
            # Tentar novamente se ainda tiver tentativas
            if retry_count < MAX_RETRIES - 1:
                print(f"   🔄 Tentando novamente...")
                time.sleep(2)  # Aguarda um pouco antes de tentar novamente
                return download_file(url, destination, filename, retry_count + 1)
            else:
                print(f"   ✗ Máximo de tentativas atingido para {filename}")
                return False
        
    except requests.exceptions.RequestException as e:
        print(f"   ✗ Erro ao baixar {url}: {e}")
        # Remove arquivo parcial se existir
        if file_path.exists():
            file_path.unlink()
        
        # Tentar novamente se ainda tiver tentativas
        if retry_count < MAX_RETRIES - 1:
            print(f"   🔄 Tentando novamente...")
            time.sleep(2)
            return download_file(url, destination, filename, retry_count + 1)
        else:
            print(f"   ✗ Máximo de tentativas atingido para {filename}")
            return False
            
    except Exception as e:
        print(f"   ✗ Erro inesperado: {e}")
        if file_path.exists():
            file_path.unlink()
        
        # Tentar novamente se ainda tiver tentativas
        if retry_count < MAX_RETRIES - 1:
            print(f"   🔄 Tentando novamente...")
            time.sleep(2)
            return download_file(url, destination, filename, retry_count + 1)
        else:
            print(f"   ✗ Máximo de tentativas atingido para {filename}")
            return False

def unzip_file(zip_path: Path, extract_to: Path, delete_after: bool = True) -> bool:
    """
    Descompacta um arquivo zip e opcionalmente deleta o arquivo original
    Retorna True se bem-sucedido, False caso contrário
    """
    try:
        # Verifica se já foi extraído (procura por CSVs no diretório)
        csv_files = list(extract_to.glob("*.csv"))
        if csv_files:
            print(f"   ✓ Já extraído: {zip_path.name}")
            # Se já foi extraído, pode deletar o ZIP
            if delete_after and zip_path.exists():
                zip_path.unlink()
                print(f"   🗑️  ZIP deletado: {zip_path.name}")
            return True
        
        print(f"   📦 Descompactando: {zip_path.name}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Testa integridade antes de extrair
            if zip_ref.testzip() is not None:
                raise zipfile.BadZipFile("Arquivo contém dados corrompidos")
            zip_ref.extractall(extract_to)
        
        print(f"   ✓ Extraído com sucesso: {zip_path.name}")
        
        # Deletar o arquivo ZIP após extração bem-sucedida
        if delete_after and zip_path.exists():
            zip_path.unlink()
            print(f"   🗑️  ZIP deletado: {zip_path.name}")
        
        return True
        
    except zipfile.BadZipFile as e:
        print(f"   ✗ Arquivo ZIP corrompido: {zip_path.name} - {e}")
        # Remove o arquivo corrompido
        if zip_path.exists():
            zip_path.unlink()
            print(f"   🗑️  Arquivo corrompido removido: {zip_path.name}")
        return False
    except Exception as e:
        print(f"   ✗ Erro ao extrair {zip_path.name}: {e}")
        return False

def main():
    print("=" * 80)
    print("DOWNLOAD E EXTRAÇÃO DE DADOS DA PGFN")
    print("=" * 80)
    print(f"Diretório base: {BASE_DIR}")
    print(f"Máximo de tentativas por arquivo: {MAX_RETRIES}")
    print()
    
    downloads = get_downloads_list()
    total = len(downloads)
    successful_downloads = 0
    failed_downloads = 0
    successful_extractions = 0
    failed_extractions = 0
    
    # Iniciar cronômetro
    start_time = time.time()
    
    print(f"Total de arquivos para processar: {total}")
    print()
    
    for idx, (year, quarter, data_type) in enumerate(downloads, 1):
        item_start_time = time.time()
        
        # Calcular tempo decorrido e ETA
        elapsed_time = time.time() - start_time
        eta = format_eta(elapsed_time, idx - 1, total)
        
        print(f"[{idx}/{total}] Processando: {year} - Trimestre {quarter} - {data_type}")
        print(f"   ⏱️  Tempo decorrido: {format_time(elapsed_time)} | ETA: {eta}")
        
        url = build_url(year, quarter, data_type)
        target_dir = get_target_directory(year, quarter)
        filename = f"{data_type}.zip"
        zip_path = target_dir / filename
        
        # Verificar se os arquivos já foram extraídos (ANTES de tentar baixar)
        extract_dir = target_dir / data_type.replace("Dados_abertos_", "")
        csv_files = list(extract_dir.glob("*.csv")) if extract_dir.exists() else []
        
        if csv_files:
            print(f"   ✓ Arquivos já extraídos, pulando download")
            successful_downloads += 1
            successful_extractions += 1
            item_elapsed = time.time() - item_start_time
            print(f"   ⏱️  Tempo do item: {format_time(item_elapsed)}")
            print()
            continue  # Pula para o próximo arquivo
        
        # Download (com retry automático se corrompido)
        if download_file(url, target_dir, filename):
            successful_downloads += 1
            
            # Descompactar (deleta ZIP após extração)
            if zip_path.exists():
                # Cria subpasta para extração
                if unzip_file(zip_path, extract_dir, delete_after=True):
                    successful_extractions += 1
                else:
                    failed_extractions += 1
                    # Se falhou a extração, tentar baixar novamente
                    if zip_path.exists():
                        zip_path.unlink()
                    print(f"   🔄 Tentando re-baixar arquivo corrompido...")
                    if download_file(url, target_dir, filename):
                        if unzip_file(zip_path, extract_dir, delete_after=True):
                            successful_extractions += 1
                            failed_extractions -= 1
        else:
            failed_downloads += 1
        
        item_elapsed = time.time() - item_start_time
        print(f"   ⏱️  Tempo do item: {format_time(item_elapsed)}")
        print()
        
        # Pausa pequena entre downloads para não sobrecarregar o servidor
        if idx < total:
            time.sleep(1)
    
    # Tempo total
    total_time = time.time() - start_time
    
    # Resumo final
    print("=" * 80)
    print("RESUMO DA EXECUÇÃO")
    print("=" * 80)
    print(f"⏱️  Tempo total de execução: {format_time(total_time)}")
    print()
    print(f"Downloads bem-sucedidos: {successful_downloads}/{total}")
    print(f"Downloads falhos: {failed_downloads}/{total}")
    print(f"Extrações bem-sucedidas: {successful_extractions}/{successful_downloads if successful_downloads > 0 else total}")
    print(f"Extrações falhas: {failed_extractions}/{successful_downloads if successful_downloads > 0 else total}")
    print("=" * 80)
    
    if failed_downloads > 0:
        print("\n⚠️  Alguns downloads falharam após todas as tentativas. Verifique os erros acima.")
        print(f"   Execute o script novamente para tentar apenas os arquivos que falharam.")
    elif failed_extractions > 0:
        print("\n⚠️  Alguns arquivos não puderam ser extraídos. Verifique os erros acima.")
    else:
        print("\n✓ Todos os arquivos foram baixados, extraídos e os ZIPs foram removidos com sucesso!")
        print(f"📊 Total de espaço economizado: os arquivos ZIP foram deletados automaticamente.")

if __name__ == "__main__":
    main()

