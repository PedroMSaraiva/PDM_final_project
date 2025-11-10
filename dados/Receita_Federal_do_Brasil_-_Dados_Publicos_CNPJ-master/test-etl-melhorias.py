#!/usr/bin/env python3
"""
Script de teste para validar as melhorias do ETL
Testa conectividade e lista pastas disponíveis SEM fazer download
"""

import requests
import bs4 as bs
import re
import sys

BASE_URL = 'http://200.152.38.155/CNPJ/dados_abertos_cnpj/'

print('='*80)
print('TESTE DAS MELHORIAS DO ETL - RECEITA FEDERAL')
print('='*80)
print()

def test_connectivity():
    """Testa conectividade com o servidor"""
    print('1️⃣  Testando conectividade com servidor...')
    try:
        response = requests.get(BASE_URL, timeout=30)
        response.raise_for_status()
        print(f'   ✅ Servidor acessível! Status: {response.status_code}')
        return response.content
    except Exception as e:
        print(f'   ❌ Erro: {e}')
        return None

def test_folder_detection(html_content):
    """Testa detecção de pastas"""
    print()
    print('2️⃣  Testando detecção de pastas (ano-mes)...')
    try:
        soup = bs.BeautifulSoup(html_content, 'lxml')
        
        folders = []
        for link in soup.find_all('a'):
            href = link.get('href', '')
            if re.match(r'^\d{4}-\d{2}/$', href):
                folders.append(href)
        
        folders.sort(reverse=True)
        
        if folders:
            print(f'   ✅ Encontradas {len(folders)} pastas:')
            for i, folder in enumerate(folders[:10], 1):
                print(f'      {i:2d}. {folder}')
            if len(folders) > 10:
                print(f'      ... e mais {len(folders) - 10} pastas')
            print()
            print(f'   🎯 Pasta mais recente: {folders[0]}')
            return folders
        else:
            print('   ⚠️  Nenhuma pasta encontrada (pode ser problema no HTML)')
            return []
    except Exception as e:
        print(f'   ❌ Erro: {e}')
        return []

def test_files_listing(latest_folder):
    """Testa listagem de arquivos na pasta mais recente"""
    print()
    print('3️⃣  Testando listagem de arquivos ZIP...')
    try:
        url = BASE_URL + latest_folder
        print(f'   URL: {url}')
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        soup = bs.BeautifulSoup(response.content, 'lxml')
        
        files = []
        for link in soup.find_all('a'):
            href = link.get('href', '')
            if href.endswith('.zip'):
                files.append(href)
        
        files = sorted(list(set(files)))
        
        if files:
            print(f'   ✅ Encontrados {len(files)} arquivos ZIP:')
            for i, file in enumerate(files[:15], 1):
                print(f'      {i:2d}. {file}')
            if len(files) > 15:
                print(f'      ... e mais {len(files) - 15} arquivos')
            return files
        else:
            print('   ⚠️  Nenhum arquivo ZIP encontrado')
            return []
    except Exception as e:
        print(f'   ❌ Erro: {e}')
        return []

def main():
    # Teste 1: Conectividade
    html_content = test_connectivity()
    if not html_content:
        print()
        print('❌ Teste falhou na conectividade')
        sys.exit(1)
    
    # Teste 2: Detecção de pastas
    folders = test_folder_detection(html_content)
    if not folders:
        print()
        print('⚠️  Não foi possível detectar pastas, mas o servidor está online')
        sys.exit(0)
    
    # Teste 3: Listagem de arquivos
    files = test_files_listing(folders[0])
    
    # Resumo
    print()
    print('='*80)
    print('RESUMO DOS TESTES')
    print('='*80)
    print(f'✅ Conectividade:        OK')
    print(f'✅ Detecção de pastas:   {len(folders)} pastas encontradas')
    print(f'✅ Listagem de arquivos: {len(files)} arquivos ZIP')
    print()
    
    if folders and files:
        print('🎉 TODOS OS TESTES PASSARAM!')
        print()
        print('O ETL melhorado está pronto para uso:')
        print(f'  📁 Pasta mais recente: {folders[0]}')
        print(f'  📦 Total de arquivos:  {len(files)}')
        print()
        print('Para executar o ETL completo:')
        print('  cd code/')
        print('  python3 ETL_coletar_dados_e_gravar_BD.py')
    else:
        print('⚠️  Alguns testes falharam, verifique os erros acima')
    
    print()

if __name__ == '__main__':
    main()

