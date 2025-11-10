import datetime
import gc
import pathlib
from dotenv import load_dotenv
from sqlalchemy import create_engine
import bs4 as bs
import ftplib
import gzip
import os
import pandas as pd
import psycopg2
import re
import sys
import time
import requests
import urllib.request
import wget
import zipfile


def make_request_with_retry(url, max_retries=5, timeout=60):
    '''
    Faz requisição HTTP com retry automático e melhor tratamento de erros
    '''
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
    '''
    Lista as pastas disponíveis (ano-mes) no servidor da Receita Federal
    Retorna lista ordenada de pastas (ex: ['2024-08/', '2024-09/', '2024-10/'])
    '''
    try:
        print(f'🔍 Buscando pastas disponíveis em: {base_url}')
        response = make_request_with_retry(base_url, max_retries=3, timeout=30)
        
        soup = bs.BeautifulSoup(response.content, 'lxml')
        
        # Procurar por links que parecem pastas de data (YYYY-MM/)
        folders = []
        for link in soup.find_all('a'):
            href = link.get('href', '')
            # Padrão: YYYY-MM/ (ex: 2024-08/)
            if re.match(r'^\d{4}-\d{2}/$', href):
                folders.append(href)
        
        # Ordenar para pegar a mais recente
        folders.sort(reverse=True)
        
        if folders:
            print(f'✓ Encontradas {len(folders)} pastas: {", ".join(folders[:5])}...')
        else:
            print('⚠️  Nenhuma pasta encontrada, tentando método alternativo...')
            # Fallback: tentar extrair de qualquer link com padrão YYYY-MM
            text = str(soup)
            folders = list(set(re.findall(r'(\d{4}-\d{2}/)', text)))
            folders.sort(reverse=True)
        
        return folders
    except Exception as e:
        print(f'❌ Erro ao listar pastas: {e}')
        return []

def select_data_folder(base_url, preferred_folder=None):
    '''
    Seleciona a pasta de dados a ser usada
    - Se preferred_folder for especificado, usa ele
    - Senão, tenta pegar a pasta mais recente automaticamente
    - Fallback para pasta padrão configurada
    '''
    # Se uma pasta específica foi configurada, usar ela
    if preferred_folder:
        folder_url = base_url + preferred_folder if not preferred_folder.startswith('http') else preferred_folder
        print(f'📁 Usando pasta configurada: {folder_url}')
        return folder_url
    
    # Tentar descobrir automaticamente a pasta mais recente
    folders = get_available_folders(base_url)
    
    if folders:
        latest_folder = folders[0]  # Primeira da lista (mais recente)
        folder_url = base_url + latest_folder
        print(f'✓ Selecionada pasta mais recente: {latest_folder}')
        return folder_url
    
    # Fallback: tentar algumas opções conhecidas
    print('⚠️  Tentando pastas conhecidas como fallback...')
    current_year = datetime.datetime.now().year
    current_month = datetime.datetime.now().month
    
    # Tentar mês atual e meses anteriores
    for month_offset in range(0, 6):
        year = current_year
        month = current_month - month_offset
        
        if month <= 0:
            month += 12
            year -= 1
        
        folder = f'{year}-{month:02d}/'
        test_url = base_url + folder
        
        try:
            response = requests.head(test_url, timeout=10)
            if response.status_code == 200:
                print(f'✓ Encontrada pasta disponível: {folder}')
                return test_url
        except:
            continue
    
    # Último fallback: usar pasta padrão
    fallback_folder = f'{current_year}-{current_month:02d}/'
    folder_url = base_url + fallback_folder
    print(f'⚠️  Usando fallback: {fallback_folder}')
    return folder_url

def check_diff(url, file_name):
    '''
    Verifica se o arquivo no servidor existe no disco e se ele tem o mesmo
    tamanho no servidor.
    '''
    if not os.path.isfile(file_name):
        return True # ainda nao foi baixado

    try:
        response = requests.head(url, timeout=30)
        response.raise_for_status()
        new_size = int(response.headers.get('content-length', 0))
        old_size = os.path.getsize(file_name)
        if new_size != old_size:
            os.remove(file_name)
            return True # tamanho diferentes
        return False # arquivos sao iguais
    except Exception as e:
        print(f'⚠️  Erro ao verificar arquivo {os.path.basename(file_name)}: {str(e)[:80]}')
        # Se não conseguir verificar, assume que precisa baixar
        return True


#%%
def makedirs(path):
    '''
    cria path caso seja necessario
    '''
    if not os.path.exists(path):
        os.makedirs(path)

#%%
def to_sql(dataframe, **kwargs):
    '''
    Quebra em pedacos a tarefa de inserir registros no banco
    '''
    size = 4096  #TODO param
    total = len(dataframe)
    name = kwargs.get('name')

    def chunker(df):
        return (df[i:i + size] for i in range(0, len(df), size))

    for i, df in enumerate(chunker(dataframe)):
        df.to_sql(**kwargs)
        index = i * size
        percent = (index * 100) / total
        progress = f'{name} {percent:.2f}% {index:0{len(str(total))}}/{total}'
        sys.stdout.write(f'\r{progress}')
    sys.stdout.write('\n')

#%%
# Ler arquivo de configuração de ambiente # https://dev.to/jakewitcher/using-env-files-for-environment-variables-in-python-applications-55a1
def getEnv(env, default=None):
    '''
    Obtém variável de ambiente com valor padrão opcional
    Args:
        env: nome da variável de ambiente
        default: valor padrão se a variável não existir
    Returns:
        valor da variável ou default
    '''
    return os.getenv(env, default)


# Procurar arquivo .env em locais possíveis (Docker-friendly)
possible_paths = [
    os.path.join(pathlib.Path().resolve(), '.env'),  # Diretório atual
    os.path.join('/opt/airflow/etl_scripts', '.env'),  # Docker: etl_scripts
    os.path.join('/opt/airflow', '.env'),  # Docker: raiz do airflow
]

dotenv_path = None
for path in possible_paths:
    if os.path.isfile(path):
        dotenv_path = path
        break

if dotenv_path:
    print(f'Arquivo .env encontrado: {dotenv_path}')
    load_dotenv(dotenv_path=dotenv_path)
else:
    print('Arquivo .env não encontrado. Usando variáveis de ambiente do sistema.')
    # Tenta carregar variáveis de ambiente diretamente (para ambientes Docker)
    load_dotenv()

#%%
# ========================================================================
# CONFIGURAÇÃO DA URL DA RECEITA FEDERAL
# ========================================================================
print('='*80)
print('CONFIGURAÇÃO DO ETL - RECEITA FEDERAL')
print('='*80)

# URL base da Receita Federal
BASE_URL_RFB = 'http://200.152.38.155/CNPJ/dados_abertos_cnpj/'

# Pasta específica (ano-mes) - deixe None para auto-detectar a mais recente
# Exemplos: '2024-08/', '2024-09/', '2024-10/', etc
PREFERRED_FOLDER = getEnv('RECEITA_FOLDER')  # Ex: 2024-08/

print()

# Read details from ".env" file:
output_files = None
extracted_files = None
try:
    # Detectar se está rodando no Docker ou localmente
    is_docker = os.path.exists('/opt/airflow') or os.getenv('AIRFLOW_HOME') is not None
    
    # Definir valores padrão baseado no ambiente
    if is_docker:
        default_downloads = "/opt/airflow/data/downloads"
        default_extracted = "/opt/airflow/data/extracted"
    else:
        # Se for local, usar diretórios relativos ao script
        script_dir = pathlib.Path(__file__).parent.resolve()
        default_downloads = str(script_dir / "downloads")
        default_extracted = str(script_dir / "extracted")
    
    output_files = getEnv('OUTPUT_FILES_PATH', default_downloads)
    if not output_files:
        raise ValueError('OUTPUT_FILES_PATH não definido')
    makedirs(output_files)

    extracted_files = getEnv('EXTRACTED_FILES_PATH', default_extracted)
    if not extracted_files:
        raise ValueError('EXTRACTED_FILES_PATH não definido')
    makedirs(extracted_files)

    ambiente = "Docker" if is_docker else "Local"
    print(f'✓ Ambiente detectado: {ambiente}')
    print('✓ Diretórios definidos:')
    print(f'  📂 Downloads: {output_files}')
    print(f'  📂 Extraídos: {extracted_files}')
except Exception as e:
    print(f'❌ Erro na definição dos diretórios: {e}')
    print('Verifique o arquivo ".env" ou configure as variáveis de ambiente.')
    sys.exit(1)

print()

#%%
# ========================================================================
# DESCOBRIR E CONECTAR À PASTA DE DADOS
# ========================================================================
print('🔗 Conectando ao servidor da Receita Federal...')
print(f'   URL base: {BASE_URL_RFB}')
print()

try:
    # Seleciona automaticamente a pasta mais recente ou usa a configurada
    dados_rf = select_data_folder(BASE_URL_RFB, PREFERRED_FOLDER)
    print()
    
    # Tentar acessar a URL selecionada
    print(f'📡 Testando conexão com: {dados_rf}')
    response = make_request_with_retry(dados_rf, max_retries=5, timeout=60)
    raw_html = response.content
    print('✅ Conexão estabelecida com sucesso!')
    print()
    
except requests.exceptions.Timeout:
    print('❌ ERRO: Timeout - Servidor não respondeu após várias tentativas.')
    print('   Possíveis causas:')
    print('   - Servidor temporariamente sobrecarregado')
    print('   - Problemas de conexão de internet')
    print('   Tente novamente mais tarde ou em horário alternativo.')
    sys.exit(1)
except requests.exceptions.ConnectionError as e:
    print(f'❌ ERRO: Não foi possível conectar ao servidor.')
    print(f'   Detalhes: {str(e)[:150]}')
    print('   Verifique sua conexão com a internet.')
    sys.exit(1)
except Exception as e:
    print(f'❌ ERRO inesperado: {e}')
    print('   Verifique a URL ou tente novamente mais tarde.')
    sys.exit(1)

#%%
# ========================================================================
# LISTAR ARQUIVOS DISPONÍVEIS
# ========================================================================
print('📋 Listando arquivos ZIP disponíveis...')

# Formatar página e converter em string
page_items = bs.BeautifulSoup(raw_html, 'lxml')

# Método mais robusto: procurar diretamente pelos links <a> 
Files = []
for link in page_items.find_all('a'):
    href = link.get('href', '')
    if href.endswith('.zip'):
        # Remove caracteres indesejados
        href = href.replace('.zip">', '')
        if href and not href.startswith('http'):
            Files.append(href)

# Fallback: método antigo se não encontrar nada
if not Files:
    print('⚠️  Método primário não encontrou arquivos, usando fallback...')
    html_str = str(page_items)
    text = '.zip'
    for m in re.finditer(text, html_str):
        i_start = m.start()-40
        i_end = m.end()
        i_loc = html_str[i_start:i_end].find('href=')+6
        file_name = html_str[i_start+i_loc:i_end]
        # Limpar nome do arquivo
        if not file_name.find('.zip">') > -1:
            Files.append(file_name)

# Remover duplicatas e ordenar
Files = sorted(list(set(Files)))

if not Files:
    print('❌ ERRO: Nenhum arquivo .zip encontrado na URL.')
    print(f'   URL verificada: {dados_rf}')
    print('   Verifique se a URL está correta e contém arquivos .zip')
    sys.exit(1)

print(f'✓ Encontrados {len(Files)} arquivos para download:')
for i, f in enumerate(Files, 1):
    print(f'  {i:3d}. {f}')
print()

#%%
# ========================================================================
# DOWNLOAD DOS ARQUIVOS
# ========================================================================
def bar_progress(current, total, width=80):
    '''Barra de progresso para wget'''
    progress_message = "   Progresso: %d%% [%d / %d] bytes" % (current / total * 100, current, total)
    sys.stdout.write("\r" + progress_message)
    sys.stdout.flush()

print('='*80)
print('INICIANDO DOWNLOAD DOS ARQUIVOS')
print('='*80)
print(f'Total de arquivos: {len(Files)}')
print(f'Destino: {output_files}')
print()

download_start = time.time()
downloaded = 0
skipped = 0
failed = []

for i, file_name in enumerate(Files, 1):
    print(f'[{i}/{len(Files)}] {file_name}')
    
    url = dados_rf + file_name
    file_path = os.path.join(output_files, file_name)
    
    try:
        if check_diff(url, file_path):
            print(f'   ⬇  Baixando de: {url}')
            wget.download(url, out=output_files, bar=bar_progress)
            print()  # Nova linha após download
            downloaded += 1
            print(f'   ✅ Download concluído')
        else:
            print(f'   ✓  Arquivo já existe e está atualizado')
            skipped += 1
    except Exception as e:
        print(f'   ❌ Erro no download: {str(e)[:100]}')
        failed.append(file_name)
    
    print()

download_time = time.time() - download_start
print('='*80)
print('RESUMO DO DOWNLOAD')
print('='*80)
print(f'✓ Baixados:  {downloaded}')
print(f'○ Pulados:   {skipped} (já existiam)')
print(f'✗ Falhos:    {len(failed)}')
print(f'⏱  Tempo:     {download_time:.1f}s ({download_time/60:.1f} min)')
if failed:
    print(f'\nArquivos que falharam:')
    for f in failed:
        print(f'  - {f}')
print()

#%%
# ========================================================================
# EXTRAÇÃO DOS ARQUIVOS
# ========================================================================
print('='*80)
print('EXTRAINDO ARQUIVOS')
print('='*80)
print(f'Destino: {extracted_files}')
print()

extract_start = time.time()
extracted = 0
extract_failed = []

for i, file_name in enumerate(Files, 1):
    print(f'[{i}/{len(Files)}] {file_name}')
    
    full_path = os.path.join(output_files, file_name)
    
    if not os.path.exists(full_path):
        print(f'   ⚠️  Arquivo não existe, pulando')
        continue
    
    try:
        with zipfile.ZipFile(full_path, 'r') as zip_ref:
            # Testar integridade
            bad_file = zip_ref.testzip()
            if bad_file:
                raise zipfile.BadZipFile(f'Arquivo corrompido: {bad_file}')
            
            # Verificar se já foi extraído
            members = zip_ref.namelist()
            already_extracted = all(
                os.path.exists(os.path.join(extracted_files, m)) 
                for m in members if not m.endswith('/')
            )
            
            if already_extracted:
                print(f'   ✓  Já extraído')
            else:
                print(f'   📦 Extraindo...')
                zip_ref.extractall(extracted_files)
                print(f'   ✅ Extraído com sucesso')
                extracted += 1
    except zipfile.BadZipFile as e:
        print(f'   ❌ Arquivo ZIP corrompido: {e}')
        extract_failed.append(file_name)
    except Exception as e:
        print(f'   ❌ Erro na extração: {str(e)[:100]}')
        extract_failed.append(file_name)
    
    print()

extract_time = time.time() - extract_start
print('='*80)
print('RESUMO DA EXTRAÇÃO')
print('='*80)
print(f'✓ Extraídos: {extracted}')
print(f'✗ Falhos:    {len(extract_failed)}')
print(f'⏱  Tempo:    {extract_time:.1f}s ({extract_time/60:.1f} min)')
if extract_failed:
    print(f'\nArquivos que falharam na extração:')
    for f in extract_failed:
        print(f'  - {f}')
print()

#%%
########################################################################################################################
## LER E INSERIR DADOS #################################################################################################
########################################################################################################################
insert_start = time.time()

# Files:
Items = [name for name in os.listdir(extracted_files) if name.endswith('')]

# Separar arquivos:
arquivos_empresa = []
arquivos_estabelecimento = []
arquivos_socios = []
arquivos_simples = []
arquivos_cnae = []
arquivos_moti = []
arquivos_munic = []
arquivos_natju = []
arquivos_pais = []
arquivos_quals = []
for i in range(len(Items)):
    if Items[i].find('EMPRE') > -1:
        arquivos_empresa.append(Items[i])
    elif Items[i].find('ESTABELE') > -1:
        arquivos_estabelecimento.append(Items[i])
    elif Items[i].find('SOCIO') > -1:
        arquivos_socios.append(Items[i])
    elif Items[i].find('SIMPLES') > -1:
        arquivos_simples.append(Items[i])
    elif Items[i].find('CNAE') > -1:
        arquivos_cnae.append(Items[i])
    elif Items[i].find('MOTI') > -1:
        arquivos_moti.append(Items[i])
    elif Items[i].find('MUNIC') > -1:
        arquivos_munic.append(Items[i])
    elif Items[i].find('NATJU') > -1:
        arquivos_natju.append(Items[i])
    elif Items[i].find('PAIS') > -1:
        arquivos_pais.append(Items[i])
    elif Items[i].find('QUALS') > -1:
        arquivos_quals.append(Items[i])
    else:
        pass

#%%
# Conectar no banco de dados:
# Dados da conexão com o BD
# Valores padrão baseados no ambiente
if is_docker:
    default_host = "postgres-dados-rfb"  # Nome do serviço no docker-compose
else:
    default_host = "localhost"  # Localhost quando rodando fora do Docker

user=getEnv('DB_USER', "postgres")
passw=getEnv('DB_PASSWORD', "postgres")
host=getEnv('DB_HOST', default_host)
port=getEnv('DB_PORT', "5432")
database=getEnv('DB_NAME', "Dados_RFB")

# Conectar:
engine = create_engine('postgresql://'+user+':'+passw+'@'+host+':'+port+'/'+database)
conn = psycopg2.connect('dbname='+database+' '+'user='+user+' '+'host='+host+' '+'port='+port+' '+'password='+passw)
cur = conn.cursor()

# #%%
# # Arquivos de empresa:
empresa_insert_start = time.time()
print("""
#######################
## Arquivos de EMPRESA:
#######################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "empresa";')
conn.commit()

for e in range(0, len(arquivos_empresa)):
    print('Trabalhando no arquivo: '+arquivos_empresa[e]+' [...]')
    try:
        del empresa
    except:
        pass

    #empresa = pd.DataFrame(columns=[0, 1, 2, 3, 4, 5, 6])
    empresa_dtypes = {0: object, 1: object, 2: 'Int32', 3: 'Int32', 4: object, 5: 'Int32', 6: object}
    extracted_file_path = os.path.join(extracted_files, arquivos_empresa[e])

    empresa = pd.read_csv(filepath_or_buffer=extracted_file_path,
                          sep=';',
                          #nrows=100,
                          skiprows=0,
                          header=None,
                          dtype=empresa_dtypes,
                          encoding='latin-1',
    )

    # Tratamento do arquivo antes de inserir na base:
    empresa = empresa.reset_index()
    del empresa['index']

    # Renomear colunas
    empresa.columns = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel']

    # Replace "," por "."
    empresa['capital_social'] = empresa['capital_social'].apply(lambda x: x.replace(',','.'))
    empresa['capital_social'] = empresa['capital_social'].astype(float)

    # Gravar dados no banco:
    # Empresa
    to_sql(empresa, name='empresa', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_empresa[e] + ' inserido com sucesso no banco de dados!')

try:
    del empresa
except:
    pass
print('Arquivos de empresa finalizados!')
empresa_insert_end = time.time()
empresa_Tempo_insert = round((empresa_insert_end - empresa_insert_start))
print('Tempo de execução do processo de empresa (em segundos): ' + str(empresa_Tempo_insert))

#%%
# Arquivos de estabelecimento:
estabelecimento_insert_start = time.time()
print("""
###############################
## Arquivos de ESTABELECIMENTO:
###############################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "estabelecimento";')
conn.commit()

print('Tem %i arquivos de estabelecimento!' % len(arquivos_estabelecimento))
for e in range(0, len(arquivos_estabelecimento)):
    print('Trabalhando no arquivo: '+arquivos_estabelecimento[e]+' [...]')
    try:
        del estabelecimento
        gc.collect()
    except:
        pass

    # estabelecimento = pd.DataFrame(columns=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28])
    estabelecimento_dtypes = {0: object, 1: object, 2: object, 3: 'Int32', 4: object, 5: 'Int32', 6: 'Int32',
                              7: 'Int32', 8: object, 9: object, 10: 'Int32', 11: 'Int32', 12: object, 13: object,
                              14: object, 15: object, 16: object, 17: object, 18: object, 19: object,
                              20: 'Int32', 21: object, 22: object, 23: object, 24: object, 25: object,
                              26: object, 27: object, 28: object, 29: 'Int32'}
    extracted_file_path = os.path.join(extracted_files, arquivos_estabelecimento[e])

    NROWS = 2000000
    part = 0
    while True:
        estabelecimento = pd.read_csv(filepath_or_buffer=extracted_file_path,
                              sep=';',
                              nrows=NROWS,
                              skiprows=NROWS * part,
                              header=None,
                              dtype=estabelecimento_dtypes,
                              encoding='latin-1',
        )

        # Tratamento do arquivo antes de inserir na base:
        estabelecimento = estabelecimento.reset_index()
        del estabelecimento['index']
        gc.collect()

        # Renomear colunas
        estabelecimento.columns = ['cnpj_basico',
                                   'cnpj_ordem',
                                   'cnpj_dv',
                                   'identificador_matriz_filial',
                                   'nome_fantasia',
                                   'situacao_cadastral',
                                   'data_situacao_cadastral',
                                   'motivo_situacao_cadastral',
                                   'nome_cidade_exterior',
                                   'pais',
                                   'data_inicio_atividade',
                                   'cnae_fiscal_principal',
                                   'cnae_fiscal_secundaria',
                                   'tipo_logradouro',
                                   'logradouro',
                                   'numero',
                                   'complemento',
                                   'bairro',
                                   'cep',
                                   'uf',
                                   'municipio',
                                   'ddd_1',
                                   'telefone_1',
                                   'ddd_2',
                                   'telefone_2',
                                   'ddd_fax',
                                   'fax',
                                   'correio_eletronico',
                                   'situacao_especial',
                                   'data_situacao_especial']

        # Gravar dados no banco:
        # estabelecimento
        to_sql(estabelecimento, name='estabelecimento', con=engine, if_exists='append', index=False)
        print('Arquivo ' + arquivos_estabelecimento[e] + ' / ' + str(part) + ' inserido com sucesso no banco de dados!')
        if len(estabelecimento) == NROWS:
            part += 1
        else:
            break

try:
    del estabelecimento
except:
    pass
print('Arquivos de estabelecimento finalizados!')
estabelecimento_insert_end = time.time()
estabelecimento_Tempo_insert = round((estabelecimento_insert_end - estabelecimento_insert_start))
print('Tempo de execução do processo de estabelecimento (em segundos): ' + str(estabelecimento_Tempo_insert))

#%%
# Arquivos de socios:
socios_insert_start = time.time()
print("""
######################
## Arquivos de SOCIOS:
######################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "socios";')
conn.commit()

for e in range(0, len(arquivos_socios)):
    print('Trabalhando no arquivo: '+arquivos_socios[e]+' [...]')
    try:
        del socios
    except:
        pass

    socios_dtypes = {0: object, 1: 'Int32', 2: object, 3: object, 4: 'Int32', 5: 'Int32', 6: 'Int32',
                     7: object, 8: object, 9: 'Int32', 10: 'Int32'}
    extracted_file_path = os.path.join(extracted_files, arquivos_socios[e])
    socios = pd.read_csv(filepath_or_buffer=extracted_file_path,
                          sep=';',
                          #nrows=100,
                          skiprows=0,
                          header=None,
                          dtype=socios_dtypes,
                          encoding='latin-1',
    )

    # Tratamento do arquivo antes de inserir na base:
    socios = socios.reset_index()
    del socios['index']

    # Renomear colunas
    socios.columns = ['cnpj_basico',
                      'identificador_socio',
                      'nome_socio_razao_social',
                      'cpf_cnpj_socio',
                      'qualificacao_socio',
                      'data_entrada_sociedade',
                      'pais',
                      'representante_legal',
                      'nome_do_representante',
                      'qualificacao_representante_legal',
                      'faixa_etaria']

    # Gravar dados no banco:
    # socios
    to_sql(socios, name='socios', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_socios[e] + ' inserido com sucesso no banco de dados!')

try:
    del socios
except:
    pass
print('Arquivos de socios finalizados!')
socios_insert_end = time.time()
socios_Tempo_insert = round((socios_insert_end - socios_insert_start))
print('Tempo de execução do processo de sócios (em segundos): ' + str(socios_Tempo_insert))

#%%
# Arquivos de simples:
simples_insert_start = time.time()
print("""
################################
## Arquivos do SIMPLES NACIONAL:
################################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "simples";')
conn.commit()

for e in range(0, len(arquivos_simples)):
    print('Trabalhando no arquivo: '+arquivos_simples[e]+' [...]')
    try:
        del simples
    except:
        pass

    # Verificar tamanho do arquivo:
    print('Lendo o arquivo ' + arquivos_simples[e]+' [...]')
    simples_dtypes = ({0: object, 1: object, 2: 'Int32', 3: 'Int32', 4: object, 5: 'Int32', 6: 'Int32'})
    extracted_file_path = os.path.join(extracted_files, arquivos_simples[e])

    simples_lenght = sum(1 for line in open(extracted_file_path, "r"))
    print('Linhas no arquivo do Simples '+ arquivos_simples[e] +': '+str(simples_lenght))

    tamanho_das_partes = 1000000 # Registros por carga
    partes = round(simples_lenght / tamanho_das_partes)
    nrows = tamanho_das_partes
    skiprows = 0

    print('Este arquivo será dividido em ' + str(partes) + ' partes para inserção no banco de dados')

    for i in range(0, partes):
        print('Iniciando a parte ' + str(i+1) + ' [...]')
        simples = pd.DataFrame(columns=[1,2,3,4,5,6])

        simples = pd.read_csv(filepath_or_buffer=extracted_file_path,
                              sep=';',
                              nrows=nrows,
                              skiprows=skiprows,
                              header=None,
                              dtype=simples_dtypes,
                              encoding='latin-1',
        )

        # Tratamento do arquivo antes de inserir na base:
        simples = simples.reset_index()
        del simples['index']

        # Renomear colunas
        simples.columns = ['cnpj_basico',
                           'opcao_pelo_simples',
                           'data_opcao_simples',
                           'data_exclusao_simples',
                           'opcao_mei',
                           'data_opcao_mei',
                           'data_exclusao_mei']

        skiprows = skiprows+nrows

        # Gravar dados no banco:
        # simples
        to_sql(simples, name='simples', con=engine, if_exists='append', index=False)
        print('Arquivo ' + arquivos_simples[e] + ' inserido com sucesso no banco de dados! - Parte '+ str(i+1))

        try:
            del simples
        except:
            pass

try:
    del simples
except:
    pass

print('Arquivos do simples finalizados!')
simples_insert_end = time.time()
simples_Tempo_insert = round((simples_insert_end - simples_insert_start))
print('Tempo de execução do processo do Simples Nacional (em segundos): ' + str(simples_Tempo_insert))

#%%
# Arquivos de cnae:
cnae_insert_start = time.time()
print("""
######################
## Arquivos de cnae:
######################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "cnae";')
conn.commit()

for e in range(0, len(arquivos_cnae)):
    print('Trabalhando no arquivo: '+arquivos_cnae[e]+' [...]')
    try:
        del cnae
    except:
        pass

    extracted_file_path = os.path.join(extracted_files, arquivos_cnae[e])
    cnae = pd.DataFrame(columns=[1,2])
    cnae = pd.read_csv(filepath_or_buffer=extracted_file_path, sep=';', skiprows=0, header=None, dtype='object', encoding='latin-1')

    # Tratamento do arquivo antes de inserir na base:
    cnae = cnae.reset_index()
    del cnae['index']

    # Renomear colunas
    cnae.columns = ['codigo', 'descricao']

    # Gravar dados no banco:
    # cnae
    to_sql(cnae, name='cnae', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_cnae[e] + ' inserido com sucesso no banco de dados!')

try:
    del cnae
except:
    pass
print('Arquivos de cnae finalizados!')
cnae_insert_end = time.time()
cnae_Tempo_insert = round((cnae_insert_end - cnae_insert_start))
print('Tempo de execução do processo de cnae (em segundos): ' + str(cnae_Tempo_insert))

#%%
# Arquivos de moti:
moti_insert_start = time.time()
print("""
#########################################
## Arquivos de motivos da situação atual:
#########################################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "moti";')
conn.commit()

for e in range(0, len(arquivos_moti)):
    print('Trabalhando no arquivo: '+arquivos_moti[e]+' [...]')
    try:
        del moti
    except:
        pass

    moti_dtypes = ({0: 'Int32', 1: object})
    extracted_file_path = os.path.join(extracted_files, arquivos_moti[e])
    moti = pd.read_csv(filepath_or_buffer=extracted_file_path, sep=';', skiprows=0, header=None, dtype=moti_dtypes, encoding='latin-1')

    # Tratamento do arquivo antes de inserir na base:
    moti = moti.reset_index()
    del moti['index']

    # Renomear colunas
    moti.columns = ['codigo', 'descricao']

    # Gravar dados no banco:
    # moti
    to_sql(moti, name='moti', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_moti[e] + ' inserido com sucesso no banco de dados!')

try:
    del moti
except:
    pass
print('Arquivos de moti finalizados!')
moti_insert_end = time.time()
moti_Tempo_insert = round((moti_insert_end - moti_insert_start))
print('Tempo de execução do processo de motivos da situação atual (em segundos): ' + str(moti_Tempo_insert))

#%%
# Arquivos de munic:
munic_insert_start = time.time()
print("""
##########################
## Arquivos de municípios:
##########################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "munic";')
conn.commit()

for e in range(0, len(arquivos_munic)):
    print('Trabalhando no arquivo: '+arquivos_munic[e]+' [...]')
    try:
        del munic
    except:
        pass

    munic_dtypes = ({0: 'Int32', 1: object})
    extracted_file_path = os.path.join(extracted_files, arquivos_munic[e])
    munic = pd.read_csv(filepath_or_buffer=extracted_file_path, sep=';', skiprows=0, header=None, dtype=munic_dtypes, encoding='latin-1')

    # Tratamento do arquivo antes de inserir na base:
    munic = munic.reset_index()
    del munic['index']

    # Renomear colunas
    munic.columns = ['codigo', 'descricao']

    # Gravar dados no banco:
    # munic
    to_sql(munic, name='munic', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_munic[e] + ' inserido com sucesso no banco de dados!')

try:
    del munic
except:
    pass
print('Arquivos de munic finalizados!')
munic_insert_end = time.time()
munic_Tempo_insert = round((munic_insert_end - munic_insert_start))
print('Tempo de execução do processo de municípios (em segundos): ' + str(munic_Tempo_insert))

#%%
# Arquivos de natju:
natju_insert_start = time.time()
print("""
#################################
## Arquivos de natureza jurídica:
#################################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "natju";')
conn.commit()

for e in range(0, len(arquivos_natju)):
    print('Trabalhando no arquivo: '+arquivos_natju[e]+' [...]')
    try:
        del natju
    except:
        pass

    natju_dtypes = ({0: 'Int32', 1: object})
    extracted_file_path = os.path.join(extracted_files, arquivos_natju[e])
    natju = pd.read_csv(filepath_or_buffer=extracted_file_path, sep=';', skiprows=0, header=None, dtype=natju_dtypes, encoding='latin-1')

    # Tratamento do arquivo antes de inserir na base:
    natju = natju.reset_index()
    del natju['index']

    # Renomear colunas
    natju.columns = ['codigo', 'descricao']

    # Gravar dados no banco:
    # natju
    to_sql(natju, name='natju', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_natju[e] + ' inserido com sucesso no banco de dados!')

try:
    del natju
except:
    pass
print('Arquivos de natju finalizados!')
natju_insert_end = time.time()
natju_Tempo_insert = round((natju_insert_end - natju_insert_start))
print('Tempo de execução do processo de natureza jurídica (em segundos): ' + str(natju_Tempo_insert))

#%%
# Arquivos de pais:
pais_insert_start = time.time()
print("""
######################
## Arquivos de país:
######################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "pais";')
conn.commit()

for e in range(0, len(arquivos_pais)):
    print('Trabalhando no arquivo: '+arquivos_pais[e]+' [...]')
    try:
        del pais
    except:
        pass

    pais_dtypes = ({0: 'Int32', 1: object})
    extracted_file_path = os.path.join(extracted_files, arquivos_pais[e])
    pais = pd.read_csv(filepath_or_buffer=extracted_file_path, sep=';', skiprows=0, header=None, dtype=pais_dtypes, encoding='latin-1')

    # Tratamento do arquivo antes de inserir na base:
    pais = pais.reset_index()
    del pais['index']

    # Renomear colunas
    pais.columns = ['codigo', 'descricao']

    # Gravar dados no banco:
    # pais
    to_sql(pais, name='pais', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_pais[e] + ' inserido com sucesso no banco de dados!')

try:
    del pais
except:
    pass
print('Arquivos de pais finalizados!')
pais_insert_end = time.time()
pais_Tempo_insert = round((pais_insert_end - pais_insert_start))
print('Tempo de execução do processo de país (em segundos): ' + str(pais_Tempo_insert))

#%%
# Arquivos de qualificação de sócios:
quals_insert_start = time.time()
print("""
######################################
## Arquivos de qualificação de sócios:
######################################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "quals";')
conn.commit()

for e in range(0, len(arquivos_quals)):
    print('Trabalhando no arquivo: '+arquivos_quals[e]+' [...]')
    try:
        del quals
    except:
        pass

    quals_dtypes = ({0: 'Int32', 1: object})
    extracted_file_path = os.path.join(extracted_files, arquivos_quals[e])
    quals = pd.read_csv(filepath_or_buffer=extracted_file_path, sep=';', skiprows=0, header=None, dtype=quals_dtypes, encoding='latin-1')

    # Tratamento do arquivo antes de inserir na base:
    quals = quals.reset_index()
    del quals['index']

    # Renomear colunas
    quals.columns = ['codigo', 'descricao']

    # Gravar dados no banco:
    # quals
    to_sql(quals, name='quals', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_quals[e] + ' inserido com sucesso no banco de dados!')

try:
    del quals
except:
    pass
print('Arquivos de quals finalizados!')
quals_insert_end = time.time()
quals_Tempo_insert = round((quals_insert_end - quals_insert_start))
print('Tempo de execução do processo de qualificação de sócios (em segundos): ' + str(quals_Tempo_insert))

#%%
insert_end = time.time()
Tempo_insert = round((insert_end - insert_start))

print("""
#############################################
## Processo de carga dos arquivos finalizado!
#############################################
""")

print('Tempo total de execução do processo de carga (em segundos): ' + str(Tempo_insert)) # Tempo de execução do processo (em segundos): 17.770 (4hrs e 57 min)

# ###############################
# Tamanho dos arquivos:
# empresa = 45.811.638
# estabelecimento = 48.421.619
# socios = 20.426.417
# simples = 27.893.923
# ###############################

#%%
# Criar índices na base de dados:
index_start = time.time()
print("""
#######################################
## Criar índices na base de dados [...]
#######################################
""")
cur.execute("""
create index if not exists empresa_cnpj on empresa(cnpj_basico);
commit;
create index if not exists estabelecimento_cnpj on estabelecimento(cnpj_basico);
commit;
create index if not exists socios_cnpj on socios(cnpj_basico);
commit;
create index if not exists simples_cnpj on simples(cnpj_basico);
commit;
""")
conn.commit()
print("""
############################################################
## Índices criados nas tabelas, para a coluna `cnpj_basico`:
   - empresa
   - estabelecimento
   - socios
   - simples
############################################################
""")
index_end = time.time()
index_time = round(index_end - index_start)
print('Tempo para criar os índices (em segundos): ' + str(index_time))

#%%
print("""Processo 100% finalizado! Você já pode usar seus dados no BD!
 - Desenvolvido por: Aphonso Henrique do Amaral Rafael
 - Contribua com esse projeto aqui: https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ
""")