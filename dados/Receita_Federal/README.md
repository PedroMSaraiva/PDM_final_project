# Crawler - Estabelecimentos Receita Federal

Script Python para baixar automaticamente todos os arquivos de **Estabelecimentos** da Receita Federal de todos os anos e meses disponíveis.

## 🎯 Funcionalidades

- ✅ Busca automaticamente todas as pastas ano-mês disponíveis no servidor
- ✅ Baixa apenas arquivos de Estabelecimentos (Estabelecimentos0.zip, Estabelecimentos1.zip, etc.)
- ✅ Verifica integridade dos arquivos antes e depois do download
- ✅ Extrai automaticamente os arquivos baixados
- ✅ Evita redownloads de arquivos já baixados
- ✅ Sistema de retry automático em caso de falhas
- ✅ Barra de progresso visual
- ✅ Relatório detalhado ao final

## 📋 Requisitos

- Python 3.7+
- Dependências listadas em `requirements.txt`

## 🚀 Como Usar

### 1. Instalar dependências

```bash
pip install -r requirements.txt
```

### 2. Executar o script

```bash
python3 crawler_receita_estabelecimentos.py
```

Ou diretamente (se tiver permissão de execução):

```bash
./crawler_receita_estabelecimentos.py
```

## 📁 Estrutura de Diretórios

Após a execução, a seguinte estrutura será criada:

```
Receita_Federal/
├── crawler_receita_estabelecimentos.py
├── requirements.txt
├── downloads/
│   ├── 2024-01/
│   │   ├── Estabelecimentos0.zip
│   │   ├── Estabelecimentos1.zip
│   │   └── ...
│   ├── 2024-02/
│   │   └── ...
│   └── ...
└── extracted/
    ├── 2024-01/
    │   ├── arquivo1.csv
    │   └── ...
    └── ...
```

## ⚙️ Configurações

Você pode modificar as seguintes constantes no início do script:

```python
BASE_URL = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/'
DOWNLOAD_DIR = Path(__file__).parent / 'downloads'
EXTRACTED_DIR = Path(__file__).parent / 'extracted'
MAX_RETRIES = 3
TIMEOUT = 60
```

## 📊 Relatório de Execução

O script exibe um relatório completo ao final:

```
================================================================================
RESUMO FINAL
================================================================================
📊 Estatísticas:
   Total de arquivos processados: 100
   ✅ Baixados:                   80
   ⏭️  Pulados (já existiam):      15
   ❌ Falhas:                      5
   📦 Extraídos:                   95

⏱️  Tempo total: 2.5h

📁 Arquivos salvos em: /path/to/downloads
📁 Extraídos em:       /path/to/extracted

💾 Espaço utilizado: 25.3 GB

✅ Processo concluído!
```

## 🔧 Tratamento de Erros

O script possui:

- **Retry automático**: Tenta até 3 vezes em caso de falha de rede
- **Verificação de integridade**: Testa arquivos ZIP antes e depois do download
- **Recuperação de erros**: Remove arquivos corrompidos e tenta novamente
- **Skip inteligente**: Não reprocessa arquivos já baixados e validados

## ⚠️ Observações

- **Espaço em disco**: Os arquivos de Estabelecimentos são grandes (vários GB). Certifique-se de ter espaço suficiente.
- **Tempo de execução**: O processo completo pode levar várias horas dependendo da sua conexão.
- **Interrupção**: Você pode interromper com `Ctrl+C` e retomar depois - arquivos já baixados não serão reprocessados.

## 🐛 Solução de Problemas

### Erro de conexão
```
❌ Erro ao listar pastas: Connection timed out
```
**Solução**: Verifique sua conexão com a internet e tente novamente.

### Arquivo corrompido
```
❌ Arquivo baixado está corrompido!
```
**Solução**: O script automaticamente remove e tenta baixar novamente.

### Falta de espaço em disco
```
OSError: [Errno 28] No space left on device
```
**Solução**: Libere espaço em disco ou altere `DOWNLOAD_DIR` para outro local.

## 📝 Licença

Script de uso livre para fins educacionais e pesquisa.

## 👨‍💻 Contribuições

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou pull requests.

