# 🔧 Solução para Erro de Timeout

## ✅ Progresso

**Erro anterior RESOLVIDO!** ✓  
O problema do `input()` foi corrigido. Agora o script consegue:
- ✅ Carregar variáveis de ambiente
- ✅ Configurar diretórios
- ✅ Iniciar o processamento

## ❌ Novo Erro: Timeout de Conexão

```
TimeoutError: [Errno 110] Connection timed out
urllib.error.URLError: <urlopen error [Errno 110] Connection timed out>
```

### 🔍 O que está acontecendo?

O script está tentando acessar o servidor da Receita Federal em:
```
http://200.152.38.155/CNPJ/
```

Mas a conexão está expirando após 2 minutos de tentativa.

## 🛠️ Melhorias Aplicadas no Código

### 1. Retry Logic com Timeout Maior
```python
# Agora tenta 5 vezes com timeout de 60 segundos
max_retries = 5
retry_delay = 10  # segundos entre tentativas

for attempt in range(max_retries):
    try:
        response = requests.get(dados_rf, timeout=60)
        # ...
    except requests.exceptions.Timeout:
        # Aguarda e tenta novamente
```

### 2. Melhor Tratamento de Erros
- Mensagens mais claras
- Diferentes tipos de erro tratados separadamente
- Logs detalhados para diagnóstico

## 🧪 Como Testar a Conectividade

### Opção 1: Script Automático de Teste

```bash
cd Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ-master

# Tornar executável
chmod +x test-connectivity.sh

# Executar
./test-connectivity.sh
```

Este script vai testar:
1. Conectividade básica com a internet
2. Acesso direto ao servidor da Receita Federal
3. Conectividade dentro do container do Airflow

### Opção 2: Teste Manual

```bash
# Teste 1: Acesso direto do seu sistema
curl -I http://200.152.38.155/CNPJ/

# Teste 2: Dentro do container do Airflow
docker exec -it airflow-scheduler curl -I http://200.152.38.155/CNPJ/

# Teste 3: Teste com Python dentro do container
docker exec -it airflow-scheduler python3 -c "
import requests
try:
    r = requests.get('http://200.152.38.155/CNPJ/', timeout=30)
    print(f'✅ Status: {r.status_code}')
except Exception as e:
    print(f'❌ Erro: {e}')
"
```

## 🔧 Soluções Possíveis

### Solução 1: Aguardar e Tentar Novamente (Mais Simples)

O servidor da Receita Federal pode estar:
- Temporariamente fora do ar
- Sobrecarregado
- Em manutenção

**O que fazer:**
```bash
# Aguarde 10-30 minutos e tente novamente
cd Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ-master

# Reiniciar e tentar novamente
docker compose restart airflow-scheduler

# Aguarde 1 minuto para o serviço subir
sleep 60

# Execute a DAG novamente no Airflow Web UI
```

### Solução 2: Verificar Conectividade do Docker

```bash
# Testar se o Docker tem acesso à internet
docker run --rm alpine ping -c 4 google.com

# Se não funcionar, reiniciar o Docker
sudo systemctl restart docker

# Aguardar 30 segundos
sleep 30

# Reiniciar o Compose
cd Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ-master
docker compose down
docker compose up -d
```

### Solução 3: Configurar Rede do Docker (Avançado)

Se o problema persistir, pode ser necessário ajustar a configuração de rede:

**Editar `docker-compose.yml`**, adicionar no serviço `airflow-common`:

```yaml
x-airflow-common:
  &airflow-common
  image: apache/airflow:2.8.1-python3.11
  network_mode: "bridge"  # ← ADICIONAR ESTA LINHA
  environment:
    # ... resto do arquivo
```

Depois:
```bash
docker compose down
docker compose up -d
```

### Solução 4: Usar DNS Público

Adicionar DNS público (Google) ao `docker-compose.yml`:

```yaml
x-airflow-common:
  &airflow-common
  image: apache/airflow:2.8.1-python3.11
  dns:  # ← ADICIONAR ESTAS LINHAS
    - 8.8.8.8
    - 8.8.4.4
  environment:
    # ... resto do arquivo
```

### Solução 5: Testar em Horário Alternativo

O servidor da Receita Federal pode estar mais responsivo em horários de menor movimento:
- Madrugada (0h-6h)
- Finais de semana

## 📊 Monitoramento da Execução

### Ver Logs em Tempo Real

```bash
# Logs gerais
docker compose logs -f airflow-scheduler

# Filtrar apenas logs do ETL
docker compose logs -f airflow-scheduler | grep -A 5 "ETL_coletar"

# Ver últimas 100 linhas
docker compose logs --tail=100 airflow-scheduler
```

### Verificar Status dos Containers

```bash
# Status
docker compose ps

# Estatísticas de recursos
docker stats

# Logs de erro
docker compose logs airflow-scheduler | grep -i error
```

## ⏱️ Tempo de Espera Esperado

Com as melhorias implementadas, o script agora:

1. **Primeira tentativa**: Aguarda até 60 segundos
2. **Retry automático**: 5 tentativas com 10s de intervalo
3. **Tempo total máximo**: ~5 minutos antes de falhar

Se após 5 tentativas ainda não conectar, indica problema mais sério de conectividade.

## 🎯 Resultado Esperado (Sucesso)

Quando funcionar, você verá nos logs:

```
[INFO] Conectando ao servidor da Receita Federal...
[INFO] URL: http://200.152.38.155/CNPJ/
[INFO] Tentativa 1/5...
[INFO] ✓ Conexão estabelecida com sucesso!
[INFO] Arquivos que serão baixados:
[INFO] 1 - EMPRESA...
[INFO] 2 - ESTABELE...
[INFO] 3 - SOCIO...
...
[INFO] Baixando arquivo:
[INFO] 1 - EMPRESA...
```

## 🆘 Troubleshooting Adicional

### Problema: "Network unreachable" no Docker

```bash
# Verificar redes do Docker
docker network ls

# Recriar rede do Airflow
docker network rm receita_federal_airflow-network
docker compose up -d
```

### Problema: Firewall bloqueando

```bash
# Ubuntu/Debian
sudo ufw status
sudo ufw allow out 80/tcp
sudo ufw allow out 443/tcp

# Verificar se iptables está bloqueando
sudo iptables -L -n
```

### Problema: Proxy ou VPN

Se você usa proxy ou VPN:

```yaml
# Adicionar ao docker-compose.yml
environment:
  HTTP_PROXY: http://seu-proxy:porta
  HTTPS_PROXY: http://seu-proxy:porta
  NO_PROXY: localhost,127.0.0.1
```

## 📚 Comandos Úteis de Diagnóstico

```bash
# 1. Verificar se o servidor está online (do seu sistema)
ping -c 4 200.152.38.155

# 2. Testar HTTP direto
telnet 200.152.38.155 80

# 3. Trace route para ver onde está travando
traceroute 200.152.38.155

# 4. Verificar DNS dentro do container
docker exec airflow-scheduler nslookup google.com

# 5. Testar conectividade geral do container
docker exec airflow-scheduler wget -O- https://www.google.com
```

## 📝 Checklist de Resolução

- [ ] Executar `test-connectivity.sh`
- [ ] Verificar se o servidor está acessível do host
- [ ] Verificar se o servidor está acessível dentro do container
- [ ] Tentar reiniciar Docker e Compose
- [ ] Aguardar 10-30 minutos e tentar novamente
- [ ] Tentar em horário alternativo (madrugada/fim de semana)
- [ ] Configurar DNS público se necessário
- [ ] Verificar firewall/proxy se aplicável

## 🎓 Lições Aprendidas

1. **Servidores públicos podem ser instáveis**: É normal ter timeouts ocasionais
2. **Retry é essencial**: Sempre implementar lógica de retry para serviços externos
3. **Timeout adequado**: 60 segundos é um bom equilíbrio
4. **Logs detalhados**: Facilitam o diagnóstico de problemas

## 📞 Próximos Passos

1. **Execute o teste de conectividade**:
   ```bash
   cd Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ-master
   chmod +x test-connectivity.sh
   ./test-connectivity.sh
   ```

2. **Se tudo OK**, reinicie a DAG no Airflow

3. **Se continuar falhando**, tente as soluções 2-5 acima

4. **Se nada funcionar**, pode ser que o servidor esteja realmente fora do ar. Aguarde e tente mais tarde.

---

**Status**: 🔄 Melhorias aplicadas, aguardando teste  
**Data**: 2025-11-10  
**Tipo**: Timeout de conexão com servidor externo

