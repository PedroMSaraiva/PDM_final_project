-- Script de inicialização do banco de dados Dados_RFB
-- Este script é executado automaticamente quando o container do PostgreSQL é criado

-- O banco já é criado pelo POSTGRES_DB no docker-compose
-- Apenas adicionando comentário para documentação

COMMENT ON DATABASE "Dados_RFB"
    IS 'Base de dados para gravar os dados públicos de CNPJ da Receita Federal do Brasil';

-- Criar extensões úteis se necessário
-- CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- CREATE EXTENSION IF NOT EXISTS unaccent;

