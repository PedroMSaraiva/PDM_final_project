#!/usr/bin/env python3
"""
Script local para executar o loader de empresas da Receita Federal
Útil quando o Cloud Run/Cloud Function atinge o limite de tempo

Uso:
    # Carregar todas as empresas de todos os períodos
    python run_loader_empresas_local.py

    # Carregar apenas um período específico
    python run_loader_empresas_local.py --period 2024-03

    # Carregar apenas alguns períodos
    python run_loader_empresas_local.py --periods 2024-01 2024-02 2024-03
"""

import os
import sys
import argparse
import time
from typing import List, Optional

# Adicionar o diretório atual ao path para importar o main
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import (
    PROJECT_ID,
    DATASET_ID,
    load_receita_data,
    load_receita_by_period,
    get_available_periods,
    DATA_TYPES_CONFIG
)


def main():
    parser = argparse.ArgumentParser(
        description='Loader local de empresas da Receita Federal para BigQuery'
    )
    parser.add_argument(
        '--period',
        type=str,
        help='Período específico no formato YYYY-MM (ex: 2024-03)'
    )
    parser.add_argument(
        '--periods',
        nargs='+',
        help='Lista de períodos para processar (ex: 2024-01 2024-02 2024-03)'
    )
    parser.add_argument(
        '--append',
        action='store_true',
        default=True,
        help='Adicionar dados sem apagar existentes (padrão: True)'
    )
    parser.add_argument(
        '--truncate',
        action='store_true',
        help='Substituir dados existentes (WRITE_TRUNCATE)'
    )
    parser.add_argument(
        '--data-type',
        type=str,
        default='empresas',
        choices=['empresas', 'estabelecimentos', 'all'],
        help='Tipo de dado para carregar (padrão: empresas)'
    )
    
    args = parser.parse_args()
    
    # Determinar modo de escrita
    append = not args.truncate if args.truncate else args.append
    
    # Determinar tipos de dados
    if args.data_type == 'all':
        data_types = list(DATA_TYPES_CONFIG.keys())
    else:
        data_types = [args.data_type]
    
    print("=" * 80)
    print("LOADER LOCAL - RECEITA FEDERAL")
    print("=" * 80)
    print(f"Projeto: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print(f"Tipos de dados: {', '.join([DATA_TYPES_CONFIG[dt]['description'] for dt in data_types])}")
    print(f"Modo: {'WRITE_APPEND' if append else 'WRITE_TRUNCATE'}")
    print()
    
    start_time = time.time()
    
    try:
        if args.period:
            # Carregar período específico
            print(f"📊 Carregando período específico: {args.period}")
            result = load_receita_by_period(
                args.period,
                data_types=data_types,
                append=append
            )
            
            if isinstance(result, dict) and 'status' in result:
                if result['status'] == 'success':
                    print(f"\n✅ Sucesso! Linhas: {result.get('rows', 0):,}")
                else:
                    print(f"\n❌ Erro: {result.get('error', 'Desconhecido')}")
            else:
                # Múltiplos tipos
                for dt in data_types:
                    if dt in result:
                        r = result[dt]
                        status = "✅" if r.get('status') == 'success' else "❌"
                        print(f"{status} {dt}: {r.get('rows', 0):,} linhas")
        
        elif args.periods:
            # Carregar múltiplos períodos específicos
            print(f"📊 Carregando {len(args.periods)} períodos específicos...")
            print(f"Períodos: {', '.join(args.periods)}")
            print()
            
            all_results = {}
            for idx, period in enumerate(args.periods):
                print(f"[{idx + 1}/{len(args.periods)}] Processando {period}...")
                result = load_receita_by_period(
                    period,
                    data_types=data_types,
                    append=append if idx > 0 else not append  # Primeiro pode truncar, demais append
                )
                all_results[period] = result
                print()
            
            # Resumo
            print("=" * 80)
            print("RESUMO")
            print("=" * 80)
            for period, result in all_results.items():
                if isinstance(result, dict) and 'status' in result:
                    status = "✅" if result['status'] == 'success' else "❌"
                    print(f"{status} {period}: {result.get('rows', 0):,} linhas")
        
        else:
            # Carregar todos os períodos
            print("📊 Carregando todos os períodos disponíveis...")
            print("⚠️  ATENÇÃO: Isso pode levar muito tempo!")
            print()
            
            result = load_receita_data(data_types=data_types)
            
            # Resumo
            print("=" * 80)
            print("RESUMO FINAL")
            print("=" * 80)
            
            if 'data_types' in result:
                for dt, dt_result in result['data_types'].items():
                    desc = DATA_TYPES_CONFIG[dt]['description']
                    status = "✅" if dt_result['status'] == 'success' else "⚠️"
                    print(f"{status} {desc}:")
                    print(f"   Períodos processados: {dt_result['periods_processed']}")
                    print(f"   Períodos com erro: {dt_result['periods_failed']}")
                    print(f"   Total de linhas: {dt_result['total_rows']:,}")
                    print()
        
        elapsed_time = time.time() - start_time
        print(f"⏱️  Tempo total: {elapsed_time:.1f}s ({elapsed_time/60:.1f} min)")
        print("=" * 80)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Processo interrompido pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()