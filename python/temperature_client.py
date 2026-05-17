"""(4) Cliente REST: consulta o servico para obter ultimas/historicas estatisticas."""
from __future__ import print_function
import json
import logging
import sys

import requests
import const


def _base_url() -> str:
    return f'http://{const.CLIENT_IP}:{const.REST_PORT}'


def _print_stats(label: str, data):
    print(label)
    print(json.dumps(data, indent=2, ensure_ascii=False))


def run(sensor_id: str = None, history_limit: int = 5):
    base = _base_url()
    print(f'[client] conectando em {base}')

    # Lista sensores conhecidos
    r = requests.get(f'{base}/sensors')
    r.raise_for_status()
    sensors = r.json().get('sensor_ids', [])
    print('Sensores conhecidos: ' + str(sensors))

    # Total de estatisticas armazenadas
    r = requests.get(f'{base}/stats/count')
    r.raise_for_status()
    print('Total de estatisticas armazenadas: ' + str(r.json().get('count')))

    # Ultima estatistica geral
    r = requests.get(f'{base}/stats/latest')
    if r.status_code == 404:
        print('\nNenhuma estatistica registrada ainda.')
    else:
        r.raise_for_status()
        _print_stats('\nUltima estatistica registrada:', r.json())

    # Se um sensor_id foi informado, faz consultas adicionais
    target = sensor_id or (sensors[0] if sensors else None)
    if target is None:
        print('Nenhum sensor disponivel para consulta detalhada.')
        return

    r = requests.get(f'{base}/sensors/{target}/stats/latest')
    if r.status_code == 404:
        print(f'\nNenhuma estatistica para o sensor "{target}".')
    else:
        r.raise_for_status()
        _print_stats(f'\nUltima estatistica do sensor "{target}":', r.json())

    r = requests.get(f'{base}/sensors/{target}/history', params={'limit': history_limit})
    r.raise_for_status()
    hist = r.json().get('stats', [])
    print(f'\nHistorico (ultimos {history_limit}) do sensor "{target}":')
    for i, s in enumerate(hist, 1):
        print(f'--- {i} ---')
        print(json.dumps(s, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    logging.basicConfig()
    sid = sys.argv[1] if len(sys.argv) > 1 else None
    lim = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    run(sid, lim)
