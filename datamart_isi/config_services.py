from pathlib import Path

import os
import json

config_file = Path(os.path.join(os.path.dirname(__file__), 'datamart-services.json'))

if not config_file.exists():
    raise FileNotFoundError(f'Must define service location: {config_file}\nFor example, see datamart-services-dsbox01.json')

with open(config_file) as input:
    service_defs = json.load(input)


def _get_service_def(service_name) -> dict:
    for definition in service_defs['services']:
        if service_name == definition['name']:
            return definition
    return None


def get_service_url(service_name, as_url=True) -> str:
    definition = _get_service_def(service_name)
    if definition is None:
        print('get_service_url missing definition: ', service_name)
        raise ValueError(f'Service name not found: {service_name}')

    default_host = service_defs['server'].get('default_host', '')
    host = service_defs['server'].get('host', default_host)
    if host == '':
        raise ValueError(f'Host for service {service_name} not defined')

    port = definition['port']
    path = definition.get('path', '')
    if as_url:
        if path:
            url = f'http://{host}:{port}/{path}'
        else:
            url = f'http://{host}:{port}'
    else:
        if path:
            url = f'{host}:{port}/{path}'
        else:
            url = f'{host}:{port}'
    print('get_service_url', service_name, url)
    return url


def get_default_datamart_url() -> str:
    return get_service_url('isi_datamart')
