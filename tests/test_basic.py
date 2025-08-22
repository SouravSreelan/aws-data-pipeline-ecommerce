import pytest
import yaml

def test_config_load():
    with open('src/config.yaml') as f:
        data = yaml.safe_load(f)
    assert data['local']['data_dir'] == 'data/temp'