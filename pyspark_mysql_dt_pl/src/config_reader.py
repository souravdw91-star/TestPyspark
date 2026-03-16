try:
    import yaml
except ImportError as e:
    raise ImportError(
        "Missing dependency 'pyyaml'. Install it with: pip install pyyaml"
    ) from e


def load_config():

    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)

    return config