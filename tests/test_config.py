from movie.config import Config


def test_config_loads_defaults():
    defaults = {
        "actor": {
            "system": {"name": "default-system", "loglevel": "INFO", "port": "8080"}
        }
    }
    config = Config.from_dict(defaults)
    assert config is not None
    assert config.get("actor.system.name") == "default-system"
    assert config.get("actor.system.loglevel") == "INFO"

    system_config = config.get_config("actor.system")
    assert system_config is not None
    assert system_config.get("name") == "default-system"
    assert system_config.get("loglevel") == "INFO"
    assert system_config.get_int("port") == 8080
