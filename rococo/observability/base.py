
class ObservabilityBase:
    def __init__(self, **config_kwargs):
        self.config = config_kwargs
        self._validate_required_config()
        self.setup()

    def setup(self):
        raise NotImplementedError("Subclasses must implement the setup method.")

    def _validate_required_config(self):
        missing = [key for key in self.REQUIRED_CONFIG_KEYS if not self.config.get(key)]
        if missing:
            raise ValueError(
                f"{self.__class__.__name__} configuration is incomplete. "
                f"Missing required key(s): {', '.join(missing)}."
            )
