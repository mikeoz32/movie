
class ClassLoader:
    @staticmethod
    def load_class(class_path: str) -> type:
        components = class_path.split(".")
        module_path = ".".join(components[:-1])
        class_name = components[-1]
        module = __import__(module_path, fromlist=[class_name])
        return getattr(module, class_name)

