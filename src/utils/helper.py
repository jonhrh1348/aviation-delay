def safe_get(obj, *path, default_value=None):
    for key in path:
        if isinstance(obj, dict) and key in obj:
            obj = obj.get(key)
        else:
            return default_value
    return obj