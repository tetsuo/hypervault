
# -*- coding: utf-8 -*-

def memoize(f):
    """Birden fazla defa aynı argümanlarla çağırılan metodların
    çıktılarını cache etmeye yarayan decorator.
    """
    class memodict(dict):
        def __getitem__(self, *key):
            return dict.__getitem__(self, key)
        def __missing__(self, key):
            ret = self[key] = f(*key)
            return ret
    return memodict().__getitem__
