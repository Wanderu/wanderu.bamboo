# coding: utf-8

try:
    # setuptools namespace package declaration version
    # (include matching setup.py)
    from pkg_resources import declare_namespace
    declare_namespace(__name__)
except ImportError:
    # fallback on stdlib discovery process
    from pkgutil import extend_path
    __path__ = extend_path(__path__, __name__)
