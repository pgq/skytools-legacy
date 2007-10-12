
"""Functions to install londiste and its depentencies into database."""

import os, skytools

__all__ = ['install_provider', 'install_subscriber']

provider_object_list = [
    skytools.DBLanguage("plpgsql"),
    skytools.DBFunction('txid_current_snapshot', 0, sql_file = "txid.sql"),
    skytools.DBSchema('pgq', sql_file = "pgq.sql"),
    skytools.DBSchema('londiste', sql_file = "londiste.sql")
]

subscriber_object_list = [
    skytools.DBLanguage("plpgsql"),
    skytools.DBSchema('londiste', sql_file = "londiste.sql")
]

def install_provider(curs, log):
    """Installs needed code into provider db."""
    skytools.db_install(curs, provider_object_list, log)

def install_subscriber(curs, log):
    """Installs needed code into subscriber db."""
    skytools.db_install(curs, subscriber_object_list, log)

