import pytest
import requests
import unittest
from unittest.mock import patch, MagicMock, Mock, DEFAULT
from dojot.module import Auth, Config, HttpRequester
import requests
import json


def test_get_management_token_ok():
    config = Mock(
        keycloak={
            "base_path": "http://kc_base_path/",
            "ignore_realm": "master",
            "credentials": {
                "username": "admin",
                "password": "admin",
                "client_id": "admin-cli",
                "grant_type": "password",
            }
        }
    )
    mock_self = Mock(config=config)

    response = requests.Response()

    def json_func():
        return {"access_token": "xyz"}
    response.json = json_func
    patch_http_perform = patch(
        "requests.post", return_value=response)
    with patch_http_perform as mock_http_perform:
        token = Auth.get_management_token(mock_self)
        mock_http_perform.assert_called_with(
            'http://kc_base_path/realms/master/protocol/openid-connect/token',
            data=mock_self.config.keycloak['credentials'])
        assert token == "xyz"


def test_get_management_token_fail():
    config = Mock(
        keycloak={
            "base_path": "http://kc_base_path/",
            "ignore_realm": "master",
            "credentials": {
                "username": "admin",
                "password": "admin",
                "client_id": "admin-cli",
                "grant_type": "password",
            }
        }
    )

    mock_self = Mock(config=config)

    patch_http_perform = patch("requests.post")
    with patch_http_perform as mock_http_perform:
        mock_http_perform.side_effect = requests.exceptions.ConnectionError()
        with pytest.raises(Exception):
            Auth.get_management_token(mock_self)


def test_get_access_token_ok():
    config = Mock(
        dojot={
            "management": {
                "user": "management-user",
                "tenant": "non-management-tenant"
            }
        }
    )
    mock_self = Mock(config=config)
    token = Auth.get_access_token(mock_self, "non-management-tenant")
    assert token is not None


def test_get_tenants():
    config = Mock(
        keycloak={
            "timeout_sleep": 1,
            "connection_retries": 3,
            "base_path": "http://kc_base_path/",
            "ignore_realm": "master",
            "credentials": {
                "username": "admin",
                "password": "admin",
                "client_id": "admin-cli",
                "grant_type": "password",
            }
        },
        dojot={
            "management": {
                "user": "sample-user",
                "tenant": "sample-tenant"
            }
        }
    )
    mock_self = Mock(
        get_management_token=Mock(return_value="123"),
        config=config
    )

    master_tenant = '[{"realm": "master"}, {"realm": "admin"}]'
    patch_http_perform = patch(
        "dojot.module.HttpRequester.do_it", return_value=json.loads(master_tenant))
    with patch_http_perform as mock_http_perform:
        tenants = Auth.get_tenants(mock_self)
        mock_http_perform.assert_called_with(
            "http://kc_base_path/admin/realms", "123", 3, 1)
        assert tenants[0] == "admin"
    patch_http_perform = patch(
        "dojot.module.HttpRequester.do_it", return_value=None)
    with patch_http_perform as mock_http_perform:
        tenants = Auth.get_tenants(mock_self)
        mock_http_perform.assert_called_with(
            "http://kc_base_path/admin/realms", "123", 3, 1)
        assert tenants is None


def test_get_tenants_fail():
    config = Mock(
        keycloak={
            "timeout_sleep": 1,
            "connection_retries": 3,
            "base_path": "http://kc_base_path/",
            "tenants_endpoint": "keycloak_tenants_endpoint/",
            "ignore_realm": "master",
            "credentials": {
                "username": "admin",
                "password": "admin",
                "client_id": "admin-cli",
                "grant_type": "password",
            }
        },
        dojot={
            "management": {
                "user": "sample-user",
                "tenant": "sample-tenant"
            }
        }
    )
    mock_self = Mock(
        get_management_token=Mock(
            side_effect=requests.exceptions.ConnectionError()),
        config=config
    )

    tenants = Auth.get_tenants(mock_self)
    assert tenants is None
