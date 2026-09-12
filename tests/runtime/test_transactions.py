import pytest

from .conftest import build_client, prepare_database


def test_public_transaction_commit_and_rollback(tmp_path):
    prepare_database(tmp_path / 'transactions.db')
    _, client = build_client()
    with client.transaction():
        client.runtime_user.insert({'name': '保留', 'email': None})
        with pytest.raises(RuntimeError), client.transaction():
            client.runtime_user.insert_many([{'name': '回滚', 'email': None}])
            client.runtime_user.update_many(data={'email': '不提交'})
            raise RuntimeError('内层事务失败')
    rows = client.runtime_user.find_many()
    assert len(rows) == 1 and rows[0].name == '保留' and rows[0].email is None
    with pytest.raises(RuntimeError), client.transaction():
        client.runtime_user.delete_many()
        client.runtime_user.insert({'name': '不保留', 'email': None})
        raise RuntimeError('外层事务失败')
    assert client.runtime_user.find_many()[0].name == '保留'
    client.close()


def test_clients_with_same_datasource_do_not_commit_each_other(tmp_path):
    prepare_database(tmp_path / 'isolation.db')
    _, first = build_client()
    _, second = build_client()
    with first.transaction():
        first.runtime_user.insert({'name': '尚未提交', 'email': None})
        assert second.runtime_user.find_many() == []
    assert len(second.runtime_user.find_many()) == 1
    first.close()
    assert len(second.runtime_user.find_many()) == 1
    second.close()
