from dclassql.model_inspector import FieldTo


def test_field_to_delegates_to_original_mapping() -> None:
    mapping: dict[str, type[object]] = {"id": int}
    fields = FieldTo.from_mapping(mapping)

    assert fields["id"] is int
    assert list(fields) == ["id"]
    assert len(fields) == 1

    mapping["name"] = str
    assert fields["name"] is str
