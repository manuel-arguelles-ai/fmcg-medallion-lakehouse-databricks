"""Tablas de referencia y DDL de Silver: cobertura contra datagen y sincronia del SQL generado."""
from datagen import catalog_data as cd
from silver import reference_data as ref
from silver import rules as R


def test_city_mapping_covers_every_datagen_city_and_typo():
    for city, typos in cd.CITY_TYPOS.items():
        assert ref.CITY_MAPPING[city.lower()] == city
        for t in typos:
            assert ref.CITY_MAPPING[t.lower()] == city
    assert set(cd.CITIES) == set(cd.CITY_TYPOS)


def test_category_division_covers_every_datagen_category():
    cats = set(cd.MADRE_PRODUCTS) | set(cd.HIJA_PRODUCTS)
    for c in cats:
        assert ref.CATEGORY_DIVISION[c.lower()][0] == c
    assert {k for k in ref.CATEGORY_DIVISION} == {c.lower() for c in cats}
    assert all(d for _, d in ref.CATEGORY_DIVISION.values())


def test_typo_fix_matches_datagen_term():
    (_, col, wrong, right), = ref.TYPO_FIXES
    assert (right, wrong) == cd.TYPO_TERM and col == "product_name"


def test_company_attrs_cover_both_companies():
    assert {c for c, *_ in ref.COMPANY_ATTRS} == {"madre", "hija"}


def test_product_typo_fixed_for_every_datagen_product():
    fixes = [(w, c) for _, _, w, c in ref.TYPO_FIXES]
    for tree in (cd.MADRE_PRODUCTS, cd.HIJA_PRODUCTS):
        for bases in tree.values():
            for base, variants in bases:
                for v in variants:
                    name = f"{base} ({v})".replace(cd.TYPO_TERM[1], cd.TYPO_TERM[0])
                    dirty = name.replace(*cd.TYPO_TERM)
                    assert R.apply_typo_fixes(dirty, fixes) == name
