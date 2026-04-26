# tests/test_transformations.py
"""
Tests unitaires pour les transformations métier du pipeline Disaster Pipeline.
Exécutez avec : pytest tests/test_transformations.py -v
"""
import pytest
import pandas as pd
from datetime import datetime


# =============================================================================
# Test 1: Validation des coordonnées GPS (nettoyage données)
# =============================================================================
def test_coordinates_valid():
    """Vérifie que seules les coordonnées valides sont conservées"""
    df = pd.DataFrame({
        "latitude": [45.2, -12.5, "invalid", None, 0],
        "longitude": [3.1, -45.2, "bad", None, 0]
    })
    
    # Conversion numérique avec gestion des erreurs
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    
    # Filtrage des valeurs nulles
    valid = df.dropna(subset=["latitude", "longitude"])
    
    assert len(valid) == 3, "Should keep only valid numeric coordinates"
    assert all(valid["latitude"].between(-90, 90)), "Latitude must be between -90 and 90"
    assert all(valid["longitude"].between(-180, 180)), "Longitude must be between -180 and 180"


# =============================================================================
# Test 2: Mapping des types d'événements GDACS (transformation métier)
# =============================================================================
def test_map_event_type():
    """Vérifie le mapping des codes GDACS vers labels lisibles"""
    mapping = {
        "EQ": "Earthquake",
        "FL": "Flood", 
        "TC": "Tropical Cyclone",
        "DR": "Drought",
        "VO": "Volcano",
        "WF": "Wildfire",
        "TS": "Tsunami"
    }
    
    # Test cas normaux
    assert mapping.get("EQ") == "Earthquake"
    assert mapping.get("FL") == "Flood"
    
    # Test cas inconnu → fallback
    assert mapping.get("UNKNOWN", "Other") == "Other"
    assert mapping.get("", "Other") == "Other"


# =============================================================================
# Test 3: Calcul du niveau d'alerte numérique (agrégation)
# =============================================================================
def test_alert_level_num():
    """Vérifie la conversion des niveaux d'alerte texte → numérique"""
    alert_map = {"GREEN": 1, "ORANGE": 2, "RED": 3}
    
    # Test valeurs connues
    assert alert_map.get("RED", 0) == 3
    assert alert_map.get("ORANGE", 0) == 2
    assert alert_map.get("GREEN", 0) == 1
    
    # Test valeur inconnue → 0
    assert alert_map.get("UNKNOWN", 0) == 0
    assert alert_map.get("", 0) == 0


# =============================================================================
# Test 4: Filtrage des pays vides (pour mart_disasters_by_country)
# =============================================================================
def test_filter_empty_countries():
    """Vérifie que les pays vides/null sont exclus de l'agrégation par pays"""
    df = pd.DataFrame({
        "country": ["France", "", None, "USA", "   ", "Turkey"],
        "disasters": [10, 5, 3, 20, 2, 8]
    })
    
    # Filtrage : country non null ET non vide ET non whitespace-only
    filtered = df[
        df["country"].notna() & 
        (df["country"].str.strip() != "")
    ]
    
    assert len(filtered) == 3, "Should keep only non-empty countries"
    assert set(filtered["country"]) == {"France", "USA", "Turkey"}


# =============================================================================
# Test 5: Validation et parsing des dates d'événement
# =============================================================================
def test_event_date_parse():
    """Vérifie le parsing robuste des dates d'événements"""
    # Dates valides ISO 8601
    valid_dates = ["2026-04-25", "2026-01-01", "2025-12-31"]
    # Dates invalides
    invalid_dates = ["invalid", "25/04/2026", "2026/04/25", None, ""]
    
    all_dates = valid_dates + invalid_dates
    
    # Parsing avec gestion des erreurs
    parsed = pd.to_datetime(all_dates, errors="coerce")
    
    # Compter les dates valides
    valid_count = parsed.notna().sum()
    
    assert valid_count == len(valid_dates), f"Expected {len(valid_dates)} valid dates, got {valid_count}"
    assert parsed[0] == pd.Timestamp("2026-04-25"), "First date should parse correctly"


# =============================================================================
# Test bonus: Union GDACS + EONET (gold layer)
# =============================================================================
 # =============================================================================
# Test bonus: Union GDACS + EONET (gold layer)
# =============================================================================
def test_union_sources_schema():
    """Vérifie que l'union GDACS+EONET préserve le schéma gold_disasters"""
    # Schéma attendu (colonnes clés)
    expected_cols = {
        "disaster_id", "event_type", "event_type_label", "event_name",
        "alert_level", "alert_level_num", "status", "country", "iso3",
        "latitude", "longitude", "event_date", "source_tag", "is_active"
    }
    
    # Données GDACS simulées (avec TOUTES les colonnes attendues)
    gdacs = pd.DataFrame({
        "disaster_id": ["GDACS-001"],
        "event_type": ["EQ"],
        "event_type_label": ["Earthquake"],
        "event_name": ["Test Earthquake"],
        "alert_level": ["ORANGE"],
        "alert_level_num": [2],
        "status": ["ongoing"],
        "country": ["Turkey"],
        "iso3": ["TUR"],
        "latitude": [39.0],
        "longitude": [35.0],
        "event_date": [pd.Timestamp("2026-04-20")],
        "source_tag": ["GDACS"],
        "is_active": [True]
    })
    
    # Données EONET simulées (mêmes colonnes, pays=null)
    eonet = pd.DataFrame({
        "disaster_id": ["EONET-001"],
        "event_type": ["WF"],
        "event_type_label": ["Wildfire"],
        "event_name": ["Test Wildfire"],
        "alert_level": ["UNKNOWN"],
        "alert_level_num": [0],
        "status": ["open"],
        "country": [None],
        "iso3": [None],
        "latitude": [37.0],
        "longitude": [30.0],
        "event_date": [pd.Timestamp("2026-04-21")],
        "source_tag": ["EONET"],
        "is_active": [True]
    })
    
    # Union
    gold = pd.concat([gdacs, eonet], ignore_index=True)
    
    # Vérifications
    assert len(gold) == 2, "Union should have 2 rows"
    assert set(gold.columns).issuperset(expected_cols), "Gold schema should include all expected columns"
    assert gold["source_tag"].nunique() == 2, "Should have both GDACS and EONET sources"
    # Vérifier que les valeurs NULL pour EONET sont préservées
    eonet_row = gold[gold["source_tag"] == "EONET"].iloc[0]
    assert pd.isna(eonet_row["country"]), "EONET should have NULL country"
    assert pd.isna(eonet_row["iso3"]), "EONET should have NULL iso3"