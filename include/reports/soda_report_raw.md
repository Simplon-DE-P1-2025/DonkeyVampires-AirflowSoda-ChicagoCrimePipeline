# Soda Data Quality Report

**Date** : 2026-03-12 00:16:39

## Dataset : `chicago_crime`

**Statut** : `ContractVerificationStatus.FAILED`  |  **Début** : 2026-03-12 00:16:39.297047+00:00  |  **Fin** : 2026-03-12 00:16:39.365044+00:00

### Colonne : `id`

| | Check | Type | Statut | Seuil | Diagnostics |
|--|-------|------|--------|-------|-------------|
| ✅ | No missing values | missing | CheckOutcome.PASSED | 0 | missing_count: 0
missing_percent: 0.0
check_rows_tested: 40000
dataset_rows_tested: 40000 |
| ✅ | No duplicate values | duplicate | CheckOutcome.PASSED | 0 | dataset_rows_tested: 40000
check_rows_tested: 40000
missing_count: 0
duplicate_count: 0
duplicate_percent: 0.0 |

### Colonne : `case_number`

| | Check | Type | Statut | Seuil | Diagnostics |
|--|-------|------|--------|-------|-------------|
| ✅ | No missing values | missing | CheckOutcome.PASSED | 0 | missing_count: 0
missing_percent: 0.0
check_rows_tested: 40000
dataset_rows_tested: 40000 |

### Colonne : `date`

| | Check | Type | Statut | Seuil | Diagnostics |
|--|-------|------|--------|-------|-------------|
| ✅ | No missing values | missing | CheckOutcome.PASSED | 0 | missing_count: 0
missing_percent: 0.0
check_rows_tested: 40000
dataset_rows_tested: 40000 |

### Colonne : `year`

| | Check | Type | Statut | Seuil | Diagnostics |
|--|-------|------|--------|-------|-------------|
| ✅ | No missing values | missing | CheckOutcome.PASSED | 0 | missing_count: 0
missing_percent: 0.0
check_rows_tested: 40000
dataset_rows_tested: 40000 |

### Colonne : `updated_on`

| | Check | Type | Statut | Seuil | Diagnostics |
|--|-------|------|--------|-------|-------------|
| ✅ | No missing values | missing | CheckOutcome.PASSED | 0 | missing_count: 0
missing_percent: 0.0
check_rows_tested: 40000
dataset_rows_tested: 40000 |

### Colonne : `block`

| | Check | Type | Statut | Seuil | Diagnostics |
|--|-------|------|--------|-------|-------------|
| ✅ | No missing values | missing | CheckOutcome.PASSED | 0 | missing_count: 0
missing_percent: 0.0
check_rows_tested: 40000
dataset_rows_tested: 40000 |

### Colonne : `beat`

| | Check | Type | Statut | Seuil | Diagnostics |
|--|-------|------|--------|-------|-------------|
| ✅ | No missing values | missing | CheckOutcome.PASSED | 0 | missing_count: 0
missing_percent: 0.0
check_rows_tested: 40000
dataset_rows_tested: 40000 |

### Colonne : `district`

| | Check | Type | Statut | Seuil | Diagnostics |
|--|-------|------|--------|-------|-------------|
| ✅ | No missing values | missing | CheckOutcome.PASSED | 0 | missing_count: 0
missing_percent: 0.0
check_rows_tested: 40000
dataset_rows_tested: 40000 |

### Colonne : `ward`

| | Check | Type | Statut | Seuil | Diagnostics |
|--|-------|------|--------|-------|-------------|
| ✅ | No missing values | missing | CheckOutcome.PASSED | 0 | missing_count: 0
missing_percent: 0.0
check_rows_tested: 40000
dataset_rows_tested: 40000 |

### Colonne : `community_area`

| | Check | Type | Statut | Seuil | Diagnostics |
|--|-------|------|--------|-------|-------------|
| ❌ | No missing values | missing | CheckOutcome.FAILED | 1 | missing_count: 1
missing_percent: 0.0025
check_rows_tested: 40000
dataset_rows_tested: 40000 |

### Colonne : `iucr`

| | Check | Type | Statut | Seuil | Diagnostics |
|--|-------|------|--------|-------|-------------|
| ✅ | No missing values | missing | CheckOutcome.PASSED | 0 | missing_count: 0
missing_percent: 0.0
check_rows_tested: 40000
dataset_rows_tested: 40000 |

### Colonne : `primary_type`

| | Check | Type | Statut | Seuil | Diagnostics |
|--|-------|------|--------|-------|-------------|
| ✅ | No missing values | missing | CheckOutcome.PASSED | 0 | missing_count: 0
missing_percent: 0.0
check_rows_tested: 40000
dataset_rows_tested: 40000 |

### Colonne : `description`

| | Check | Type | Statut | Seuil | Diagnostics |
|--|-------|------|--------|-------|-------------|
| ✅ | No missing values | missing | CheckOutcome.PASSED | 0 | missing_count: 0
missing_percent: 0.0
check_rows_tested: 40000
dataset_rows_tested: 40000 |

### Colonne : `fbi_code`

| | Check | Type | Statut | Seuil | Diagnostics |
|--|-------|------|--------|-------|-------------|
| ✅ | No missing values | missing | CheckOutcome.PASSED | 0 | missing_count: 0
missing_percent: 0.0
check_rows_tested: 40000
dataset_rows_tested: 40000 |

### Colonne : `arrest`

| | Check | Type | Statut | Seuil | Diagnostics |
|--|-------|------|--------|-------|-------------|
| ✅ | No missing values | missing | CheckOutcome.PASSED | 0 | missing_count: 0
missing_percent: 0.0
check_rows_tested: 40000
dataset_rows_tested: 40000 |

### Colonne : `domestic`

| | Check | Type | Statut | Seuil | Diagnostics |
|--|-------|------|--------|-------|-------------|
| ✅ | No missing values | missing | CheckOutcome.PASSED | 0 | missing_count: 0
missing_percent: 0.0
check_rows_tested: 40000
dataset_rows_tested: 40000 |

| Métrique | Valeur |
|----------|--------|
| Total checks | 17 |
| ✅ Passés | 16 |
| ❌ Échoués | 1 |

---

## Résumé global de la session

| Métrique | Valeur |
|----------|--------|
| Total checks | 17 |
| ✅ Passés | 16 |
| ❌ Échoués | 1 |
| is_passed | False |
| is_failed | True |
| is_warned | False |
| has_errors | False |
| is_ok | False |