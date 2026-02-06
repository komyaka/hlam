import re
import difflib
from pathlib import Path
import pandas as pd

# ==========================
# Настройки
# ==========================
INPUT_EXCEL = "22-12-2025.xlsx"                 # Исходный Excel
SHEET_NAME = "Детальная информация"            # Если листа нет — скрипт найдёт подходящий автоматически
STOP_CATEGORIES_FILE = "1stop.txt"             # Стоп-категории
STOP_WORDS_FILE = "2stop.txt"                  # Стоп-слова
OUTPUT_CSV = "22-12-2025_clean.csv"            # Итоговый CSV
OUTPUT_REMOVED_WORDS_CSV = "22-12-2025_removed_by_2stop.csv"  # Удалённые по 2stop.txt (+ matched_stop)

# Если хотите вручную задать соответствие (в случае совсем нестандартных заголовков) — раскомментируйте:
# USER_COLUMN_MAP = {
#     "query": "Поисковый запрос",
#     "count": "Количество запросов",
#     "avg_per_day": "Запросов в среднем за день",
#     "category": "Больше всего заказов в предмете",
# }
USER_COLUMN_MAP = None


# ==========================
# Утилиты
# ==========================
def _norm(x: object) -> str:
    """Нормализация заголовков для устойчивого сравнения."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x)
    s = s.replace("\ufeff", "")          # BOM
    s = s.replace("\xa0", " ")           # NBSP
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


REQUIRED = {
    "query": {
        "label": "Поисковый запрос",
        "keywords_any": ["поисков", "запрос", "query", "keyword", "search"],
        "keywords_all": ["поисков", "запрос"],
    },
    "count": {
        "label": "Количество запросов",
        "keywords_any": ["колич", "запрос", "count", "frequency", "частот"],
        "keywords_all": ["колич", "запрос"],
    },
    "avg_per_day": {
        "label": "Запросов в среднем за день",
        "keywords_any": ["средн", "день", "avg", "per day", "в среднем"],
        "keywords_all": ["средн", "день"],
    },
    "category": {
        "label": "Больше всего заказов в предмете",
        "keywords_any": ["заказ", "предмет", "катег", "category", "больше всего"],
        "keywords_all": ["заказ", "предмет"],
    },
}


def _score(col_norm: str, spec: dict) -> int:
    """Оценивает, насколько заголовок похож на требуемый."""
    if not col_norm:
        return 0

    target = _norm(spec["label"])
    if col_norm == target:
        return 100

    if all(k in col_norm for k in spec["keywords_all"]):
        return 95

    if any(k in col_norm for k in spec["keywords_any"]):
        return 80

    ratio = difflib.SequenceMatcher(a=col_norm, b=target).ratio()
    return int(ratio * 60)


def load_stop_list(path: Path) -> list[str]:
    """Загружает список стоп-значений из текстового файла, убирая пустые строки и дубликаты (с сохранением порядка)."""
    if not path.exists():
        raise FileNotFoundError(f"Файл со стоп-значениями не найден: {path}")

    raw: list[str] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            value = line.strip()
            if value:
                raw.append(value)

    seen = set()
    unique: list[str] = []
    for v in raw:
        if v not in seen:
            seen.add(v)
            unique.append(v)
    return unique


def build_categories_regex(stop_categories: list[str]) -> re.Pattern:
    """Для 1stop.txt: матчим как отдельные слова."""
    if not stop_categories:
        return re.compile(r"$^")
    stop_categories = sorted(set(stop_categories), key=len, reverse=True)
    parts = [rf"\b{re.escape(v)}\b" for v in stop_categories]
    return re.compile("|".join(parts), flags=re.IGNORECASE)


def build_stop_words_word_start_regex(stop_words: list[str]) -> tuple[re.Pattern, dict[str, str]]:
    """
    Для 2stop.txt: матчим по вхождению, но только если стоп-слово начинается с начала слова/токена.
    Пример: 'сова' матчится в 'совами', но НЕ матчится внутри 'рисования'.
    """
    if not stop_words:
        return re.compile(r"$^"), {}

    canonical_by_lower: dict[str, str] = {}
    for w in stop_words:
        lw = w.lower()
        if lw not in canonical_by_lower:
            canonical_by_lower[lw] = w

    ordered = sorted(canonical_by_lower.keys(), key=len, reverse=True)
    parts = [re.escape(canonical_by_lower[lw]) for lw in ordered]

    pattern = re.compile(rf"(?<!\w)({'|'.join(parts)})", flags=re.IGNORECASE)
    return pattern, canonical_by_lower


def detect_sheet_header_and_columns(excel_path: Path) -> tuple[str, int, dict[str, str]]:
    """
    Автоматически определяет:
      - лист
      - строку заголовков (header row)
      - соответствие: required_key -> реальный заголовок колонки в файле
    """
    xls = pd.ExcelFile(excel_path)

    preferred_sheets = []
    if SHEET_NAME and SHEET_NAME in xls.sheet_names:
        preferred_sheets.append(SHEET_NAME)
    preferred_sheets.extend([s for s in xls.sheet_names if s not in preferred_sheets])

    best = None  # (score_sum, matched_count, sheet, header_row, mapping)
    for sheet in preferred_sheets:
        preview = pd.read_excel(excel_path, sheet_name=sheet, header=None, nrows=60)
        if preview.empty:
            continue

        max_header_row = min(25, len(preview) - 1)
        for header_row in range(0, max_header_row + 1):
            row = preview.iloc[header_row].tolist()
            col_candidates = [str(c) if c is not None else "" for c in row]
            col_norms = [_norm(c) for c in col_candidates]

            mapping_name: dict[str, str] = {}
            used = set()
            score_sum = 0
            matched = 0

            for key, spec in REQUIRED.items():
                best_idx = None
                best_sc = 0
                for i, cn in enumerate(col_norms):
                    if i in used:
                        continue
                    sc = _score(cn, spec)
                    if sc > best_sc:
                        best_sc = sc
                        best_idx = i

                if best_idx is not None and best_sc >= 70:
                    used.add(best_idx)
                    mapping_name[key] = col_candidates[best_idx]
                    score_sum += best_sc
                    matched += 1

            candidate = (score_sum, matched, sheet, header_row, mapping_name)
            if best is None or (candidate[1] > best[1]) or (candidate[1] == best[1] and candidate[0] > best[0]):
                best = candidate

            if matched == 4 and score_sum >= 360:
                break

    if best is None or best[1] < 3:
        raise KeyError(
            "Не удалось надёжно определить заголовки. "
            "Убедитесь, что в файле есть колонки (или их аналоги): "
            f"{[v['label'] for v in REQUIRED.values()]}. "
            "Если заголовки сильно отличаются — задайте USER_COLUMN_MAP вручную."
        )

    _, matched, sheet, header_row, mapping = best
    if matched < 4:
        missing = [REQUIRED[k]["label"] for k in REQUIRED.keys() if k not in mapping]
        print("Внимание: не все колонки найдены автоматически.")
        print("Не найдены:", missing)
        print("Скрипт продолжит работу, но проверьте корректность сопоставления.\n")

    print(f"Автоопределение: лист='{sheet}', строка заголовков={header_row}")
    print("Сопоставление колонок:")
    for k in REQUIRED.keys():
        if k in mapping:
            print(f"  {REQUIRED[k]['label']}  <-  '{mapping[k]}'")
        else:
            print(f"  {REQUIRED[k]['label']}  <-  НЕ НАЙДЕНО")
    print()

    return sheet, header_row, mapping


def main() -> None:
    excel_path = Path(INPUT_EXCEL)
    stop_cat_path = Path(STOP_CATEGORIES_FILE)
    stop_words_path = Path(STOP_WORDS_FILE)

    if not excel_path.exists():
        raise FileNotFoundError(f"Не найден Excel-файл: {excel_path}")

    # Определяем лист/заголовки/колонки
    if USER_COLUMN_MAP:
        sheet_to_use = SHEET_NAME if SHEET_NAME else 0
        df = pd.read_excel(excel_path, sheet_name=sheet_to_use)
        col_map = USER_COLUMN_MAP.copy()

        missing = [col_map[k] for k in ["query", "count", "avg_per_day", "category"] if col_map[k] not in df.columns]
        if missing:
            print("Найденные заголовки колонок:", list(df.columns))
            raise KeyError(f"USER_COLUMN_MAP задан, но в файле не найдены колонки: {missing}")

        print("Используется USER_COLUMN_MAP (ручное сопоставление колонок).")
    else:
        sheet_to_use, header_row, auto_map = detect_sheet_header_and_columns(excel_path)
        df = pd.read_excel(excel_path, sheet_name=sheet_to_use, header=header_row)
        col_map = auto_map

    total_rows_before = len(df)

    # Канонические имена
    QUERY_COL = "Поисковый запрос"
    COUNT_COL = "Количество запросов"
    AVG_PER_DAY_COL = "Запросов в среднем за день"
    CATEGORY_COL = "Больше всего заказов в предмете"

    rename_map = {
        col_map.get("query"): QUERY_COL,
        col_map.get("count"): COUNT_COL,
        col_map.get("avg_per_day"): AVG_PER_DAY_COL,
        col_map.get("category"): CATEGORY_COL,
    }
    rename_map = {k: v for k, v in rename_map.items() if k}
    df = df.rename(columns=rename_map)

    required_cols = [QUERY_COL, COUNT_COL, AVG_PER_DAY_COL, CATEGORY_COL]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        print("Найденные заголовки колонок:", list(df.columns))
        raise KeyError(f"Отсутствуют обязательные столбцы: {missing_cols}")

    # Стоп-списки
    stop_categories = load_stop_list(stop_cat_path)
    stop_words = load_stop_list(stop_words_path)

    cat_regex = build_categories_regex(stop_categories)
    words_regex, words_canonical_map = build_stop_words_word_start_regex(stop_words)

    # Удаляем по стоп-категориям
    mask_stop_cat = df[CATEGORY_COL].fillna("").astype(str).str.contains(cat_regex, na=False)
    removed_by_categories = int(mask_stop_cat.sum())
    df_after_categories = df[~mask_stop_cat].copy()

    # Удаляем по стоп-словам + matched_stop
    query_series = df_after_categories[QUERY_COL].fillna("").astype(str)

    mask_stop_words = query_series.str.contains(words_regex, na=False)
    removed_by_words = int(mask_stop_words.sum())

    matched_raw = query_series.str.extract(words_regex, expand=False)
    matched_canonical = matched_raw.fillna("").astype(str).str.lower().map(words_canonical_map)
    matched_stop = matched_canonical.where(matched_canonical.notna(), matched_raw)

    df_removed_by_words = df_after_categories[mask_stop_words].copy()
    df_removed_by_words["matched_stop"] = matched_stop[mask_stop_words].values

    df_final = df_after_categories[~mask_stop_words].copy()

    # Порядок колонок
    cols_out = [QUERY_COL, COUNT_COL, AVG_PER_DAY_COL, CATEGORY_COL]
    df_final = df_final[cols_out]
    df_removed_by_words = df_removed_by_words[cols_out + ["matched_stop"]]

    # Сохраняем
    output_path = Path(OUTPUT_CSV)
    df_final.to_csv(output_path, index=False, encoding="utf-8-sig")

    removed_words_path = Path(OUTPUT_REMOVED_WORDS_CSV)
    df_removed_by_words.to_csv(removed_words_path, index=False, encoding="utf-8-sig")

    # Статистика
    print("=== Статистика обработки ===")
    print(f"Исходное количество строк: {total_rows_before}")
    print(f"Удалено по стоп-категориям (1stop.txt): {removed_by_categories}")
    print(f"Удалено по стоп-словам (2stop.txt): {removed_by_words}")
    print(f"Итоговое количество строк: {len(df_final)}")
    print()
    print(f"Итоговый CSV-файл сохранён как: {output_path.resolve()}")
    print(f"Удалённые по стоп-словам сохранены в: {removed_words_path.resolve()}")


if __name__ == "__main__":
    main()
