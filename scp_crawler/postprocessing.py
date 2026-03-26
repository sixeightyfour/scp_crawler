import json
import os
from pathlib import Path
from datetime import datetime

import click
from tqdm import tqdm

from .utils import get_wiki_source, get_images, get_hubs

cwd = os.getcwd()


def from_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def to_file(data, path):
    print(f"Saving data to {path}")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)



def load_optional_json(path):
    if os.path.exists(path):
        return from_file(path)
    return {}


def load_split_maps(spider_name, fields):
    split_dir = Path(cwd) / "data" / "split"
    maps = {}
    for field in fields:
        path = split_dir / f"{spider_name}__{field}.json"
        maps[field] = load_optional_json(path)
    return maps


def get_field(item, split_maps, field, default=None):
    if field in item and item[field] is not None:
        return item[field]

    url = item.get("url")
    if url and field in split_maps:
        value = split_maps[field].get(url)
        if value is not None:
            return value

    return default



def process_history(history):
    if not history:
        return []

    if isinstance(history, dict):
        history = list(history.values())
    elif not isinstance(history, list):
        return []

    for revision in history:
        if isinstance(revision.get("date"), str):
            revision["date"] = datetime.strptime(
                revision["date"], "%d %b %Y %H:%M"
            )

    history.sort(key=lambda x: x["date"])
    return history


@click.group()
def cli():
    pass



@cli.command()
def run_postproc_items():
    processed_path = Path(cwd + "/data/processed/items")
    os.makedirs(processed_path, exist_ok=True)

    title_list = from_file(cwd + "/data/scp_titles.json")
    title_index = {title["link"]: title["title"] for title in title_list}

    item_list = from_file(cwd + "/data/scp_items.json")

    split_maps = load_split_maps(
        "scp",
        ["raw_content", "history", "page_id", "domain", "link", "references"],
    )

    items = {}
    series_items = {}

    for item in tqdm(item_list, smoothing=0):
        link = get_field(item, split_maps, "link", "")
        raw_content = get_field(item, split_maps, "raw_content", "")
        history = get_field(item, split_maps, "history", {})
        page_id = get_field(item, split_maps, "page_id")
        domain = get_field(item, split_maps, "domain")

        item["link"] = link
        item["raw_content"] = raw_content
        item["history"] = history
        item["page_id"] = page_id
        item["domain"] = domain

        if link in title_index:
            item["title"] = title_index[link]

        if page_id and domain:
            item["raw_source"] = get_wiki_source(page_id, domain)
        else:
            item["raw_source"] = None

        item["images"] = get_images(raw_content) if raw_content else []
        item["hubs"] = get_hubs(link) if link else []

        item["history"] = process_history(history)

        if item["history"]:
            item["created_at"] = item["history"][0]["date"]
            item["creator"] = item["history"][0]["author"]
        else:
            item["created_at"] = "unknown"
            item["creator"] = "unknown"

        items[item["scp"]] = item

        series = item["series"]
        number = item["scp_number"]

        if series.startswith("series-") and number >= 5000:
            label = series + (".5" if number % 1000 > 500 else ".0")
        else:
            label = series

        series_items.setdefault(label, {})[item["scp"]] = item

    item_files = {}
    series_index = {}

    for series, group in series_items.items():
        filename = f"content_{series}.json"
        series_index[series] = filename
        to_file(group, processed_path / filename)

        for item_val in group.values():
            item_files[item_val["link"]] = filename

    to_file(series_index, processed_path / "content_index.json")

    for item_id in items:
        items[item_id].pop("raw_content", None)
        items[item_id].pop("raw_source", None)
        items[item_id]["content_file"] = item_files.get(items[item_id]["link"])

    to_file(items, processed_path / "index.json")



@cli.command()
def run_postproc_tales():
    processed_path = Path(cwd + "/data/processed/tales")
    os.makedirs(processed_path, exist_ok=True)

    tale_list = from_file(cwd + "/data/scp_tales.json")

    split_maps = load_split_maps(
        "scp_tales",
        ["raw_content", "history", "page_id", "domain", "link"],
    )

    tales = {}
    tale_years = {}

    for tale in tqdm(tale_list, smoothing=0):
        raw_content = get_field(tale, split_maps, "raw_content", "")
        history = get_field(tale, split_maps, "history", {})
        page_id = get_field(tale, split_maps, "page_id")
        domain = get_field(tale, split_maps, "domain")

        tale["raw_content"] = raw_content
        tale["history"] = history

        tale["images"] = get_images(raw_content) if raw_content else []

        if page_id and domain:
            tale["raw_source"] = get_wiki_source(page_id, domain)
        else:
            tale["raw_source"] = None

        tale["history"] = process_history(history)

        if tale["history"]:
            dt = tale["history"][0]["date"]
            tale["created_at"] = dt
            tale["creator"] = tale["history"][0]["author"]
            tale["year"] = dt.year
        else:
            tale["created_at"] = "unknown"
            tale["creator"] = "unknown"
            tale["year"] = "unknown"

        link = tale["url"].replace("https://scp-wiki.wikidot.com/", "")
        tales[link] = tale
        tale_years.setdefault(tale["year"], {})[link] = tale

    year_index = {}

    for year, group in tale_years.items():
        filename = f"content_{year}.json"
        year_index[year] = filename
        to_file(group, processed_path / filename)

    to_file(year_index, processed_path / "content_index.json")

    for tale_id in tales:
        tales[tale_id].pop("raw_content", None)
        tales[tale_id].pop("raw_source", None)
        tales[tale_id]["content_file"] = f"content_{tales[tale_id]['year']}.json"

    to_file(tales, processed_path / "index.json")



@cli.command()
def run_postproc_goi():
    processed_path = Path(cwd + "/data/processed/goi")
    os.makedirs(processed_path, exist_ok=True)

    goi_list = from_file(cwd + "/data/goi.json")

    split_maps = load_split_maps(
        "goi",
        ["raw_content", "history", "page_id", "domain"],
    )

    goi_data = {}

    for item in tqdm(goi_list, smoothing=0):
        raw_content = get_field(item, split_maps, "raw_content", "")
        history = get_field(item, split_maps, "history", {})

        item["raw_content"] = raw_content
        item["history"] = history

        item["images"] = get_images(raw_content) if raw_content else []
        item["history"] = process_history(history)

        if item["history"]:
            item["created_at"] = item["history"][0]["date"]
            item["creator"] = item["history"][0]["author"]
        else:
            item["created_at"] = "unknown"
            item["creator"] = "unknown"

        link = item["url"].replace("https://scp-wiki.wikidot.com/", "")
        goi_data[link] = item

    to_file(goi_data, processed_path / "content_goi.json")

    for key in goi_data:
        goi_data[key].pop("raw_content", None)
        goi_data[key]["content_file"] = "content_goi.json"

    to_file(goi_data, processed_path / "index.json")
