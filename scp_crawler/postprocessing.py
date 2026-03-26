import html
import json
import os
import re
import time
from datetime import date, datetime
from pathlib import Path

import httpx
import typer
from bs4 import BeautifulSoup
from tqdm import tqdm

cwd = os.getcwd()
MAIN_TOKEN = "123456"
cli = typer.Typer()


def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)


def from_file(path):
    with open(path, "r", encoding="utf-8") as fs:
        data = json.load(fs)
    return data


def to_file(obj, path):
    with open(path, "w", encoding="utf-8") as fs:
        print(f"Saving data to {path}")
        json.dump(obj, fs, sort_keys=True, default=json_serial)


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
    elif isinstance(history, list):
        history = list(history)
    else:
        return []

    for revision in history:
        if isinstance(revision.get("date"), str):
            revision["date"] = datetime.strptime(
                revision["date"], "%d %b %Y %H:%M"
            )

    history.sort(key=lambda x: x["date"])
    return history


def get_images(html_text):
    content_soup = BeautifulSoup(html_text, "lxml")
    img_tags = content_soup.find_all("img")
    return [
        img["src"]
        for img in img_tags
        if not img["src"].startswith("https://www.wikidot.com/avatar.php")
    ]


def get_wiki_source(page_id, domain, attempts=5):
    try:
        response = httpx.post(
            f"https://{domain}/ajax-module-connector.php",
            data={
                "wikidot_token7": MAIN_TOKEN,
                "page_id": str(page_id),
                "moduleName": "viewsource/ViewSourceModule",
            },
            cookies={"wikidot_token7": MAIN_TOKEN},
            timeout=30.0,
        )
        response.raise_for_status()
    except Exception:
        print(f"Failed to load source for {page_id}")
        attempts -= 1
        if attempts > 0:
            print(f"Sleeping before retry - {attempts} attempts remaining.")
            time.sleep(1)
            return get_wiki_source(page_id, domain, attempts=attempts)
        return False

    try:
        page_response = response.json()
        soup = BeautifulSoup(page_response["body"], "lxml")
        raw_source = "".join(
            str(x) for x in soup.find("div", {"class": "page-source"}).contents
        )
        return re.sub(r"<br\s*/?>", "\n", html.unescape(raw_source), flags=re.IGNORECASE)
    except Exception:
        print(f"Unable to pull body for wikisource from {page_id}")
        return None

def load_hubs():
    hub_path = Path(cwd) / "data" / "scp_hubs.json"
    if not hub_path.exists():
        return {}, {}

    print("Processing Hub list.")
    hub_list = from_file(hub_path)
    hub_items = {}
    hub_references = {}

    for hub in tqdm(hub_list):
        hub["history"] = process_history(hub.get("history"))
        if len(hub["history"]) > 0:
            hub["created_at"] = hub["history"][0]["date"]
            hub["creator"] = hub["history"][0]["author"]
        else:
            hub["created_at"] = "unknown"
            hub["creator"] = "unknown"

        hub_items[hub["link"]] = hub
        hub_references[hub["link"]] = set(hub.get("references", []))

    hub_dir = Path(cwd + "/data/processed/hubs")
    os.makedirs(hub_dir, exist_ok=True)
    to_file(hub_items, hub_dir / "index.json")

    return hub_items, hub_references


def get_hubs(link, hub_references):
    in_hubs = []
    for hub_name, hub_links in hub_references.items():
        if link in hub_links:
            in_hubs.append(hub_name)
    return in_hubs


@cli.command()
def run_postproc_items():
    processed_path = Path(cwd + "/data/processed/items")
    os.makedirs(processed_path, exist_ok=True)

    _, hub_references = load_hubs()

    title_list = from_file(cwd + "/data/scp_titles.json")
    title_index = {title["link"]: title["title"] for title in title_list}

    print("Processing Item list.")
    item_list = from_file(cwd + "/data/scp_items.json")

    split_maps = load_split_maps(
        "scp",
        ["raw_content", "history", "page_id", "domain", "link", "references"],
    )

    items = {}
    series_items = {}

    for item in tqdm(item_list, smoothing=0):
        link = get_field(
            item,
            split_maps,
            "link",
            item.get("url", "").replace("https://scp-wiki.wikidot.com/", ""),
        )
        raw_content = get_field(item, split_maps, "raw_content", "")
        history = get_field(item, split_maps, "history", {})
        page_id = get_field(item, split_maps, "page_id")
        domain = get_field(item, split_maps, "domain")
        references = get_field(item, split_maps, "references", [])

        item["link"] = link
        item["raw_content"] = raw_content
        item["history"] = history
        item["page_id"] = page_id
        item["domain"] = domain
        item["references"] = references

        if item["link"] in title_index:
            item["title"] = title_index[item["link"]]
        else:
            item["title"] = item["scp"]

        if item["page_id"] and item["domain"]:
            item["raw_source"] = get_wiki_source(item["page_id"], item["domain"])
        else:
            item["raw_source"] = None

        item["images"] = get_images(item["raw_content"]) if item["raw_content"] else []
        item["hubs"] = get_hubs(item["link"], hub_references) if item["link"] else []
        
        item["history"] = process_history(item["history"])
        if len(item["history"]) > 0:
            item["created_at"] = item["history"][0]["date"]
            item["creator"] = item["history"][0]["author"]
        else:
            item["created_at"] = "unknown"
            item["creator"] = "unknown"

        items[item["scp"]] = item

        if item["series"].startswith("series-") and item["scp_number"] >= 5000:
            if item["scp_number"] % 1000 > 500:
                label = item["series"] + ".5"
            else:
                label = item["series"] + ".0"
        else:
            label = item["series"]

        if label not in series_items:
            series_items[label] = {}
        series_items[label][item["scp"]] = item

    item_files = {}
    series_index = {}

    for series, series_group in series_items.items():
        filename = f"content_{series}.json"
        series_index[series] = filename
        to_file(series_group, processed_path / filename)

        for item_key, item_value in series_group.items():
            item_files[item_value["link"]] = filename

    to_file(series_index, processed_path / "content_index.json")

    for item_id in items:
        items[item_id].pop("raw_content", None)
        items[item_id].pop("raw_source", None)
        items[item_id]["content_file"] = item_files[items[item_id]["link"]]

    to_file(items, processed_path / "index.json")


@cli.command()
def run_postproc_tales():
    processed_path = Path(cwd + "/data/processed/tales")
    os.makedirs(processed_path, exist_ok=True)

    _, hub_references = load_hubs()
    
    print("Processing Tale list.")
    tale_list = from_file(cwd + "/data/scp_tales.json")

    split_maps = load_split_maps(
        "scp_tales",
        ["raw_content", "history", "page_id", "domain", "link", "references"],
    )

    tales = {}
    tale_years = {}

    for tale in tqdm(tale_list, smoothing=0):
        link = get_field(
            tale,
            split_maps,
            "link",
            tale.get("url", "").replace("https://scp-wiki.wikidot.com/", ""),
        )
        raw_content = get_field(tale, split_maps, "raw_content", "")
        history = get_field(tale, split_maps, "history", {})
        page_id = get_field(tale, split_maps, "page_id")
        domain = get_field(tale, split_maps, "domain")
        references = get_field(tale, split_maps, "references", [])

        tale["link"] = link
        tale["raw_content"] = raw_content
        tale["history"] = history
        tale["page_id"] = page_id
        tale["domain"] = domain
        tale["references"] = references

        tale["images"] = get_images(tale["raw_content"]) if tale["raw_content"] else []
        tale["hubs"] = get_hubs(tale["link"], hub_references) if tale["link"] else []
        
        if tale["page_id"] and tale["domain"]:
            tale["raw_source"] = get_wiki_source(tale["page_id"], tale["domain"])
        else:
            tale["raw_source"] = None

        tale["history"] = process_history(tale["history"])
        if len(tale["history"]) > 0:
            tale["created_at"] = tale["history"][0]["date"]
            tale["creator"] = tale["history"][0]["author"]
            tale["year"] = tale["created_at"].year
        else:
            tale["created_at"] = "unknown"
            tale["creator"] = "unknown"
            tale["year"] = "unknown"

        tale["link"] = tale["url"].replace("https://scp-wiki.wikidot.com/", "")
        tales[tale["link"]] = tale

        if tale["year"] not in tale_years:
            tale_years[tale["year"]] = {}
        tale_years[tale["year"]][tale["link"]] = tale

    year_index = {}
    for year in tale_years:
        filename = f"content_{year}.json"
        year_index[year] = filename
        to_file(tale_years[year], processed_path / filename)

    to_file(year_index, processed_path / "content_index.json")

    for tale_id in tales:
        tales[tale_id].pop("raw_content", None)
        tales[tale_id].pop("raw_source", None)
        year = tales[tale_id]["year"]
        tales[tale_id]["content_file"] = f"content_{year}.json"

    to_file(tales, processed_path / "index.json")


@cli.command()
def run_postproc_goi():
    processed_path = Path(cwd + "/data/processed/goi")
    os.makedirs(processed_path, exist_ok=True)

    _, hub_references = load_hubs()
    
    print("Processing GOI list.")
    tale_list = from_file(cwd + "/data/goi.json")

    split_maps = load_split_maps(
        "goi",
        ["raw_content", "history", "page_id", "domain", "link", "references"],
    )

    tales = {}

    for tale in tqdm(tale_list, smoothing=0):
        link = get_field(
            tale,
            split_maps,
            "link",
            tale.get("url", "").replace("https://scp-wiki.wikidot.com/", ""),
        )
        raw_content = get_field(tale, split_maps, "raw_content", "")
        history = get_field(tale, split_maps, "history", {})
        page_id = get_field(tale, split_maps, "page_id")
        domain = get_field(tale, split_maps, "domain")
        references = get_field(tale, split_maps, "references", [])

        tale["link"] = link
        tale["raw_content"] = raw_content
        tale["history"] = history
        tale["page_id"] = page_id
        tale["domain"] = domain
        tale["references"] = references

        tale["images"] = get_images(tale["raw_content"]) if tale["raw_content"] else []
        tale["hubs"] = get_hubs(tale["link"], hub_references) if tale["link"] else []

        if tale["page_id"] and tale["domain"]:
            tale["raw_source"] = get_wiki_source(tale["page_id"], tale["domain"])
        else:
            tale["raw_source"] = None

        tale["history"] = process_history(tale["history"])
        if len(tale["history"]) > 0:
            tale["created_at"] = tale["history"][0]["date"]
            tale["creator"] = tale["history"][0]["author"]
        else:
            tale["created_at"] = "unknown"
            tale["creator"] = "unknown"

        tale["link"] = tale["url"].replace("https://scp-wiki.wikidot.com/", "")
        tales[tale["link"]] = tale

    to_file(tales, processed_path / "content_goi.json")

    for tale_id in tales:
        tales[tale_id].pop("raw_content", None)
        tales[tale_id].pop("raw_source", None)
        tales[tale_id]["content_file"] = "content_goi.json"

    to_file(tales, processed_path / "index.json")


if __name__ == "__main__":
    cli()
