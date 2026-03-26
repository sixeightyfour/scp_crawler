import json
from pathlib import Path

from itemadapter import ItemAdapter


class SelectiveExportPipeline:
    def __init__(
        self,
        export_fields=None,
        split_fields=None,
        split_output_dir="data/split",
        drop_split_fields_from_main=False,
    ):
        self.export_fields = set(export_fields or [])
        self.split_fields = set(split_fields or [])
        self.split_output_dir = Path(split_output_dir)
        self.drop_split_fields_from_main = drop_split_fields_from_main
        self.buffers = {}

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            export_fields=crawler.settings.getlist("EXPORT_FIELDS") or None,
            split_fields=crawler.settings.getlist("SPLIT_FIELDS") or [],
            split_output_dir=crawler.settings.get("SPLIT_OUTPUT_DIR", "data/split"),
            drop_split_fields_from_main=crawler.settings.getbool(
                "DROP_SPLIT_FIELDS_FROM_MAIN", False
            ),
        )

    def open_spider(self, spider):
        self.split_output_dir.mkdir(parents=True, exist_ok=True)
        self.buffers = {field: {} for field in self.split_fields}

    def close_spider(self, spider):
        for field, data in self.buffers.items():
            outpath = self.split_output_dir / f"{spider.name}__{field}.json"
            with outpath.open("w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        data = dict(adapter)

        url = data.get("url")
        if not url:
            return item

        for field in self.split_fields:
            if field in data:
                self.buffers[field][url] = data[field]

        if self.export_fields:
            data = {field: data[field] for field in self.export_fields if field in data}

        if self.drop_split_fields_from_main:
            for field in self.split_fields:
                data.pop(field, None)

        new_item = item.__class__()
        for key, value in data.items():
            new_item[key] = value

        return new_item
