"""
Parsers for TCGPlayer plugin.
"""

import csv
import json
from io import StringIO
from typing import AsyncIterator, Any, List, Dict

from core.interfaces import Transform
from core.models import RawItem, ParsedItem


class PokemonSetsParser(Transform):
    """Parse Pokemon sets CSV data into structured format."""
    
    def __init__(self):
        """Initialize parser."""
        pass
    
    @property
    def name(self) -> str:
        return "PokemonSetsParser"
    
    async def parse_csv(self, raw_item: RawItem) -> List[ParsedItem]:
        """Parse CSV data into ParsedItems."""
        if not raw_item.source.endswith(".csv"):
            return []
        
        try:
            # Decode bytes to string, handling BOM if present
            csv_text = raw_item.payload.decode('utf-8-sig')
            
            # Parse CSV with semicolon delimiter
            reader = csv.DictReader(StringIO(csv_text), delimiter=';')
            
            parsed_items = []
            for row in reader:
                # Clean up the data and convert to appropriate types
                content = {
                    "set_name": row["Set Name"].strip() if row["Set Name"] else None,
                    "release_date": row["Release Date"].strip() if row["Release Date"] else None,
                    "booster_product_id": int(row["TCGPlayer Booster Product ID"]) if row["TCGPlayer Booster Product ID"].strip() else None,
                    "booster_box_product_id": int(row["TCGPlayer Booster Box Product ID"]) if row["TCGPlayer Booster Box Product ID"].strip() else None,
                    "group_id": int(row["TCGPlayer Group ID"]) if row["TCGPlayer Group ID"].strip() else None,
                }
                
                # Only include rows with valid data
                if content["set_name"]:
                    parsed_items.append(ParsedItem(
                        topic="tcg.pokemon_sets",
                        content=content,
                        discovered_at=raw_item.fetched_at
                    ))
            
            return parsed_items
            
        except Exception as e:
            print(f"Error parsing CSV data: {e}")
            return []
    
    async def __call__(self, items: AsyncIterator[Any]) -> AsyncIterator[ParsedItem]:
        """Transform interface: parse RawItems into ParsedItems."""
        async for item in items:
            if isinstance(item, RawItem):
                parsed_items = await self.parse_csv(item)
                for parsed in parsed_items:
                    yield parsed
            else:
                # Pass through non-RawItems
                yield item


class PriceHistoryParser(Transform):
    """Parse TCGPlayer price history API responses."""
    
    def __init__(self):
        """Initialize parser."""
        pass
    
    @property
    def name(self) -> str:
        return "PriceHistoryParser"
    
    async def parse_price_history(self, raw_item: RawItem) -> List[ParsedItem]:
        """Parse price history JSON into ParsedItems."""
        if not raw_item.source.startswith("tcgplayer.price_history"):
            return []
        
        try:
            # Extract product ID from source
            product_id = raw_item.source.split(".")[-1]
            
            # Parse JSON response
            json_text = raw_item.payload.decode('utf-8')
            data = json.loads(json_text)
            
            parsed_items = []
            
            # Handle the actual API response structure
            if isinstance(data, dict) and "result" in data:
                # Each result contains a sku with buckets
                for sku_data in data["result"]:
                    sku_id = sku_data.get("skuId")
                    variant = sku_data.get("variant")
                    language = sku_data.get("language")
                    condition = sku_data.get("condition")
                    
                    # Parse each bucket (time period) for this SKU
                    buckets = sku_data.get("buckets", [])
                    for bucket in buckets:
                        content = {
                            "product_id": int(product_id),
                            "sku_id": sku_id,
                            "variant": variant,
                            "language": language,
                            "condition": condition,
                            "market_price": float(bucket.get("marketPrice", 0)) if bucket.get("marketPrice") else None,
                            "quantity_sold": int(bucket.get("quantitySold", 0)) if bucket.get("quantitySold") else None,
                            "low_sale_price": float(bucket.get("lowSalePrice", 0)) if bucket.get("lowSalePrice") else None,
                            "high_sale_price": float(bucket.get("highSalePrice", 0)) if bucket.get("highSalePrice") else None,
                            "bucket_start_date": bucket.get("bucketStartDate"),
                        }
                        
                        parsed_items.append(ParsedItem(
                            topic="tcg.price_history",
                            content=content,
                            discovered_at=raw_item.fetched_at
                        ))
            
            return parsed_items
            
        except Exception as e:
            print(f"Error parsing price history data for {raw_item.source}: {e}")
            return []
    
    async def __call__(self, items: AsyncIterator[Any]) -> AsyncIterator[ParsedItem]:
        """Transform interface: parse RawItems into ParsedItems."""
        async for item in items:
            if isinstance(item, RawItem):
                parsed_items = await self.parse_price_history(item)
                for parsed in parsed_items:
                    yield parsed
            else:
                # Pass through non-RawItems
                yield item
