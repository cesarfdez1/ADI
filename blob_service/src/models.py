from dataclasses import dataclass, field
from typing import List, Dict

@dataclass
class BlobMeta:
    id: str
    name: str
    owner: str
    readable_by: List[str] = field(default_factory=list)
    extra: Dict = field(default_factory=dict)
