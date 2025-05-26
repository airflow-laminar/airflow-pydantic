from typing import List, Tuple

__all__ = ("RenderedCode",)

Imports = List[str]
Globals = List[str]
TaskCode = str

RenderedCode = Tuple[Imports, Globals, TaskCode]
