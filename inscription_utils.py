DELEGATE_ID = (
    "66475024139f5a7500b48ac688a7418fdf5838a7eabbc7e6792b7dc7829c8ef7i0"
)

def build_payload(block: int) -> dict:
    """
    Build the inscription JSON for a given block.
    { "p":"ubit", "op":"mint", "tick":"placeholder", "blk":"<BLOCK>" }
    """
    return {
        "p":   "ubit",
        "op":  "mint",
        "tick":"placeholder",
        "blk": str(block),
    }
