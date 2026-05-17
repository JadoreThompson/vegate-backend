class SymbolNotFoundException(Exception):

    def __init__(self, symbol: str):
        super().__init__(f"Symbol '{symbol}' not found.")
