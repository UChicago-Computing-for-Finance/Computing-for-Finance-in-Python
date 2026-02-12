class FixClass:

    # Common FIX tag names
    TAG_NAMES = {
        '8': 'BeginString',
        '9': 'BodyLength',
        '35': 'MsgType',
        '49': 'SenderCompID',
        '56': 'TargetCompID',
        '34': 'MsgSeqNum',
        '52': 'SendingTime',
        '55': 'Symbol',
        '54': 'Side',
        '38': 'OrderQty',
        '40': 'OrdType',
        '44': 'Price',
        '132': 'BidPx',
        '133': 'OfferPx',
        '134': 'BidSize',
        '135': 'OfferSize',
        '10': 'CheckSum'
    }
    
    # Message type codes
    MSG_TYPES = {
        'D': 'NewOrderSingle',
        'S': 'Quote',
        '8': 'ExecutionReport',
        '0': 'Heartbeat',
        'A': 'Logon'
    }
    
    # Required tags for different message types
    REQUIRED_TAGS = {
        'D': ['35', '55', '54', '38', '40'],  # NewOrderSingle: MsgType, Symbol, Side, Qty, OrdType
        'S': ['35', '55', '132', '133']        # Quote: MsgType, Symbol, BidPx, OfferPx
    }

    def __init__(self, parsed_dict: dict):
        """
        Initialize FixClass from a parsed FIX message dictionary.
        
        Args:
            parsed_dict: Dictionary with tag numbers as keys and values as strings
        """
        # Set attributes for all tags in the dictionary
        # Use tag names when available, otherwise use 'tag_<number>' format
        for tag, value in parsed_dict.items():
            attr_name = self.TAG_NAMES.get(tag, f'tag_{tag}')
            # Convert to valid Python attribute name (replace spaces, etc.)
            attr_name = attr_name.replace(' ', '_')
            setattr(self, attr_name, value)
        
        # Also store the raw dictionary for backward compatibility
        self._parsed_dict = parsed_dict
    
    def __getitem__(self, tag: str):
        """Allow dictionary-like access using tag numbers."""
        return self._parsed_dict.get(tag)
    
    def get(self, tag: str, default=None):
        """Get value by tag number, with optional default."""
        return self._parsed_dict.get(tag, default)
    
    def __repr__(self):
        """String representation of the object."""
        msg_type = getattr(self, 'MsgType', 'Unknown')
        msg_type_name = self.MSG_TYPES.get(msg_type, msg_type)
        return f"FixClass({msg_type_name})"

        