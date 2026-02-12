import sys
import os

# Add parent directory to path to import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from fix_parser import FixParser


def test_fix_parser():
    """Comprehensive test suite for FixParser."""
    
    parser = FixParser()
    
    print("=" * 60)
    print("TEST 1: Valid NewOrderSingle (Limit Order)")
    print("=" * 60)
    try:
        msg1 = "8=FIX.4.2|35=D|55=AAPL|54=1|38=100|40=2|44=150.50|10=128"
        parsed1 = parser.parse(msg1)
        print(parser.format_message(parsed1))
        print("\n✓ Parsing successful\n")
        assert parsed1['55'] == 'AAPL'
        assert parsed1['54'] == '1'
        assert parsed1['38'] == '100'
    except ValueError as e:
        print(f"✗ Error: {e}\n")
        raise
    
    print("=" * 60)
    print("TEST 2: Valid NewOrderSingle (Market Order)")
    print("=" * 60)
    try:
        msg2 = "8=FIX.4.2|35=D|55=MSFT|54=2|38=50|40=1|10=100"
        parsed2 = parser.parse(msg2)
        print(parser.format_message(parsed2))
        print("\n✓ Parsing successful\n")
        assert parsed2['55'] == 'MSFT'
        assert parsed2['54'] == '2'
        assert parsed2['40'] == '1'
    except ValueError as e:
        print(f"✗ Error: {e}\n")
        raise
    
    print("=" * 60)
    print("TEST 3: Valid Quote Message")
    print("=" * 60)
    try:
        msg3 = "8=FIX.4.2|35=S|55=GOOGL|132=2800.25|133=2801.50|134=100|135=100|10=200"
        parsed3 = parser.parse(msg3)
        print(parser.format_message(parsed3))
        print("\n✓ Parsing successful\n")
        assert parsed3['35'] == 'S'
        assert parsed3['132'] == '2800.25'
        assert parsed3['133'] == '2801.50'
    except ValueError as e:
        print(f"✗ Error: {e}\n")
        raise
    
    print("=" * 60)
    print("TEST 4: Invalid - Missing Side (tag 54)")
    print("=" * 60)
    try:
        msg4 = "8=FIX.4.2|35=D|55=AAPL|38=100|40=2|10=128"
        parsed4 = parser.parse(msg4)
        print(parser.format_message(parsed4))
        print("\n✓ Parsing successful\n")
        raise AssertionError("Should have raised ValueError for missing Side")
    except ValueError as e:
        print(f"✗ Error: {e}\n")
        assert "54" in str(e), "Error should mention missing tag 54"
    
    print("=" * 60)
    print("TEST 5: Invalid - Limit Order Missing Price")
    print("=" * 60)
    try:
        msg5 = "8=FIX.4.2|35=D|55=AAPL|54=1|38=100|40=2|10=128"
        parsed5 = parser.parse(msg5)
        print(parser.format_message(parsed5))
        print("\n✓ Parsing successful\n")
        raise AssertionError("Should have raised ValueError for missing Price")
    except ValueError as e:
        print(f"✗ Error: {e}\n")
        assert "44" in str(e) or "Price" in str(e), "Error should mention missing Price"
    
    print("=" * 60)
    print("TEST 6: Invalid - Quote Missing BidPx")
    print("=" * 60)
    try:
        msg6 = "8=FIX.4.2|35=S|55=GOOGL|133=2801.50|10=200"
        parsed6 = parser.parse(msg6)
        print(parser.format_message(parsed6))
        print("\n✓ Parsing successful\n")
        raise AssertionError("Should have raised ValueError for missing BidPx")
    except ValueError as e:
        print(f"✗ Error: {e}\n")
        assert "132" in str(e), "Error should mention missing tag 132"
    
    print("=" * 60)
    print("TEST 7: Invalid - Bad Side Value")
    print("=" * 60)
    try:
        msg7 = "8=FIX.4.2|35=D|55=AAPL|54=9|38=100|40=1|10=128"
        parsed7 = parser.parse(msg7)
        print(parser.format_message(parsed7))
        print("\n✓ Parsing successful\n")
        raise AssertionError("Should have raised ValueError for invalid Side value")
    except ValueError as e:
        print(f"✗ Error: {e}\n")
        assert "Side" in str(e), "Error should mention invalid Side value"
    
    print("=" * 60)
    print("✓ All tests passed!")
    print("=" * 60)


if __name__ == "__main__":
    test_fix_parser()
