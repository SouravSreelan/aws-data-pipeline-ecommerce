import pytest
from hypothesis import given, strategies as st
from src.utils import order_schema
import pandas as pd

@given(st.data())
def test_schema_validation(data):
    valid_df = pd.DataFrame({
        'customer_id': [data.draw(st.integers(min_value=1))],
        'order_amount': [data.draw(st.floats(min_value=0))],
        'order_date': [data.draw(st.text(min_size=10, max_size=10).filter(lambda x: x == '2025-01-01'))]
    })
    order_schema.validate(valid_df)